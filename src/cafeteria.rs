use std::clone::Clone;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::mem::size_of;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::constantes::{CANT_CAFETERAS, CANT_CAFETERIAS, TIEMPO_PREPARACION_PEDIDO, TIMEOUT};
use crate::direcciones::{address_data, address_eleccion};
use crate::mensajes::{
    EstadoTransaccion, ABORT, ACK, COMMIT_RESTAR_PUNTOS, COORDINATOR, ELECTION, INFO, INFO_ACK, OK,
    PEDIR_INFO, PREPARE_RESTAR_PUNTOS, SUMAR_PUNTOS,
};
use crate::pedido::{leer_pedidos, Pedido, Pedidos, PedidosInfo};

type Transacciones = Arc<(Mutex<HashMap<u16, Option<EstadoTransaccion>>>, Condvar)>;

pub struct Cafeteria {
    id: usize,
    pedidos_path: String,
    coordinador: Arc<(Mutex<Option<usize>>, Condvar)>,
    election_ack: Arc<(Mutex<Option<usize>>, Condvar)>,
    election_socket: UdpSocket,
    cuentas: Arc<Mutex<HashMap<u32, (i32, u128)>>>,
    pedidos: Pedidos,
    sumas_pendientes: Arc<(Mutex<Vec<[u8; 24]>>, Condvar)>,
    fin: Arc<AtomicBool>,
    en_linea: Arc<AtomicBool>,
}

impl Clone for Cafeteria {
    fn clone(&self) -> Cafeteria {
        Cafeteria {
            id: self.id,
            pedidos_path: self.pedidos_path.clone(),
            coordinador: self.coordinador.clone(),
            election_ack: self.election_ack.clone(),
            election_socket: self.election_socket.try_clone().unwrap(),
            cuentas: self.cuentas.clone(),
            pedidos: self.pedidos.clone(),
            sumas_pendientes: self.sumas_pendientes.clone(),
            fin: self.fin.clone(),
            en_linea: self.en_linea.clone(),
        }
    }
}

impl Cafeteria {
    pub fn new(id: usize, pedidos_path: String) -> Cafeteria {
        Cafeteria {
            id,
            pedidos_path,
            coordinador: Arc::new((Mutex::new(None), Condvar::new())),
            election_ack: Arc::new((Mutex::new(None), Condvar::new())),
            election_socket: UdpSocket::bind(address_eleccion(id)).unwrap(),
            cuentas: Arc::new(Mutex::new(HashMap::new())),
            pedidos: Arc::new((Mutex::new(PedidosInfo::new()), Condvar::new())),
            sumas_pendientes: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            fin: Arc::new(AtomicBool::new(false)),
            en_linea: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn run(&mut self) -> Result<(), String> {
        let mut handles = Vec::new();
        let file = File::open(&self.pedidos_path)
            .map_err(|_| format!("No se pudo abrir el archivo {}", &self.pedidos_path))?;
        let reader = std::io::BufReader::new(file);

        let data_socket = UdpSocket::bind(address_data(self.id)).unwrap();
        data_socket.set_read_timeout(Some(TIMEOUT)).unwrap();

        let data_ack = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));
        let transacciones: Transacciones = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));

        for _ in 0..CANT_CAFETERAS {
            let mut clone = self.clone();
            let transacciones_clone = transacciones.clone();
            let socket_clone = data_socket.try_clone().unwrap();
            handles.push(thread::spawn(move || {
                Self::cafetera(&mut clone, transacciones_clone, &socket_clone);
            }));
        }

        let mut clone = self.clone();
        handles.push(thread::spawn(move || clone.responder()));
        self.pedir_info(&data_socket, (self.id + 1) % CANT_CAFETERIAS);

        let clone = self.clone();
        let data_ack_clone = data_ack.clone();
        let socket_clone = data_socket.try_clone().unwrap();
        handles.push(thread::spawn(move || {
            clone.recibir_mensajes(&socket_clone, data_ack_clone, transacciones)
        }));

        let socket_clone = data_socket.try_clone().unwrap();
        let mut clone = self.clone();
        handles.push(thread::spawn(move || {
            Self::esperar_acks_pedidos(&data_ack, &socket_clone, &mut clone)
        }));

        leer_pedidos(reader, &self.pedidos);
        self.fin.store(true, Ordering::Relaxed);

        for handle in handles {
            if handle.join().is_err() {
                println!("[WARN] Error en el join de un thread");
            }
        }

        Ok(())
    }

    fn esperar_acks_pedidos(
        ack: &Arc<(Mutex<HashMap<u16, bool>>, Condvar)>,
        socket: &UdpSocket,
        cafeteria: &mut Cafeteria,
    ) {
        loop {
            if cafeteria.en_linea.load(Ordering::Relaxed) {
                let (sumas_lock, sumas_cvar) = &*cafeteria.sumas_pendientes;
                let (ack_lock, ack_cvar) = &**ack;
                let mut sumas = sumas_cvar
                    .wait_while(sumas_lock.lock().unwrap(), |sumas| sumas.is_empty())
                    .unwrap();
                for suma in sumas.iter() {
                    socket
                        .send_to(suma, address_data(cafeteria.obtener_coordinador()))
                        .unwrap();
                }
                let mut ack_resp = ack_cvar
                    .wait_timeout_while(ack_lock.lock().unwrap(), TIMEOUT, |ack| {
                        !(ack.iter().any(|(_, v)| *v))
                    })
                    .unwrap();
                if ack_resp.1.timed_out() {
                    drop(sumas);
                    drop(ack_resp);
                    println!("[NODO {}] No se recibió ACK", cafeteria.id);
                    println!(
                        "[NODO {}] coordinador {} murio",
                        cafeteria.id,
                        cafeteria.coordinador.0.lock().unwrap().unwrap()
                    );
                    *(cafeteria.coordinador.0.lock().unwrap()) = None;
                    cafeteria.empezar_eleccion();
                    let coordinador = cafeteria.obtener_coordinador();
                    println!(
                        "[NODO {}] encontre coordinador {}",
                        cafeteria.id, coordinador
                    );
                } else {
                    let acks_to_remove: Vec<u16> = ack_resp
                        .0
                        .iter()
                        .filter(|(_, v)| **v)
                        .map(|(k, _)| *k)
                        .collect();
                    for ack in acks_to_remove {
                        // sumas.retain(|s| u16::from_be_bytes([s[1] as u8, s[2] as u8].try_into().unwrap()) != ack);
                        sumas.retain(|s| u16::from_be_bytes([s[1], s[2]]) != ack);
                        ack_resp.0.remove(&ack);
                    }
                }
            }
        }
    }

    fn recibir_mensajes(
        &self,
        socket: &UdpSocket,
        ack: Arc<(Mutex<HashMap<u16, bool>>, Condvar)>,
        transacciones: Transacciones,
    ) {
        let mut buffer: [u8; 24];
        let restas_pendientes: Arc<Mutex<HashMap<u16, (u32, i32)>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut transacciones_ack = Vec::new();
        loop {
            buffer = [0; 24];
            let response = socket.recv_from(&mut buffer);
            if let Ok(resp) = response {
                if !self.en_linea.load(Ordering::Relaxed) {
                    // pedir info a nodo siguiente
                    let next_id = (self.id + 1) % CANT_CAFETERAS;
                    self.pedir_info(socket, next_id);
                } else {
                    match buffer[0] {
                        ACK => {
                            println!("[NODO {}] ACK recibido", self.id);
                            let id = u16::from_be_bytes(buffer[1..=2].try_into().unwrap());
                            let (lock, cvar) = &*ack;
                            lock.lock().unwrap().insert(id, true);
                            cvar.notify_all();
                        }
                        SUMAR_PUNTOS => {
                            if self.obtener_coordinador() == self.id {
                                let id_transaccion =
                                    u16::from_be_bytes(buffer[1..=2].try_into().unwrap());
                                if transacciones_ack.contains(&id_transaccion) {
                                    let mut buf = [0; 24];
                                    buf[0] = ACK;
                                    buf[1..=2].copy_from_slice(&buffer[1..=2]);
                                    socket.send_to(&buf, response.unwrap().1).unwrap();
                                    continue;
                                }
                                let cuenta = buffer[3];
                                let puntos = i32::from_be_bytes(buffer[4..=7].try_into().unwrap());
                                println!(
                                    "[COORDINADOR {}] Sumar {} puntos a la cuenta {}",
                                    self.id, puntos, cuenta
                                );
                                let mut cuentas = self.cuentas.lock().unwrap();
                                let timestamp = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                let puntos_actuales =
                                    cuentas.entry(cuenta as u32).or_insert((0, timestamp));
                                *puntos_actuales = (puntos_actuales.0 + puntos, timestamp);

                                println!(
                                    "[COORDINADOR {}] Puntos nuevos de la cuenta {}: {}",
                                    self.id, cuenta, puntos_actuales.0
                                );
                                let mut buf = [0; 24];
                                buf[0] = ACK;
                                buf[1..=2].copy_from_slice(&buffer[1..=2]);
                                socket.send_to(&buf, response.unwrap().1).unwrap();
                                self.broadcast_info(socket, cuenta, (puntos_actuales).0);
                                transacciones_ack.push(id_transaccion);
                            }
                        }
                        PREPARE_RESTAR_PUNTOS => {
                            if self.obtener_coordinador() == self.id {
                                let id = u16::from_be_bytes(buffer[1..=2].try_into().unwrap());
                                let cuenta = buffer[3];
                                let puntos = i32::from_be_bytes(buffer[4..=7].try_into().unwrap());
                                println!(
                                    "[NODO {}] PREPARE_RESTAR_PUNTOS recibido de {}",
                                    self.id, id
                                );
                                let cuentas = self.cuentas.lock().unwrap();
                                let timestamp = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                let default = (0, timestamp);
                                let puntos_actuales =
                                    cuentas.get(&(cuenta as u32)).unwrap_or(&default);
                                let mut restas_pendientes = restas_pendientes.lock().unwrap();
                                let puntos_bloqueados = restas_pendientes
                                    .iter()
                                    .filter(|(_, v)| v.0 == cuenta as u32)
                                    .map(|(_, v)| v.1)
                                    .sum::<i32>();

                                if ((puntos_actuales).0 - puntos_bloqueados) >= puntos {
                                    restas_pendientes.insert(id, (cuenta as u32, puntos));
                                    let mut buf = [0; 24];
                                    buf[0] = OK;
                                    buf[1..=2].copy_from_slice(&buffer[1..=2]);
                                    socket.send_to(&buf, resp.1).unwrap();
                                } else {
                                    let mut buf = [0; 24];
                                    buf[0] = ABORT;
                                    buf[1..=2].copy_from_slice(&buffer[1..=2]);
                                    socket.send_to(&buf, resp.1).unwrap();
                                }
                            }
                        }
                        COMMIT_RESTAR_PUNTOS => {
                            if self.obtener_coordinador() == self.id {
                                let id = u16::from_be_bytes(buffer[1..=2].try_into().unwrap());
                                println!(
                                    "[NODO {}] COMMIT_RESTAR_PUNTOS recibido de {}",
                                    self.id, id
                                );
                                let mut restas = restas_pendientes.lock().unwrap();
                                let (cuenta, puntos) = restas.remove(&id).unwrap();
                                let mut cuentas = self.cuentas.lock().unwrap();
                                let timestamp = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                let puntos_actuales =
                                    cuentas.entry(cuenta).or_insert((0, timestamp));
                                *puntos_actuales = (puntos_actuales.0 - puntos, timestamp);
                                println!(
                                    "[COORDINADOR {}] Puntos nuevos de la cuenta {}: {}",
                                    self.id, cuenta, puntos_actuales.0
                                );
                                self.broadcast_info(socket, cuenta as u8, (puntos_actuales).0);
                            }
                        }
                        OK => {
                            println!("[NODO {}] OK recibido", self.id);
                            let id = u16::from_be_bytes(buffer[1..=2].try_into().unwrap());
                            let (lock, cvar) = &*transacciones;
                            lock.lock().unwrap().insert(id, Some(EstadoTransaccion::Ok));
                            cvar.notify_all();
                        }
                        ABORT => {
                            println!("[NODO {}] ABORT recibido", self.id);
                            let id = u16::from_be_bytes(buffer[1..=2].try_into().unwrap());
                            let (lock, cvar) = &*transacciones;
                            lock.lock()
                                .unwrap()
                                .insert(id, Some(EstadoTransaccion::Abort));
                            cvar.notify_all();
                        }
                        INFO => {
                            let cuenta = buffer[1];
                            let puntos = i32::from_be_bytes(buffer[2..=5].try_into().unwrap());
                            let timestamp = u128::from_be_bytes(buffer[6..=21].try_into().unwrap());
                            let mut cuentas = self.cuentas.lock().unwrap();
                            if cuentas.get(&(cuenta as u32)).is_none()
                                || cuentas.get(&(cuenta as u32)).unwrap().1 < timestamp
                            {
                                cuentas.insert(cuenta as u32, (puntos, timestamp));
                            }
                            for (cuenta, puntos) in cuentas.iter() {
                                println!("[NODO {}] Cuenta {}: {}", self.id, cuenta, puntos.0);
                            }
                        }
                        PEDIR_INFO => {
                            let cuentas = self.cuentas.lock().unwrap();
                            let addr = response.unwrap().1;
                            println!("[NODO {}] Enviando info a {}", self.id, addr);
                            for (cuenta, puntos) in cuentas.iter() {
                                let puntos_bytes = puntos.0.to_be_bytes();
                                let timestamp = puntos.1.to_be_bytes();
                                let mut buf = [0; 24];
                                buf[0] = INFO;
                                buf[1] = *cuenta as u8;
                                buf[2..=5].copy_from_slice(&puntos_bytes);
                                buf[6..=21].copy_from_slice(&timestamp);
                                socket.send_to(&buf, addr).unwrap();
                            }
                            let mut buf = [0; 24];
                            buf[0] = INFO_ACK;
                            socket.send_to(&buf, addr).unwrap();
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn pedir_info(&self, socket: &UdpSocket, id: usize) {
        if id == self.id {
            return;
        }
        let mut buffer = [0; 24];
        buffer[0] = PEDIR_INFO;
        socket.send_to(&buffer, address_data(id)).unwrap();
        let mut recibio_ack = false;
        while !recibio_ack {
            let mut buffer = [0; 24];
            let response = socket.recv_from(&mut buffer);
            if let Ok(data) = response {
                if buffer[0] == INFO_ACK {
                    println!("[NODO {}] Recibido INFO_ACK", self.id);
                    self.en_linea.store(true, Ordering::Relaxed);
                    let mut clone = self.clone();
                    thread::spawn(move || clone.empezar_eleccion());
                    recibio_ack = true;
                } else if buffer[0] == INFO {
                    let cuenta = buffer[1];
                    let puntos = i32::from_be_bytes(buffer[2..=5].try_into().unwrap());
                    let timestamp = u128::from_be_bytes(buffer[6..=21].try_into().unwrap());
                    let mut cuentas = self.cuentas.lock().unwrap();
                    cuentas.insert(cuenta as u32, (puntos, timestamp));
                    for (cuenta, puntos) in cuentas.iter() {
                        println!("[NODO {}] Cuenta {}: {}", self.id, cuenta, puntos.0);
                    }
                } else if buffer[0] == PEDIR_INFO && self.en_linea.load(Ordering::Relaxed) {
                    println!("[NODO {}] Enviando info", self.id);
                    let mut buf = [0; 24];
                    buf[0] = INFO_ACK;
                    socket.send_to(&buf, data.1).unwrap();
                }
            } else {
                println!("[NODO {}] No se pudo conectar con el nodo {}", self.id, id);
                self.pedir_info(socket, (id + 1) % CANT_CAFETERAS);
                recibio_ack = true;
            }
        }
    }

    fn broadcast_info(&self, socket: &UdpSocket, cuenta: u8, puntos: i32) {
        let puntos_bytes: [u8; 4] = puntos.to_be_bytes();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_be_bytes();
        let mut buffer = [0; 24];
        buffer[0] = INFO;
        buffer[1] = cuenta;
        buffer[2..=5].copy_from_slice(&puntos_bytes);
        buffer[6..=21].copy_from_slice(&timestamp);
        for i in 0..CANT_CAFETERIAS {
            if i != self.id {
                println!(
                    "[COORDINADOR {}] Enviando INFO a la cafetería {}",
                    self.id, i
                );
                socket.send_to(&buffer, address_data(i)).unwrap();
            }
        }
    }

    fn construir_paquete(&self, accion: u8, ids: &[usize]) -> Vec<u8> {
        let mut paquete = vec![accion];
        paquete.extend_from_slice(&ids.len().to_le_bytes());
        for id in ids {
            paquete.extend_from_slice(&id.to_le_bytes());
        }
        paquete
    }

    fn obtener_ids_paquete(&self, buf: &[u8]) -> Vec<usize> {
        let mut ids = Vec::new();
        let n = usize::from_le_bytes(buf[1..(size_of::<usize>() + 1)].try_into().unwrap());
        let mut i = size_of::<usize>() + 1;

        for _ in 0..n {
            ids.push(usize::from_le_bytes(
                buf[i..(size_of::<usize>() + i)].try_into().unwrap(),
            ));
            i += size_of::<usize>();
        }

        ids
    }

    fn responder(&mut self) {
        // while !self.fin.load(Ordering::Relaxed) {
        loop {
            let mut buf = [0; 1 + size_of::<usize>() + (CANT_CAFETERIAS + 1) * size_of::<usize>()];
            // self.socket.set_read_timeout(Some(TIMEOUT / 4)).unwrap();
            let (_, id_sender) = self.election_socket.recv_from(&mut buf).unwrap();
            let accion = buf[0];
            let mut ids = self.obtener_ids_paquete(&buf);

            match accion {
                ACK => {
                    println!("Nodo {} Recibi ACK de {}", self.id, id_sender);
                    *self.election_ack.0.lock().unwrap() = Some(ids[0]);
                    self.election_ack.1.notify_all();
                }
                ELECTION => {
                    println!(
                        "Nodo {} recibi ELECTION de {} contenido {:?}",
                        self.id, id_sender, ids
                    );
                    self.election_socket
                        .send_to(&self.construir_paquete(b'A', &[self.id]), id_sender)
                        .unwrap();
                    if ids.contains(&self.id) {
                        let nuevo_coordinador = *ids.iter().max().unwrap();
                        *self.coordinador.0.lock().unwrap() = Some(nuevo_coordinador);
                        self.coordinador.1.notify_all();
                        let paquete = self.construir_paquete(b'C', &[nuevo_coordinador, self.id]);

                        let clone = self.clone();
                        thread::spawn(move || clone.enviar_al_siguiente(&paquete, clone.id));
                    } else {
                        ids.push(self.id);
                        let paquete = self.construir_paquete(b'E', &ids);

                        let clone = self.clone();
                        thread::spawn(move || clone.enviar_al_siguiente(&paquete, clone.id));
                    }
                }
                COORDINATOR => {
                    println!(
                        "[Nodo {}] recibi COORDINATOR de {} contenido {:?}",
                        self.id, id_sender, ids
                    );
                    *self.coordinador.0.lock().unwrap() = Some(ids[0]);
                    self.coordinador.1.notify_all();
                    self.election_socket
                        .send_to(&self.construir_paquete(b'A', &[self.id]), id_sender)
                        .unwrap();
                    if !ids[1..].contains(&self.id) {
                        ids.push(self.id);
                        let paquete = self.construir_paquete(b'C', &ids);

                        let clone = self.clone();
                        thread::spawn(move || clone.enviar_al_siguiente(&paquete, clone.id));
                    }
                    println!(
                        "[Nodo {}] Nuevo lider {}",
                        self.id,
                        self.coordinador.0.lock().unwrap().unwrap()
                    );
                }
                _ => {
                    println!("[Nodo {}] Recibi accion desconocida {}", self.id, accion);
                }
            }
        }
    }

    fn enviar_al_siguiente(&self, paquete: &[u8], id: usize) {
        let siguiente = (id + 1) % CANT_CAFETERIAS;
        if siguiente == self.id {
            // offline
            self.en_linea.store(false, Ordering::Relaxed);
            println!("[NODO {}] Estoy offline", self.id);
            *self.coordinador.0.lock().unwrap() = Some(self.id);
            return;
        }
        *self.election_ack.0.lock().unwrap() = None;
        self.election_socket
            .send_to(paquete, address_eleccion(siguiente))
            .unwrap();
        let ack = self.election_ack.1.wait_timeout_while(
            self.election_ack.0.lock().unwrap(),
            TIMEOUT,
            |ack| ack.is_none() || ack.unwrap() != siguiente,
        );
        if ack.unwrap().1.timed_out() {
            self.enviar_al_siguiente(paquete, siguiente)
        }
    }

    fn obtener_coordinador(&self) -> usize {
        self.coordinador
            .1
            .wait_while(self.coordinador.0.lock().unwrap(), |coordinador| {
                coordinador.is_none()
            })
            .unwrap()
            .unwrap()
    }

    fn empezar_eleccion(&mut self) {
        println!("[INFO] Nodo {} empezando eleccion", self.id);
        self.enviar_al_siguiente(&self.construir_paquete(b'E', &[self.id]), self.id);
    }

    pub fn cafetera(
        cafeteria: &mut Cafeteria,
        transacciones: Transacciones,
        data_socket: &UdpSocket,
    ) {
        loop {
            let (lock, cvar) = &*(cafeteria.pedidos);
            let mut pedido = Pedido {
                id: 0,
                cuenta: 0,
                puntos: 0,
            };
            if let Ok(mut state) = cvar.wait_while(lock.lock().unwrap(), |pedidos| {
                pedidos.cola_pedidos.is_empty() && !pedidos.fin
            }) {
                if state.fin {
                    break;
                }
                pedido = state.cola_pedidos.pop_front().unwrap();
            }
            if pedido.puntos > 0 {
                thread::sleep(TIEMPO_PREPARACION_PEDIDO);
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    .to_be_bytes();

                println!(
                    "[NODO {}] pedido con id {} preparado",
                    cafeteria.id, pedido.id
                );
                let puntos_bytes: [u8; 4] = pedido.puntos.to_be_bytes();
                let mut buffer: [u8; 24] = [0; 24];
                buffer[0] = SUMAR_PUNTOS;
                buffer[1] = cafeteria.id as u8;
                buffer[2] = pedido.id as u8;
                buffer[3] = pedido.cuenta as u8;
                buffer[4..=7].copy_from_slice(&puntos_bytes);
                buffer[8..=23].copy_from_slice(&timestamp);
                cafeteria.sumas_pendientes.0.lock().unwrap().push(buffer);
                cafeteria.sumas_pendientes.1.notify_all();
            } else {
                let coordinador = cafeteria.obtener_coordinador();
                if cafeteria.en_linea.load(Ordering::SeqCst) {
                    let puntos_bytes: [u8; 4] = (pedido.puntos).abs().to_be_bytes();
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis()
                        .to_be_bytes();
                    let mut buffer: [u8; 24] = [0; 24];
                    buffer[0] = PREPARE_RESTAR_PUNTOS;
                    buffer[1] = cafeteria.id as u8;
                    buffer[2] = pedido.id as u8;
                    buffer[3] = pedido.cuenta as u8;
                    buffer[4..=7].copy_from_slice(&puntos_bytes);
                    buffer[8..=23].copy_from_slice(&timestamp);
                    data_socket
                        .send_to(&buffer, address_data(coordinador))
                        .unwrap();
                    let id = u16::from_be_bytes([cafeteria.id as u8, pedido.id as u8]);

                    let (lock, cvar) = &*(transacciones);
                    let mut transaccion = None;
                    if let Ok(mut state) = cvar
                        .wait_while(lock.lock().unwrap(), |transacciones_data| {
                            transacciones_data.get(&id).is_none()
                        })
                    {
                        transaccion = state.remove(&id).unwrap();
                    }

                    let transaccion = transaccion.unwrap();
                    if transaccion == EstadoTransaccion::Ok {
                        thread::sleep(TIEMPO_PREPARACION_PEDIDO);
                        let mut buffer: [u8; 24] = [0; 24];
                        buffer[0] = COMMIT_RESTAR_PUNTOS;
                        buffer[1] = cafeteria.id as u8;
                        buffer[2] = pedido.id as u8;
                        if transaccion == EstadoTransaccion::Ok {
                            data_socket
                                .send_to(&buffer, address_data(coordinador))
                                .unwrap();
                        } else {
                            println!("[NODO {}] Transaccion abortada", cafeteria.id);
                        }
                    } else {
                        println!("[NODO {}] Transaccion abortada", cafeteria.id);
                    }
                } else {
                    println!(
                        "[NODO {}] no esta en linea, no se pueden restar puntos",
                        cafeteria.id
                    );
                }
            }
        }
    }
}
