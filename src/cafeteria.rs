use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{BufRead};
use std::mem::size_of;
use std::net::SocketAddr;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::constants::{ACK, CANT_CAFETERAS, CANT_CAFETERIAS, INFO, SUMAR_PUNTOS, TIEMPO_PEDIDO, PREPARE_RESTAR_PUNTOS, COMMIT_RESTAR_PUNTOS, OK, ABORT};

pub const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct Pedido {
    pub id: u32,
    pub cuenta: u32,
    pub puntos: i32
}

#[derive(PartialEq)]
pub enum EstadoTransaccion {
    Ok,
    Abort
}

pub struct Cafeteria {
    id: usize,
    pedidos_path: String,
    coordinador: Arc<(Mutex<Option<usize>>, Condvar)>,
    ack: Arc<(Mutex<Option<usize>>, Condvar)>,
    socket: UdpSocket,
    termino: Arc<AtomicBool>,
    cuentas: Arc<Mutex<HashMap<u32, i32>>>,
    pedidos: Arc<(Mutex<Vec<Pedido>>, Condvar)>,
    sumas_pendientes: Arc<(Mutex<Vec<[u8; 8]>>, Condvar)>,
    en_linea: Arc<AtomicBool>
}

impl Cafeteria {
    pub fn new(id: usize, pedidos_path: String) -> Cafeteria {
        Cafeteria {
            id,
            pedidos_path,
            coordinador: Arc::new((Mutex::new(None), Condvar::new())),
            ack: Arc::new((Mutex::new(None), Condvar::new())),
            socket: UdpSocket::bind(Self::election_address(id)).unwrap(),
            termino: Arc::new(AtomicBool::new(false)),
            cuentas: Arc::new(Mutex::new(HashMap::new())),
            pedidos: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            sumas_pendientes: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            en_linea: Arc::new(AtomicBool::new(true))
        }
    }

    fn clone(&self) -> Cafeteria {
        Cafeteria {
            id: self.id,
            coordinador: self.coordinador.clone(),
            ack: self.ack.clone(),
            socket: self.socket.try_clone().unwrap(),
            termino: self.termino.clone(),
            cuentas: self.cuentas.clone(),
            pedidos_path: self.pedidos_path.clone(),
            pedidos: self.pedidos.clone(),
            sumas_pendientes: self.sumas_pendientes.clone(),
            en_linea: self.en_linea.clone()
        }
    }

    pub fn election_address(id: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], (8000 + id) as u16))
    }

    pub fn transacion_address(id: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], (9000 + id) as u16))
    }

    pub fn run(&mut self) {
        let mut handles = Vec::new();
        let file = std::fs::File::open(&self.pedidos_path).unwrap();
        let reader = std::io::BufReader::new(file);
        let pedidos_clone = self.pedidos.clone();
        handles.push(thread::spawn(move || {
            Self::leer_pedidos(reader, pedidos_clone)
        }));

        let socket = UdpSocket::bind(Self::transacion_address(self.id)).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let ack = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));
        let transacciones: Arc<(Mutex<HashMap<u16, Option<EstadoTransaccion>>>, Condvar)> = Arc::new((Mutex::new(HashMap::new()), Condvar::new()));

        for _ in 0..CANT_CAFETERAS {
            let mut clone = self.clone();
            let transacciones_clone = transacciones.clone();
            let socket_clone = socket.try_clone().unwrap();
            handles.push(thread::spawn(move || {
                Self::cafetera(&mut clone, transacciones_clone, &socket_clone);
            }));
        }

        let mut clone = self.clone();
        handles.push(thread::spawn(move || clone.responder()));
        self.empezar_eleccion();
        let clone = self.clone();
        let ack2 = ack.clone();
        let socket_clone = socket.try_clone().unwrap();
        let transacciones_clone = transacciones.clone();
        handles.push(thread::spawn(move || {
            clone.recibir_mensajes(&socket_clone, ack2, transacciones_clone)
        }));

        let socket_clone = socket.try_clone().unwrap();
        let ack_clone = ack.clone();
        let mut clone = self.clone();
        handles.push(thread::spawn(move ||
            Self::esperar_acks_pedidos(&ack_clone, &socket_clone, &mut clone)
        ));

        for handle in handles {
            handle.join().unwrap();
        }
    }

    fn leer_pedidos(
        reader: std::io::BufReader<std::fs::File>,
        pedidos: Arc<(Mutex<Vec<Pedido>>, Condvar)>,
    ) {
        let mut id_pedido = 0;
        for line in reader.lines() {
            let line = line.unwrap();
            let mut split = line.split(';');
            let id_cuenta = split.next().unwrap().parse::<u32>().unwrap();
            let puntos = split.next().unwrap().parse::<i32>().unwrap();
            let pedido = Pedido { id: id_pedido, cuenta: id_cuenta, puntos };
            id_pedido += 1;
            pedidos.0.lock().unwrap().push(pedido);
            pedidos.1.notify_one();
            thread::sleep(Duration::from_secs(TIEMPO_PEDIDO));
        }
    }

    pub fn cafetera(cafeteria: &mut Cafeteria, transacciones: Arc<(Mutex<HashMap<u16, Option<EstadoTransaccion>>>, Condvar)>, data_socket: &UdpSocket) {
        loop {
            let (lock, cvar) = &*(cafeteria.pedidos);
            let mut pedido = Pedido {
                id: 0,
                cuenta: 0,
                puntos: 0,
            };
            if let Ok(mut state) = cvar.wait_while(lock.lock().unwrap(), |pedidos_data| {
                !(pedidos_data.first().is_some())
            }) {
                pedido = state.pop().unwrap();
            }
            if pedido.puntos > 0 {
                thread::sleep(Duration::from_secs(10));
                println!("Cafetera: Pedido listo {:?}", pedido);
                let puntos_bytes: [u8; 4] = pedido.puntos.to_be_bytes();
                let buffer = [
                    SUMAR_PUNTOS,
                    cafeteria.id as u8,
                    pedido.id as u8,
                    pedido.cuenta as u8,
                    puntos_bytes[0],
                    puntos_bytes[1],
                    puntos_bytes[2],
                    puntos_bytes[3],
                ];
                cafeteria.sumas_pendientes.0.lock().unwrap().push(buffer);
                cafeteria.sumas_pendientes.1.notify_all();
            } else {
                let coordinador = cafeteria.obtener_coordinador();
                if cafeteria.en_linea.load(Ordering::SeqCst) {
                    let puntos_bytes: [u8; 4] = (pedido.puntos).abs().to_be_bytes();
                    let buffer = [
                        PREPARE_RESTAR_PUNTOS,
                        cafeteria.id as u8,
                        pedido.id as u8,
                        pedido.cuenta as u8,
                        puntos_bytes[0],
                        puntos_bytes[1],
                        puntos_bytes[2],
                        puntos_bytes[3],
                    ];
                    data_socket.send_to(&buffer, Self::transacion_address(coordinador)).unwrap();
                    let id = u16::from_be_bytes([cafeteria.id as u8, pedido.id as u8]);

                    let (lock, cvar) = &*(transacciones);
                    let mut transaccion = None;
                    if let Ok(mut state) = cvar.wait_while(lock.lock().unwrap(), |transacciones_data| {
                        !(transacciones_data.get(&id).is_some())
                    }) {
                        transaccion = state.remove(&id).unwrap();
                    }

                    let transaccion = transaccion.unwrap();
                    if transaccion == EstadoTransaccion::Ok {
                        thread::sleep(Duration::from_secs(10));
                        let buffer = [
                            COMMIT_RESTAR_PUNTOS,
                            cafeteria.id as u8,
                            pedido.id as u8,
                            0,
                            0,
                            0,
                            0,
                            0,
                        ];
                        if transaccion == EstadoTransaccion::Ok {
                            data_socket.send_to(&buffer, Self::transacion_address(coordinador)).unwrap();
                        } else {
                            println!("[NODO {}] Transaccion abortada", cafeteria.id);
                        }
                    } else {
                        println!("[NODO {}] Transaccion abortada", cafeteria.id);
                    }
                } else {
                    println!("[NODO {}] no esta en linea, no se pueden restar puntos", cafeteria.id);
                }
            }
        }
    }

    fn esperar_acks_pedidos(ack: &Arc<(Mutex<HashMap<u16, bool>>, Condvar)>, socket: &UdpSocket, cafeteria: &mut Cafeteria) {
        loop {
            let (sumas_lock, sumas_cvar) = &*cafeteria.sumas_pendientes;
            let (ack_lock, ack_cvar) = &**ack;
            let mut sumas = sumas_cvar.wait_while(sumas_lock.lock().unwrap(), |sumas| sumas.is_empty()).unwrap();
            for suma in sumas.iter() {
                let buffer = &*suma;
                socket
                .send_to(
                    buffer,
                    Self::transacion_address(cafeteria.obtener_coordinador()),
                )
                .unwrap();
            }
            let mut ack_resp = ack_cvar
                .wait_timeout_while(ack_lock.lock().unwrap(), TIMEOUT, |ack| !(ack.iter().any(|(_, v)| *v))).unwrap();
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
                println!(
                    "[NODO {}] encontre coordinador {}",
                    cafeteria.id,
                    cafeteria.coordinador.0.lock().unwrap().unwrap()
                );
            } else {
                let acks_to_remove: Vec<u16> = ack_resp.0.iter().filter(|(_, v)| **v).map(|(k, _)| *k).collect();
                for ack in acks_to_remove {
                    sumas.retain(|s| u16::from_be_bytes([s[1] as u8, s[2] as u8].try_into().unwrap()) != ack);
                    ack_resp.0.remove(&ack);
                }
            }
        }
    }

    fn recibir_mensajes(&self, socket: &UdpSocket, ack: Arc<(Mutex<HashMap<u16, bool>>, Condvar)>, transacciones: Arc<(Mutex<HashMap<u16, Option<EstadoTransaccion>>>, Condvar)>) {
        let mut buffer: [u8; 8];
        let restas_pendientes: Arc<Mutex<HashMap<u16, (u32, i32)>>> = Arc::new(Mutex::new(HashMap::new()));
        loop {
            buffer = [0; 8];
            let response = socket.recv_from(&mut buffer);
            if response.is_ok() {
                if !self.en_linea.load(Ordering::Relaxed) {
                    // pedir info a nodo siguiente
                    
                    // let mut buffer = [0; 8];
                    // buffer[0] = PREPARE_PEDIR_INFO;
                    // buffer[1] = self.id as u8;
                    // buffer[2] = self.id as u8;
                    // buffer[3] = self.id as u8;
                    // socket.send_to(&buffer, Self::transacion_address(self.siguiente)).unwrap();

                    // clear de cuentas
                    // esperar un INFO_ACK, sino contesta mandarle al siguiente
                    
                    // while (!ack) leemos acks e infos

                    // poner en_linea en true
                    
                    // triggerear eleccion (thread)
                }
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
                            let cuenta = buffer[3];
                            let puntos = i32::from_be_bytes(buffer[4..].try_into().unwrap());
                            println!(
                                "[COORDINADOR {}] Sumar {} puntos a la cuenta {}",
                                self.id, puntos, cuenta
                            );
                            let mut cuentas = self.cuentas.lock().unwrap();
                            let puntos_actuales = cuentas.entry(cuenta as u32).or_insert(0);
                            *puntos_actuales += puntos as i32;
                            println!(
                                "[COORDINADOR {}] Puntos nuevos de la cuenta {}: {}",
                                self.id, cuenta, puntos_actuales
                            );
                            socket
                                .send_to(&[ACK, buffer[1], buffer[2], 0, 0, 0, 0, 0], response.unwrap().1)
                                .unwrap();
                            self.broadcast_info(&socket, cuenta, *puntos_actuales);
                        }
                    }
                    PREPARE_RESTAR_PUNTOS => {
                        if self.obtener_coordinador() == self.id {
                            let id = u16::from_be_bytes(buffer[1..=2].try_into().unwrap());
                            let cuenta = buffer[3];
                            let puntos = i32::from_be_bytes(buffer[4..].try_into().unwrap());
                            println!(
                                "[NODO {}] PREPARE_RESTAR_PUNTOS recibido de {}",
                                self.id, id
                            );
                            let cuentas = self.cuentas.lock().unwrap();
                            let puntos_actuales = cuentas.get(&(cuenta as u32)).unwrap_or(&0);
                            let mut restas_pendientes = restas_pendientes.lock().unwrap();
                            let puntos_bloqueados = restas_pendientes
                                .iter()
                                .filter(|(_, v)| v.0 == cuenta as u32)
                                .map(|(_, v)| v.1)
                                .sum::<i32>();

                            if (*puntos_actuales - puntos_bloqueados) >= puntos {
                                restas_pendientes.insert(id, (cuenta as u32, puntos));
                                socket
                                    .send_to(&[OK, buffer[1], buffer[2], 0, 0, 0, 0, 0], response.unwrap().1)
                                    .unwrap();
                            } else {
                                socket
                                    .send_to(&[ABORT, buffer[1], buffer[2], 0, 0, 0, 0, 0], response.unwrap().1)
                                    .unwrap();
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
                            let puntos_actuales = cuentas.entry(cuenta).or_insert(0);
                            *puntos_actuales -= puntos;
                            println!(
                                "[COORDINADOR {}] Puntos nuevos de la cuenta {}: {}",
                                self.id, cuenta, puntos_actuales
                            );
                            self.broadcast_info(&socket, cuenta as u8, *puntos_actuales);
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
                        lock.lock().unwrap().insert(id, Some(EstadoTransaccion::Abort));
                        cvar.notify_all();
                    }
                    INFO => {
                        let cuenta = buffer[1];
                        let puntos = i32::from_be_bytes(buffer[2..=5].try_into().unwrap());
                        let mut cuentas = self.cuentas.lock().unwrap();
                        cuentas.insert(cuenta as u32, puntos);
                        for (cuenta, puntos) in cuentas.iter() {
                            println!("[NODO {}] Cuenta {}: {}", self.id, cuenta, puntos);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn broadcast_info(&self, socket: &UdpSocket, cuenta: u8, puntos: i32) {
        let puntos_bytes: [u8; 4] = puntos.to_be_bytes();
        let buffer = [
            INFO,
            cuenta,
            puntos_bytes[0],
            puntos_bytes[1],
            puntos_bytes[2],
            puntos_bytes[3],
            0,
            0,
        ];
        for i in 0..CANT_CAFETERIAS {
            if i != self.id {
                println!(
                    "[COORDINADOR {}] Enviando INFO a la cafetería {}",
                    self.id, i
                );
                socket
                    .send_to(&buffer, Self::transacion_address(i))
                    .unwrap();
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
        while !self.termino.load(Ordering::SeqCst) {
            let mut buf = [0; 1 + size_of::<usize>() + (CANT_CAFETERIAS + 1) * size_of::<usize>()];
            // self.socket.set_read_timeout(Some(TIMEOUT / 4)).unwrap();
            let (_, id_sender) = self.socket.recv_from(&mut buf).unwrap();
            let accion = buf[0];
            let mut ids = self.obtener_ids_paquete(&buf);

            match accion {
                b'A' => {
                    println!("Nodo {} Recibi ACK de {}", self.id, id_sender);
                    *self.ack.0.lock().unwrap() = Some(ids[0]);
                    self.ack.1.notify_all();
                }
                b'E' => {
                    println!(
                        "Nodo {} recibi ELECTION de {} contenido {:?}",
                        self.id, id_sender, ids
                    );
                    self.socket
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
                b'C' => {
                    println!(
                        "Nodo {} recibi COORDINATOR de {} contenido {:?}",
                        self.id, id_sender, ids
                    );
                    *self.coordinador.0.lock().unwrap() = Some(ids[0]);
                    self.coordinador.1.notify_all();
                    self.socket
                        .send_to(&self.construir_paquete(b'A', &[self.id]), id_sender)
                        .unwrap();
                    if !ids[1..].contains(&self.id) {
                        ids.push(self.id);
                        let paquete = self.construir_paquete(b'C', &ids);

                        let clone = self.clone();
                        thread::spawn(move || clone.enviar_al_siguiente(&paquete, clone.id));
                    }
                    println!(
                        "Nodo {} -  Nuevo lider {}",
                        self.id,
                        self.coordinador.0.lock().unwrap().unwrap()
                    );
                }
                _ => {
                    // Unknown
                }
            }
        }
    }

    fn enviar_al_siguiente(&self, paquete: &[u8], id: usize) {
        let siguiente = (id + 1) % CANT_CAFETERIAS;
        if siguiente == self.id {
            // offline
            self.en_linea.store(false, Ordering::Relaxed);
            *self.coordinador.0.lock().unwrap() = Some(self.id);
        }
        *self.ack.0.lock().unwrap() = None;
        self.socket
            .send_to(paquete, Self::election_address(siguiente)).unwrap();
        let ack = self
            .ack
            .1
            .wait_timeout_while(self.ack.0.lock().unwrap(), TIMEOUT, |ack| {
                ack.is_none() || ack.unwrap() != siguiente
            });
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
        self.coordinador
            .1
            .wait_while(self.coordinador.0.lock().unwrap(), |coordinador| {
                coordinador.is_none()
            });
    }
}
