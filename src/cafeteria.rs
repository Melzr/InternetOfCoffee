use std::clone::Clone;
use std::collections::HashMap;
use std::fs::File;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::constantes::{CANT_CAFETERAS, CANT_CAFETERIAS, TIEMPO_PREPARACION_PEDIDO, TIMEOUT};
use crate::direcciones::{address_data, address_eleccion};
use crate::mensajes_data::{
    construir_paquete_data, obtener_data_paquete, obtener_id_transaccion, EstadoTransaccion,
    Mensaje, ABORT, BUFFER, COMMIT_RESTAR_PUNTOS, INFO, INFO_ACK, OK, PEDIR_INFO,
    PREPARE_RESTAR_PUNTOS, SUMAR_PUNTOS,
};
use crate::mensajes_eleccion::{
    construir_paquete_eleccion, obtener_ids, ACK, BUFFER_ELECCION, COORDINATOR, ELECTION,
};

use crate::pedido::{leer_pedidos, Pedido, Pedidos, PedidosInfo};

type Transacciones = Arc<(Mutex<HashMap<u16, Option<EstadoTransaccion>>>, Condvar)>;

pub struct Cafeteria {
    id: usize,
    pedidos_path: String,
    coordinador: Arc<(Mutex<Option<usize>>, Condvar)>,
    election_ack: Arc<(Mutex<Option<usize>>, Condvar)>,
    election_socket: UdpSocket,
    cuentas: Arc<Mutex<HashMap<u8, (u32, u128)>>>,
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
    /// Crea una nueva cafeteria.
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

    /// Inicia la ejecución de la cafetería.
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

    /// Envia las sumas pendientes al coordinador y espera por los ACKs
    ///
    /// # Argumentos
    ///
    /// * `ack` - Mapa de ACKs de pedidos
    /// * `socket` - Socket de comunicacion
    /// * `cafeteria` - Cafeteria
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
                        sumas.retain(|s| u16::from_be_bytes([s[1], s[2]]) != ack);
                        ack_resp.0.remove(&ack);
                    }
                }
            }
        }
    }

    /// Recibe los mensajes de data de las cafeterias y los procesa
    ///
    /// # Argumentos
    ///
    /// * `socket` - Socket de data
    /// * `ack` - Mapa de acks de data
    /// * `transacciones` - Mapa de transacciones
    fn recibir_mensajes(
        &self,
        socket: &UdpSocket,
        ack: Arc<(Mutex<HashMap<u16, bool>>, Condvar)>,
        transacciones: Transacciones,
    ) {
        let mut buffer: [u8; BUFFER];
        let mut restas_pendientes: HashMap<u16, (u8, u32)> = HashMap::new();
        let mut transacciones_ack = Vec::new();
        loop {
            buffer = [0; BUFFER];
            let response = socket.recv_from(&mut buffer);
            if let Ok(resp) = response {
                if !self.en_linea.load(Ordering::Relaxed) {
                    let next_id = (self.id + 1) % CANT_CAFETERAS;
                    self.pedir_info(socket, next_id);
                } else {
                    let data = obtener_data_paquete(&buffer);
                    match data.accion {
                        ACK => self.recibir_ack(&ack, data),
                        SUMAR_PUNTOS => {
                            self.sumar_puntos(socket, data, &mut transacciones_ack, resp.1)
                        }
                        PREPARE_RESTAR_PUNTOS => {
                            self.bloquear_puntos(socket, data, &mut restas_pendientes, resp.1)
                        }
                        COMMIT_RESTAR_PUNTOS => {
                            self.restar_puntos(socket, data, &mut restas_pendientes)
                        }
                        OK | ABORT => self.recibir_estado_transaccion(data, &transacciones),
                        INFO => self.actualizar_info(data),
                        PEDIR_INFO => self.responder_info(socket, resp.1),
                        _ => {}
                    }
                }
            }
        }
    }

    /// Maneja el mensaje de ACK
    ///
    /// # Argumentos
    ///
    /// * `ack` - Mapa de acks de data
    /// * `buffer` - Buffer de datos
    fn recibir_ack(&self, ack: &Arc<(Mutex<HashMap<u16, bool>>, Condvar)>, data: Mensaje) {
        println!("[NODO {}] ACK recibido", self.id);
        let (lock, cvar) = &**ack;
        lock.lock().unwrap().insert(data.transaccion, true);
        cvar.notify_all();
    }

    /// Maneja el mensaje de OK o ABORT
    ///
    /// # Argumentos
    ///
    /// * `buffer` - Buffer de datos
    /// * `transacciones` - Mapa de transacciones
    fn recibir_estado_transaccion(&self, data: Mensaje, transacciones: &Transacciones) {
        let estado = if data.accion == OK {
            println!("[NODO {}] OK recibido", self.id);
            EstadoTransaccion::Ok
        } else {
            println!("[NODO {}] ABORT recibido", self.id);
            EstadoTransaccion::Abort
        };
        let (lock, cvar) = &**transacciones;
        lock.lock().unwrap().insert(data.transaccion, Some(estado));
        cvar.notify_all();
    }

    /// Suma puntos a una cuenta y envia un ACK y la informacion actualizada
    ///
    /// # Argumentos
    ///
    /// * `socket` - Socket de data
    /// * `buffer` - Buffer de datos
    /// * `transacciones_ack` - Vector de transacciones ya procesadas
    /// * `address` - Direccion de la cafetería que envió el mensaje
    fn sumar_puntos(
        &self,
        socket: &UdpSocket,
        data: Mensaje,
        transacciones_ack: &mut Vec<u16>,
        address: SocketAddr,
    ) {
        if self.obtener_coordinador() == self.id {
            if transacciones_ack.contains(&data.transaccion) {
                self.enviar_respuesta_control(Some(data.transaccion), socket, address, ACK);
                return;
            }
            println!(
                "[COORDINADOR {}] Sumar {} puntos a la cuenta {}",
                self.id, data.puntos, data.cuenta
            );
            let mut cuentas = self.cuentas.lock().unwrap();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let puntos_actuales = cuentas.entry(data.cuenta).or_insert((0, timestamp));
            *puntos_actuales = (puntos_actuales.0 + data.puntos, timestamp);

            println!(
                "[COORDINADOR {}] Puntos nuevos de la cuenta {}: {}",
                self.id, data.cuenta, puntos_actuales.0
            );
            self.enviar_respuesta_control(Some(data.transaccion), socket, address, ACK);
            self.broadcast_info(socket, data.cuenta, (puntos_actuales).0);
            transacciones_ack.push(data.transaccion);
        }
    }

    /// Envia una respuesta de control a una cafetería, esta puede ser un ACK, OK, ABORT o INFO_ACK
    ///
    /// # Argumentos
    ///
    /// * `id` - ID de la transacción
    /// * `socket` - Socket de data
    /// * `address` - Direccion de la cafetería a la que se le envía el mensaje
    /// * `tipo` - Tipo de mensaje a enviar
    fn enviar_respuesta_control(
        &self,
        id: Option<u16>,
        socket: &UdpSocket,
        address: SocketAddr,
        tipo: u8,
    ) {
        let msg = construir_paquete_data(tipo, id, None, None, None);
        socket.send_to(&msg, address).unwrap();
    }

    /// Bloquea los puntos luego de recibir un PREPARE y responde con un OK o ABORT dependiendo de si hay suficientes puntos
    ///
    /// # Argumentos
    ///
    /// * `socket` - Socket de data
    /// * `buffer` - Buffer de datos
    /// * `restas_pendientes` - Mapa de restas pendientes
    /// * `address` - Direccion de la cafetería que envió el mensaje
    fn bloquear_puntos(
        &self,
        socket: &UdpSocket,
        data: Mensaje,
        restas_pendientes: &mut HashMap<u16, (u8, u32)>,
        address: SocketAddr,
    ) {
        if self.obtener_coordinador() == self.id {
            println!(
                "[NODO {}] PREPARE_RESTAR_PUNTOS recibido de {}",
                self.id, data.transaccion
            );
            let cuentas = self.cuentas.lock().unwrap();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let default = (0, timestamp);
            let puntos_actuales = cuentas.get(&data.cuenta).unwrap_or(&default);
            let puntos_bloqueados = restas_pendientes
                .iter()
                .filter(|(_, v)| v.0 == data.cuenta)
                .map(|(_, v)| v.1)
                .sum::<u32>();
            let puntos_libres: i128 = (puntos_actuales.0 as i128) - (puntos_bloqueados as i128);
            if puntos_libres >= (data.puntos as i128) {
                restas_pendientes.insert(data.transaccion, (data.cuenta, data.puntos));
                self.enviar_respuesta_control(Some(data.transaccion), socket, address, OK);
            } else {
                self.enviar_respuesta_control(Some(data.transaccion), socket, address, ABORT);
            }
        }
    }

    /// Resta puntos a una cuenta y envia la informacion actualizada
    ///
    /// # Argumentos
    ///
    /// * `socket` - Socket de data
    /// * `buffer` - Buffer de datos
    /// * `restas_pendientes` - Mapa de restas pendientes
    fn restar_puntos(
        &self,
        socket: &UdpSocket,
        data: Mensaje,
        restas_pendientes: &mut HashMap<u16, (u8, u32)>,
    ) {
        if self.obtener_coordinador() == self.id {
            println!("[COORDINADOR {}] COMMIT_RESTAR_PUNTOS recibido", self.id);
            let (cuenta, _) = restas_pendientes.remove(&data.transaccion).unwrap();
            let mut cuentas = self.cuentas.lock().unwrap();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let puntos_actuales = cuentas.entry(data.cuenta).or_insert((0, timestamp));
            *puntos_actuales = (puntos_actuales.0 - data.puntos, timestamp);
            println!(
                "[COORDINADOR {}] Puntos nuevos de la cuenta {}: {}",
                self.id, cuenta, puntos_actuales.0
            );
            self.broadcast_info(socket, cuenta as u8, (puntos_actuales).0);
        }
    }

    /// Maneja la recepción de un mensaje de INFO, actualizando la información de la cuenta
    ///
    /// # Argumentos
    ///
    /// * `buffer` - Buffer de datos
    fn actualizar_info(&self, data: Mensaje) {
        let mut cuentas = self.cuentas.lock().unwrap();
        if cuentas.get(&data.cuenta).is_none()
            || cuentas.get(&data.cuenta).unwrap().1 < data.timestamp
        {
            cuentas.insert(data.cuenta, (data.puntos, data.timestamp));
        }
        for (cuenta, puntos) in cuentas.iter() {
            println!("[NODO {}] Cuenta {}: {}", self.id, cuenta, puntos.0);
        }
    }

    /// Pide info a un nodo
    ///
    /// # Argumentos
    ///
    /// * `socket` - Socket de data
    /// * `id` - ID de la cafetería a la que se le pide la info
    fn pedir_info(&self, socket: &UdpSocket, id: usize) {
        if id == self.id {
            return;
        }
        let msg = construir_paquete_data(PEDIR_INFO, None, None, None, None);
        socket.send_to(&msg, address_data(id)).unwrap();
        let mut recibio_ack = false;
        while !recibio_ack {
            let mut buffer = [0; BUFFER];
            let response = socket.recv_from(&mut buffer);
            let data = obtener_data_paquete(&buffer);
            if let Ok(res) = response {
                if data.accion == INFO_ACK {
                    println!("[NODO {}] Recibido INFO_ACK", self.id);
                    self.en_linea.store(true, Ordering::Relaxed);
                    let mut clone = self.clone();
                    thread::spawn(move || clone.empezar_eleccion());
                    recibio_ack = true;
                } else if data.accion == INFO {
                    let mut cuentas = self.cuentas.lock().unwrap();
                    cuentas.insert(data.cuenta, (data.puntos, data.timestamp));
                    for (cuenta, puntos) in cuentas.iter() {
                        println!("[NODO {}] Cuenta {}: {}", self.id, cuenta, puntos.0);
                    }
                } else if data.accion == PEDIR_INFO && self.en_linea.load(Ordering::Relaxed) {
                    println!("[NODO {}] Enviando info", self.id);
                    self.enviar_respuesta_control(None, socket, res.1, INFO_ACK);
                }
            } else {
                println!("[NODO {}] No se pudo conectar con el nodo {}", self.id, id);
                self.pedir_info(socket, (id + 1) % CANT_CAFETERAS);
                recibio_ack = true;
            }
        }
    }

    /// Responde a una petición de info
    ///
    /// # Argumentos
    ///
    /// * `socket` - Socket de data
    /// * `address` - Dirección del nodo que pidió la info
    fn responder_info(&self, socket: &UdpSocket, address: SocketAddr) {
        let cuentas = self.cuentas.lock().unwrap();
        println!("[NODO {}] Enviando info a {}", self.id, address);
        for (cuenta, puntos) in cuentas.iter() {
            let msg =
                construir_paquete_data(INFO, None, Some(*cuenta), Some(puntos.0), Some(puntos.1));
            socket.send_to(&msg, address).unwrap();
        }
        self.enviar_respuesta_control(None, socket, address, INFO_ACK);
    }

    /// Envia un mensaje de info a todas las cafeterías
    ///
    /// # Argumentos
    ///
    /// * `socket` - Socket de data
    /// * `cuenta` - Cuenta a la que se le actualizó la info
    /// * `puntos` - Puntos de la cuenta
    fn broadcast_info(&self, socket: &UdpSocket, cuenta: u8, puntos: u32) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let msg = construir_paquete_data(INFO, None, Some(cuenta), Some(puntos), Some(timestamp));
        for i in 0..CANT_CAFETERIAS {
            if i != self.id {
                println!(
                    "[COORDINADOR {}] Enviando INFO a la cafetería {}",
                    self.id, i
                );
                socket.send_to(&msg, address_data(i)).unwrap();
            }
        }
    }

    /// Responde los diferentes mensajes de control de elección de coordinador
    fn responder(&mut self) {
        loop {
            let mut buf = [0; BUFFER_ELECCION];
            let (_, id_sender) = self.election_socket.recv_from(&mut buf).unwrap();
            let accion = buf[0];
            let mut ids = obtener_ids(&buf);

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
                        .send_to(&construir_paquete_eleccion(b'A', &[self.id]), id_sender)
                        .unwrap();
                    if ids.contains(&self.id) {
                        let nuevo_coordinador = *ids.iter().max().unwrap();
                        *self.coordinador.0.lock().unwrap() = Some(nuevo_coordinador);
                        self.coordinador.1.notify_all();
                        let paquete =
                            construir_paquete_eleccion(b'C', &[nuevo_coordinador, self.id]);

                        let clone = self.clone();
                        thread::spawn(move || clone.enviar_al_siguiente(&paquete, clone.id));
                    } else {
                        ids.push(self.id);
                        let paquete = construir_paquete_eleccion(b'E', &ids);

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
                        .send_to(&construir_paquete_eleccion(b'A', &[self.id]), id_sender)
                        .unwrap();
                    if !ids[1..].contains(&self.id) {
                        ids.push(self.id);
                        let paquete = construir_paquete_eleccion(b'C', &ids);

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
            println!("[NODO {}] offline", self.id);
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
        self.enviar_al_siguiente(&construir_paquete_eleccion(b'E', &[self.id]), self.id);
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
                    .as_millis();

                println!(
                    "[NODO {}] pedido con id {} preparado",
                    cafeteria.id, pedido.id
                );

                let buffer = construir_paquete_data(
                    SUMAR_PUNTOS,
                    Some(obtener_id_transaccion(cafeteria.id as u8, pedido.id as u8)),
                    Some(pedido.cuenta as u8),
                    Some(pedido.puntos.unsigned_abs()),
                    Some(timestamp),
                );
                cafeteria.sumas_pendientes.0.lock().unwrap().push(buffer);
                cafeteria.sumas_pendientes.1.notify_all();
            } else {
                let coordinador = cafeteria.obtener_coordinador();
                if cafeteria.en_linea.load(Ordering::SeqCst) {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    let buffer = construir_paquete_data(
                        PREPARE_RESTAR_PUNTOS,
                        Some(obtener_id_transaccion(cafeteria.id as u8, pedido.id as u8)),
                        Some(pedido.cuenta as u8),
                        Some(pedido.puntos.unsigned_abs()),
                        Some(timestamp),
                    );
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
                        let buffer = construir_paquete_data(
                            COMMIT_RESTAR_PUNTOS,
                            Some(obtener_id_transaccion(cafeteria.id as u8, pedido.id as u8)),
                            Some(pedido.cuenta as u8),
                            Some(pedido.puntos.unsigned_abs()),
                            Some(timestamp),
                        );
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
