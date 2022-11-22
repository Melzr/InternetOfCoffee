use std::clone::Clone;
use std::collections::HashMap;
use std::fs::File;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cafetera::cafetera;
use crate::constantes::{CANT_CAFETERAS, CANT_CAFETERIAS, TIMEOUT};
use crate::direcciones::{address_data, address_eleccion};
use crate::mensajes_data::{
    construir_paquete_data, obtener_data_paquete, EstadoTransaccion, Mensaje, ABORT, BUFFER,
    COMMIT_RESTAR_PUNTOS, INFO, INFO_ACK, OK, PEDIR_INFO, PREPARE_RESTAR_PUNTOS, SUMAR_PUNTOS,
};
use crate::mensajes_eleccion::{
    construir_paquete_eleccion, obtener_ids, ACK, BUFFER_ELECCION, COORDINATOR, ELECTION,
};

use crate::pedido::{leer_pedidos, Pedidos, PedidosInfo};

pub type Transacciones = Arc<(Mutex<HashMap<u16, Option<EstadoTransaccion>>>, Condvar)>;

pub struct Cafeteria {
    pub id: usize,
    pedidos_path: String,
    coordinador: Arc<(Mutex<Option<usize>>, Condvar)>,
    election_ack: Arc<(Mutex<Option<usize>>, Condvar)>,
    data_ack: Arc<(Mutex<HashMap<u16, bool>>, Condvar)>,
    election_socket: UdpSocket,
    pub data_socket: UdpSocket,
    cuentas: Arc<Mutex<HashMap<u8, (u32, u128)>>>,
    pub pedidos: Pedidos,
    pub transacciones: Transacciones,
    pub sumas_pendientes: Arc<(Mutex<Vec<[u8; 24]>>, Condvar)>,
    pub en_linea: Arc<AtomicBool>,
}

impl Clone for Cafeteria {
    fn clone(&self) -> Cafeteria {
        Cafeteria {
            id: self.id,
            pedidos_path: self.pedidos_path.clone(),
            coordinador: self.coordinador.clone(),
            election_ack: self.election_ack.clone(),
            data_ack: self.data_ack.clone(),
            election_socket: self.election_socket.try_clone().unwrap(),
            data_socket: self.data_socket.try_clone().unwrap(),
            cuentas: self.cuentas.clone(),
            pedidos: self.pedidos.clone(),
            transacciones: self.transacciones.clone(),
            sumas_pendientes: self.sumas_pendientes.clone(),
            en_linea: self.en_linea.clone(),
        }
    }
}

impl Cafeteria {
    /// Crea una nueva Cafeteria.
    pub fn new(id: usize, pedidos_path: String) -> Result<Cafeteria, String> {
        let cafeteria = Cafeteria {
            id,
            pedidos_path,
            coordinador: Arc::new((Mutex::new(None), Condvar::new())),
            election_ack: Arc::new((Mutex::new(None), Condvar::new())),
            data_ack: Arc::new((Mutex::new(HashMap::new()), Condvar::new())),
            election_socket: UdpSocket::bind(address_eleccion(id)).map_err(|e| e.to_string())?,
            data_socket: UdpSocket::bind(address_data(id)).map_err(|e| e.to_string())?,
            cuentas: Arc::new(Mutex::new(HashMap::new())),
            pedidos: Arc::new((Mutex::new(PedidosInfo::new()), Condvar::new())),
            transacciones: Arc::new((Mutex::new(HashMap::new()), Condvar::new())),
            sumas_pendientes: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            en_linea: Arc::new(AtomicBool::new(true)),
        };

        cafeteria
            .data_socket
            .set_read_timeout(Some(TIMEOUT))
            .map_err(|e| e.to_string())?;
        cafeteria
            .election_socket
            .set_read_timeout(Some(TIMEOUT))
            .map_err(|e| e.to_string())?;

        Ok(cafeteria)
    }

    /// Inicia la ejecución de la cafetería.
    pub fn run(&mut self) -> Result<(), String> {
        let mut handles = Vec::new();
        let file = File::open(&self.pedidos_path)
            .map_err(|_| format!("No se pudo abrir el archivo {}", &self.pedidos_path))?;
        let reader = std::io::BufReader::new(file);

        for _ in 0..CANT_CAFETERAS {
            let mut clone = self.clone();
            handles.push(thread::spawn(move || {
                cafetera(&mut clone);
            }));
        }

        let mut clone = self.clone();
        handles.push(thread::spawn(move || clone.responder()));
        self.pedir_info((self.id + 1) % CANT_CAFETERIAS);

        let clone = self.clone();
        handles.push(thread::spawn(move || {
            clone.recibir_mensajes();
        }));

        let mut clone = self.clone();
        handles.push(thread::spawn(move || {
            clone.esperar_acks_pedidos();
        }));

        leer_pedidos(reader, &self.pedidos);

        for handle in handles {
            if handle.join().is_err() {
                println!("[WARN] Error en el join de un thread");
            }
        }

        Ok(())
    }

    /// Envia las sumas pendientes al coordinador y espera por los ACKs
    fn esperar_acks_pedidos(&mut self) {
        loop {
            if self.en_linea.load(Ordering::Relaxed) {
                let (sumas_lock, sumas_cvar) = &*self.sumas_pendientes;
                let (ack_lock, ack_cvar) = &*self.data_ack;
                let mut sumas = sumas_cvar
                    .wait_while(sumas_lock.lock().unwrap(), |sumas| sumas.is_empty())
                    .unwrap();
                for suma in sumas.iter() {
                    self.data_socket
                        .send_to(suma, address_data(self.obtener_coordinador()))
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
                    println!("[NODO {}] No se recibió ACK", self.id);
                    println!(
                        "[NODO {}] coordinador {} murio",
                        self.id,
                        self.coordinador.0.lock().unwrap().unwrap()
                    );
                    *(self.coordinador.0.lock().unwrap()) = None;
                    self.empezar_eleccion();
                    let coordinador = self.obtener_coordinador();
                    println!("[NODO {}] encontre coordinador {}", self.id, coordinador);
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
    fn recibir_mensajes(&self) {
        let mut buffer: [u8; BUFFER];
        let mut restas_pendientes: HashMap<u16, (u8, u32)> = HashMap::new();
        let mut transacciones_ack = Vec::new();
        loop {
            buffer = [0; BUFFER];
            let response = self.data_socket.recv_from(&mut buffer);
            if let Ok(resp) = response {
                if !self.en_linea.load(Ordering::Relaxed) {
                    let next_id = (self.id + 1) % CANT_CAFETERAS;

                    self.pedir_info(next_id);
                } else {
                    let data = obtener_data_paquete(&buffer);
                    match data.accion {
                        ACK => self.recibir_ack(&self.data_ack, data),
                        SUMAR_PUNTOS => self.sumar_puntos(data, &mut transacciones_ack, resp.1),
                        PREPARE_RESTAR_PUNTOS => {
                            self.bloquear_puntos(data, &mut restas_pendientes, resp.1)
                        }
                        COMMIT_RESTAR_PUNTOS => self.restar_puntos(data, &mut restas_pendientes),
                        OK | ABORT => self.recibir_estado_transaccion(data, &self.transacciones),
                        INFO => self.actualizar_info(data),
                        PEDIR_INFO => self.responder_info(resp.1),
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
    /// * `buffer` - Buffer de datos
    /// * `transacciones_ack` - Vector de transacciones ya procesadas
    /// * `address` - Direccion de la cafetería que envió el mensaje
    fn sumar_puntos(&self, data: Mensaje, transacciones_ack: &mut Vec<u16>, address: SocketAddr) {
        if self.obtener_coordinador() == self.id {
            if transacciones_ack.contains(&data.transaccion) {
                self.enviar_respuesta_control(Some(data.transaccion), address, ACK);
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
            self.enviar_respuesta_control(Some(data.transaccion), address, ACK);
            self.broadcast_info(data.cuenta, (puntos_actuales).0);
            transacciones_ack.push(data.transaccion);
        }
    }

    /// Envia una respuesta de control a una cafetería, esta puede ser un ACK, OK, ABORT o INFO_ACK
    ///
    /// # Argumentos
    ///
    /// * `id` - ID de la transacción
    /// * `address` - Direccion de la cafetería a la que se le envía el mensaje
    /// * `tipo` - Tipo de mensaje a enviar
    fn enviar_respuesta_control(&self, id: Option<u16>, address: SocketAddr, tipo: u8) {
        let msg = construir_paquete_data(tipo, id, None, None, None);
        self.data_socket.send_to(&msg, address).unwrap();
    }

    /// Bloquea los puntos luego de recibir un PREPARE y responde con un OK o ABORT dependiendo de si hay suficientes puntos
    ///
    /// # Argumentos
    ///
    /// * `buffer` - Buffer de datos
    /// * `restas_pendientes` - Mapa de restas pendientes
    /// * `address` - Direccion de la cafetería que envió el mensaje
    fn bloquear_puntos(
        &self,
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
                self.enviar_respuesta_control(Some(data.transaccion), address, OK);
            } else {
                self.enviar_respuesta_control(Some(data.transaccion), address, ABORT);
            }
        }
    }

    /// Resta puntos a una cuenta y envia la informacion actualizada
    ///
    /// # Argumentos
    ///
    /// * `buffer` - Buffer de datos
    /// * `restas_pendientes` - Mapa de restas pendientes
    fn restar_puntos(&self, data: Mensaje, restas_pendientes: &mut HashMap<u16, (u8, u32)>) {
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
            self.broadcast_info(cuenta as u8, (puntos_actuales).0);
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
    /// * `id` - ID de la cafetería a la que se le pide la info
    fn pedir_info(&self, id: usize) {
        if id == self.id {
            return;
        }
        let msg = construir_paquete_data(PEDIR_INFO, None, None, None, None);
        self.data_socket.send_to(&msg, address_data(id)).unwrap();
        loop {
            let mut buffer = [0; BUFFER];
            let response = self.data_socket.recv_from(&mut buffer);
            let data = obtener_data_paquete(&buffer);
            if let Ok(res) = response {
                if data.accion == INFO_ACK {
                    println!("[NODO {}] Recibido INFO_ACK", self.id);
                    self.en_linea.store(true, Ordering::Relaxed);
                    let mut clone = self.clone();
                    thread::spawn(move || clone.empezar_eleccion());
                    break;
                } else if data.accion == INFO {
                    let mut cuentas = self.cuentas.lock().unwrap();
                    cuentas.insert(data.cuenta, (data.puntos, data.timestamp));
                    for (cuenta, puntos) in cuentas.iter() {
                        println!("[NODO {}] Cuenta {}: {}", self.id, cuenta, puntos.0);
                    }
                } else if data.accion == PEDIR_INFO && self.en_linea.load(Ordering::Relaxed) {
                    println!("[NODO {}] Enviando info", self.id);
                    self.enviar_respuesta_control(None, res.1, INFO_ACK);
                }
            } else {
                println!("[NODO {}] No se pudo conectar con el nodo {}", self.id, id);
                self.pedir_info((id + 1) % CANT_CAFETERAS);
                break;
            }
        }
    }

    /// Responde a una petición de info
    ///
    /// # Argumentos
    ///
    /// * `address` - Dirección del nodo que pidió la info
    fn responder_info(&self, address: SocketAddr) {
        let cuentas = self.cuentas.lock().unwrap();
        println!("[NODO {}] Enviando info a {}", self.id, address);
        for (cuenta, puntos) in cuentas.iter() {
            let msg =
                construir_paquete_data(INFO, None, Some(*cuenta), Some(puntos.0), Some(puntos.1));
            self.data_socket.send_to(&msg, address).unwrap();
        }
        self.enviar_respuesta_control(None, address, INFO_ACK);
    }

    /// Envia un mensaje de info a todas las cafeterías
    ///
    /// # Argumentos
    ///
    /// * `cuenta` - Cuenta a la que se le actualizó la info
    /// * `puntos` - Puntos de la cuenta
    fn broadcast_info(&self, cuenta: u8, puntos: u32) {
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
                self.data_socket.send_to(&msg, address_data(i)).unwrap();
            }
        }
    }

    /// Responde los diferentes mensajes de control de elección de coordinador
    fn responder(&mut self) {
        loop {
            let mut buf = [0; BUFFER_ELECCION];
            if let Ok((_, id_sender)) = self.election_socket.recv_from(&mut buf) {
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
    }

    /// Envia un mensaje de elección al siguiente nodo en el ring y espera un ACK. En caso
    /// de timeout lo envía al siguiente hasta recibir un ACK o que ningún nodo responda,
    /// en cuyo caso se asume que está desconectado y se pone `self.en_linea` en false.
    ///
    /// # Argumentos
    ///
    /// * `paquete` - paquete a enviar
    /// * `id` - id del nodo siguiente
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

    /// Espera hasta que se haya definido un coordinador y devuelve su id.
    pub fn obtener_coordinador(&self) -> usize {
        self.coordinador
            .1
            .wait_while(self.coordinador.0.lock().unwrap(), |coordinador| {
                coordinador.is_none()
            })
            .unwrap()
            .unwrap()
    }

    /// Comienza una elección de coordinador mediante un mensaje [ELECTION].
    fn empezar_eleccion(&mut self) {
        println!("[INFO] Nodo {} empezando eleccion", self.id);
        self.enviar_al_siguiente(&construir_paquete_eleccion(b'E', &[self.id]), self.id);
    }
}
