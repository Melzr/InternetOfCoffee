use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::net::UdpSocket;
use std::net::SocketAddr;
use std::mem::size_of;
use std::convert::TryInto;
use std::sync::atomic::{AtomicBool, Ordering};
use std::fs::File;
use std::io::{Write, Read};
use std::io::{BufRead, BufReader};

use crate::constants::{CANT_CAFETERIAS, ACK, SUMAR_PUNTOS, INFO};

pub const TIMEOUT: Duration = Duration::from_secs(5);

pub struct Cafeteria {
    id: usize,
    pedidos_path: String,
    coordinador: Arc<(Mutex<Option<usize>>, Condvar)>,
    ack: Arc<(Mutex<Option<usize>>, Condvar)>,
    socket: UdpSocket,
    termino: Arc<AtomicBool>,
    cuentas: Arc<Mutex<HashMap<u32, i32>>>,
    // pedido_actual: Arc<(Mutex<Option<Pedido>>, Condvar)>,
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
            cuentas: Arc::new(Mutex::new(HashMap::new()))
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
            pedidos_path: self.pedidos_path.clone()
        }
    }

    pub fn election_address(id: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], (8000 + id) as u16))
    }
    
    pub fn transacion_address(id: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], (9000 + id) as u16))
    }

    pub fn run(&mut self) {
        // let file = std::fs::File::open(&self.pedidos_path).unwrap();
        // let reader = std::io::BufReader::new(file);
        // let mut clone = self.clone();
        // thread::spawn(move || clone.leer_pedidos(reader));
        
        let socket = UdpSocket::bind(Self::transacion_address(self.id)).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        
        let mut clone = self.clone();
        thread::spawn(move || clone.responder());
        self.empezar_eleccion();
        let clone = self.clone();
        let ack = Arc::new((Mutex::new(false), Condvar::new()));
        let ack2 = ack.clone();
        let socket_clone = socket.try_clone().unwrap();
        thread::spawn(move || clone.recibir_mensajes(&(socket.try_clone().unwrap()), ack2));
        loop {
            let puntos_bytes: [u8; 4] = ((100 + (self.id * 100)) as i32).to_be_bytes();
            let buffer = [SUMAR_PUNTOS, self.id as u8, puntos_bytes[0], puntos_bytes[1], puntos_bytes[2], puntos_bytes[3]];
            let (lock, cvar) = &*ack;
            socket_clone.send_to(&buffer, Self::transacion_address(self.obtener_coordinador())).unwrap();
            let condvar_resp = cvar.wait_timeout_while(lock.lock().unwrap(), TIMEOUT, |ack| !*ack).unwrap();
            if condvar_resp.1.timed_out() {
                println!("[NODO {}] No se recibió ACK", self.id);
                println!("[NODO {}] coordinador {} murio", self.id, self.coordinador.0.lock().unwrap().unwrap());
                *self.coordinador.0.lock().unwrap() = None;
                self.empezar_eleccion();
                println!("[NODO {}] encontre coordinador {}", self.id, self.coordinador.0.lock().unwrap().unwrap());
            }
            thread::sleep(Duration::from_secs(5));
        }
    }

    fn leer_pedidos(&mut self, reader: std::io::BufReader<std::fs::File>) {
        for line in reader.lines() {
            let line = line.unwrap();
            let mut split = line.split(',');
            let id = split.next().unwrap().parse::<u32>().unwrap();
            let puntos = split.next().unwrap().parse::<i32>().unwrap();
            println!("id: {}, puntos: {}", id, puntos);
        }
    }

    fn recibir_mensajes (&self, socket: &UdpSocket, ack: Arc<(Mutex<bool>, Condvar)>) {
        let mut buffer: [u8; 6];
        loop {
            buffer = [0; 6];
            let response = socket.recv_from(&mut buffer);
            if response.is_ok() {
                match buffer[0] {
                    ACK => {
                        println!("[NODO {}] ACK recibido", self.id);
                        *ack.0.lock().unwrap() = true;
                        ack.1.notify_one();
                    }
                    SUMAR_PUNTOS => {
                        if self.obtener_coordinador() == self.id {
                            let cuenta = buffer[1];
                            let puntos = i32::from_be_bytes(buffer[2..].try_into().unwrap());
                            println!("[COORDINADOR {}] Sumar {} puntos a la cuenta {}", self.id, puntos, cuenta);
                            let mut cuentas = self.cuentas.lock().unwrap();
                            let puntos_actuales = cuentas.entry(cuenta as u32).or_insert(0);
                            *puntos_actuales += puntos as i32;
                            println!("[COORDINADOR {}] Puntos nuevos de la cuenta {}: {}", self.id, cuenta, puntos_actuales);
                            socket.send_to(&[ACK, 0, 0, 0, 0, 0], response.unwrap().1).unwrap();
                            self.broadcast_info(&socket, cuenta, *puntos_actuales);
                        }
                    }
                    INFO => {
                        let cuenta = buffer[1];
                        let puntos = i32::from_be_bytes(buffer[2..].try_into().unwrap());
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

    fn broadcast_info (&self, socket: &UdpSocket, cuenta: u8, puntos: i32) {
        let puntos_bytes: [u8; 4] = puntos.to_be_bytes();
        let buffer = [INFO, cuenta, puntos_bytes[0], puntos_bytes[1], puntos_bytes[2], puntos_bytes[3]];
        for i in 0..CANT_CAFETERIAS {
            if i != self.id {
                println!("[COORDINADOR {}] Enviando INFO a la cafetería {}", self.id, i);
                socket.send_to(&buffer, Self::transacion_address(i)).unwrap();
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
            ids.push(usize::from_le_bytes(buf[i..(size_of::<usize>() + i)].try_into().unwrap()));
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
                    println!("Nodo {} recibi ELECTION de {} contenido {:?}", self.id, id_sender, ids);
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
                    println!("Nodo {} recibi COORDINATOR de {} contenido {:?}", self.id, id_sender, ids);
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
                    println!("Nodo {} -  Nuevo lider {}", self.id, self.coordinador.0.lock().unwrap().unwrap());
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
            // offline -> manejar
        }
        *self.ack.0.lock().unwrap() = None;
        self.socket.send_to(paquete, Self::election_address(siguiente));
        let ack = self.ack.1.wait_timeout_while(
            self.ack.0.lock().unwrap(),
            TIMEOUT,
            |ack| ack.is_none() || ack.unwrap() != siguiente,
        );
        if ack.unwrap().1.timed_out() {
            self.enviar_al_siguiente(paquete, siguiente)
        }
    }

    fn obtener_coordinador(&self) -> usize {
        self.coordinador.1.wait_while(
            self.coordinador.0.lock().unwrap(),
            |coordinador| coordinador.is_none(),
        ).unwrap().unwrap()
    }

    fn empezar_eleccion(&mut self) {
        println!("[INFO] Nodo {} empezando eleccion", self.id);

        self.enviar_al_siguiente(&self.construir_paquete(b'E', &[self.id]), self.id);
        self.coordinador.1.wait_while(
            self.coordinador.0.lock().unwrap(),
            |coordinador| coordinador.is_none(),
        );
    }
}
