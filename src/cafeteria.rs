use std::thread;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use std::net::UdpSocket;
use std::net::SocketAddr;
use std::mem::size_of;
use std::convert::TryInto;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::constants::CANT_CAFETERIAS;

pub const TIMEOUT: Duration = Duration::from_secs(5);

pub struct Cafeteria {
    id: usize,
    coordinador: Arc<(Mutex<Option<usize>>, Condvar)>,
    ack: Arc<(Mutex<Option<usize>>, Condvar)>,
    socket: UdpSocket,
    termino: Arc<AtomicBool>,
}

impl Cafeteria {
    pub fn new(id: usize) -> Cafeteria {
        Cafeteria {
            id,
            coordinador: Arc::new((Mutex::new(None), Condvar::new())),
            ack: Arc::new((Mutex::new(None), Condvar::new())),
            socket: UdpSocket::bind(Self::election_address(id)).unwrap(),
            termino: Arc::new(AtomicBool::new(false)),
        }
    }

    fn clone(&self) -> Cafeteria {
        Cafeteria {
            id: self.id,
            coordinador: self.coordinador.clone(),
            ack: self.ack.clone(),
            socket: self.socket.try_clone().unwrap(),
            termino: self.termino.clone(),
        }
    }

    pub fn election_address(id: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], (8000 + id) as u16))
    }
    
    pub fn transacion_address(id: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], (9000 + id) as u16))
    }

    pub fn run(&mut self) {
        let socket = UdpSocket::bind(Self::transacion_address(self.id)).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut clone = self.clone();
        thread::spawn(move || clone.responder());
        self.empezar_eleccion();
        
        loop {
            if self.obtener_coordinador() == self.id {
                // soy el coordinador
                break;
            } else {
                let mut buf: [u8; 1] = [0; 1];
                socket.send_to(&buf, Self::transacion_address(self.coordinador.0.lock().unwrap().unwrap())).unwrap();
                if socket.recv_from(&mut buf).is_err() {
                    println!("Nodo {} coordinador {} murio", self.id, self.coordinador.0.lock().unwrap().unwrap());
                    // abortar las restas de puntos que estoy haciendo, analizarlo
                    *self.coordinador.0.lock().unwrap() = None;
                    self.empezar_eleccion();
                    println!("Nodo {} encontre coordinador {}", self.id, self.coordinador.0.lock().unwrap().unwrap());
                }
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
