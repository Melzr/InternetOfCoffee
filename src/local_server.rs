// use std::sync::{Condvar, Arc, Mutex};
// use std::time::Duration;
// use std::thread;

// use crate::constants::CANT_CAFETERAS;

// #[derive(Debug)]
// pub struct Pedido {
//   cuenta: u32,
//   puntos: i32,
// }

// pub struct PedidosData {
//   pub pedidos: Vec<Pedido>,
//   pub termino: bool,
// }

// pub struct LocalServer {
//   pub id: u32,
//   pub pedidos: Arc<(Mutex<PedidosData>, Condvar)>,
//   pub peers: Arc<Mutex<Vec<u32>>>,
//   pub cuentas: Arc<Mutex<Vec<u32>>>,
// }

// impl LocalServer {
//   pub fn new(id: u32, peers: Vec<u32>) -> LocalServer {
//     LocalServer {
//       id,
//       pedidos: Arc::new((Mutex::new(PedidosData { pedidos: Vec::new(), termino: false }), Condvar::new())),
//       peers: Arc::new(Mutex::new(peers)),
//       cuentas: Arc::new(Mutex::new(Vec::new())),
//     }
//   }

//   pub fn run(&mut self) {
//     let mut handles = vec![];

//     let mut clone = server.clone();
//     handles.push(thread::spawn(move || clone.responder()));

//     for _ in 0..CANT_CAFETERAS {
//       let pedidos_local = self.pedidos.clone();
//       handles.push(thread::spawn(move || {
//         Self::cafetera(pedidos_local);
//       }));
//     }

//     for i in 0..10 {
//       self.pedidos.0.lock().unwrap().pedidos.push(Pedido { cuenta: i, puntos: i * 10 });
//       self.pedidos.1.notify_one();
//     }

//     self.pedidos.0.lock().unwrap().termino = true;
//     self.pedidos.1.notify_all();

//     for handle in handles {
//       handle.join().unwrap();
//     }
//   }

//   pub fn cafetera(pedidos: Arc<(Mutex<PedidosData>, Condvar)>) {
//     let (lock, cvar) = &*pedidos;

//     loop {
//       let mut pedido = Pedido { n: 0 };
//       if let Ok(mut state) = cvar.wait_while(lock.lock().unwrap(), |pedidos_data| !(
//         pedidos_data.pedidos.first().is_some() || (pedidos_data.pedidos.len() == 0 && pedidos_data.termino))
//       ) {
//         if state.pedidos.len() == 0 {
//           break;
//         }
//         pedido = state.pedidos.pop().unwrap();
//       }
//       thread::sleep(Duration::from_secs(1));
//       println!("Cafetera: Pedido listo {:?}", pedido);
//     }
//   }
// }
