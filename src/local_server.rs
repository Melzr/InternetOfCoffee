use std::sync::{Condvar, Arc, Mutex};
use std::time::Duration;
use std::thread;

use crate::constants::CANT_CAFETERAS;

#[derive(Debug)]
pub struct Pedido {
  n: u32,
}

pub struct PedidosData {
  pub pedidos: Vec<Pedido>,
  pub termino: bool,
}

pub struct LocalServer {
  pub pedidos: Arc<(Mutex<PedidosData>, Condvar)>,
}

impl LocalServer {
  pub fn new() -> LocalServer {
    LocalServer {
      pedidos: Arc::new((Mutex::new(PedidosData { pedidos: Vec::new(), termino: false }), Condvar::new())),
    }
  }

  pub fn run(&mut self) {
    let mut handles = vec![];

    for _ in 0..CANT_CAFETERAS {
      let pedidos_local = self.pedidos.clone();
      handles.push(thread::spawn(move || {
        Self::cafetera(pedidos_local);
      }));
    }

    for i in 0..10 {
      self.pedidos.0.lock().unwrap().pedidos.push(Pedido { n: i });
      self.pedidos.1.notify_one();
    }

    self.pedidos.0.lock().unwrap().termino = true;
    self.pedidos.1.notify_all();

    for handle in handles {
      handle.join().unwrap();
    }
  }

  pub fn cafetera(pedidos: Arc<(Mutex<PedidosData>, Condvar)>) {
    let (lock, cvar) = &*pedidos;

    loop {
      let mut pedido = Pedido { n: 0 };
      if let Ok(mut state) = cvar.wait_while(lock.lock().unwrap(), |pedidos_data| !(
        pedidos_data.pedidos.first().is_some() || (pedidos_data.pedidos.len() == 0 && pedidos_data.termino))
      ) {
        if state.pedidos.len() == 0 {
          break;
        }
        pedido = state.pedidos.pop().unwrap();
      }
      thread::sleep(Duration::from_secs(1));
      println!("Cafetera: Pedido listo {:?}", pedido);
    }
  }
}
