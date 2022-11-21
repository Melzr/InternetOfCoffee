use std::thread;

use coffeewards::cafeteria::Cafeteria;
use coffeewards::constantes::CANT_CAFETERIAS;

fn main() {
    let mut node_threads = vec![];

    for id in 0..CANT_CAFETERIAS {
        node_threads.push(
            thread::Builder::new()
                .name(format!("Cafeteria {}", id))
                .spawn(move || {
                    let mut node = Cafeteria::new(id, format!("./pedidos/pedidos{}.txt", id));
                    if let Err(msg) = node.run() {
                        println!("[ERROR] nodo {}: {}", id, msg);
                    }
                })
                .unwrap(),
        );
    }

    for handle in node_threads {
        if handle.join().is_err() {
            println!("[WARN] Error en el join de un nodo");
        }
    }
}
