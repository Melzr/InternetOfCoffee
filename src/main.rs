use std::thread;

use coffeewards::cafeteria::Cafeteria;
use coffeewards::constantes::CANT_CAFETERIAS;

fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    let mut node_threads = vec![];

    if args.len() > 1 {
        let id = args[1].parse::<usize>().unwrap();
        if id >= CANT_CAFETERIAS {
            eprintln!("[ERROR] El id del nodo debe ser menor a la cantidad de nodos");
            return;
        }
        node_threads.push(
            thread::Builder::new()
                .name(format!("Cafeteria {}", id))
                .spawn(move || {
                    match Cafeteria::new(id, format!("./pedidos/pedidos{}.txt", id)) {
                        Ok(mut cafeteria) => {
                            if let Err(msg) = cafeteria.run() {
                                println!("[ERROR] nodo {}: {}", id, msg);
                            }
                        }
                        Err(e) => {
                            eprintln!("[ERROR] no se pudo crear la cafeteria {}: {}", id, e);
                        }
                    }
                })
                .unwrap(),
        );
    } else {
        for id in 0..CANT_CAFETERIAS {
            node_threads.push(
                thread::Builder::new()
                    .name(format!("Cafeteria {}", id))
                    .spawn(move || {
                        match Cafeteria::new(id, format!("./pedidos/pedidos{}.txt", id)) {
                            Ok(mut cafeteria) => {
                                if let Err(msg) = cafeteria.run() {
                                    println!("[ERROR] nodo {}: {}", id, msg);
                                }
                            }
                            Err(e) => {
                                eprintln!("[ERROR] no se pudo crear la cafeteria {}: {}", id, e);
                            }
                        }
                    })
                    .unwrap(),
            );
        }
    }

    for handle in node_threads {
        if handle.join().is_err() {
            println!("[WARN] Error en el join de un nodo");
        }
    }
}
