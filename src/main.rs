use std::thread;

use coffeewards::cafeteria::Cafeteria;
use coffeewards::constants::CANT_CAFETERIAS;

fn main() {
    let mut node_threads = vec![];

    for id in 0..CANT_CAFETERIAS {
        node_threads.push(
            thread::Builder::new()
                .name(format!("Cafeteria {}", id))
                .spawn(move || {
                    let mut node = Cafeteria::new(id);
                    node.run()
                })
                .unwrap(),
        );
    }

    for handle in node_threads {
        handle.join().unwrap();
    }
}
