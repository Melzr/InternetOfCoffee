use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::io::BufRead;
use std::time::Duration;
use std::thread;

/// Tiempo entre pedidos
pub const TIEMPO_PEDIDO: Duration = Duration::from_secs(1);

pub type Pedidos = Arc<(Mutex<VecDeque<Pedido>>, Condvar)>;

#[derive(Debug)]
pub struct Pedido {
    pub id: usize,
    /// id de la cuenta que realiza el pedido
    pub cuenta: u32,
    /// cantidad de puntos a restar o sumar
    pub puntos: i32
}

/// Lee pedidos del archivo dado por `path` y los encola en `pedidos.0`, notificando a la condvar.
/// 
/// # Formato del archivo
/// Cada línea del archivo se interpreta como un [Pedido] con el formato `id_cuenta;puntos`.
/// El id del pedido será el número de línea.
pub fn leer_pedidos(path: &str, pedidos: &Pedidos) {
    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::new(file);
    for (id_pedido, line) in reader.lines().enumerate() {
        let line = line.unwrap();
        let mut split = line.split(';');
        let id_cuenta = split.next().unwrap().parse::<u32>().unwrap();
        let puntos = split.next().unwrap().parse::<i32>().unwrap();
        let pedido = Pedido { id: id_pedido, cuenta: id_cuenta, puntos };
        pedidos.0.lock().unwrap().push_back(pedido);
        pedidos.1.notify_one();
        thread::sleep(TIEMPO_PEDIDO);
    }
}
