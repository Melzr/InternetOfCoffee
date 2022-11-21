use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::io::{BufRead, BufReader};
use std::time::Duration;
use std::thread;

/// Tiempo entre pedidos
pub const TIEMPO_PEDIDO: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct Pedido {
    pub id: usize,
    /// id de la cuenta que realiza el pedido
    pub cuenta: u32,
    /// cantidad de puntos a restar o sumar
    pub puntos: i32
}

pub struct PedidosInfo {
    pub cola_pedidos: VecDeque<Pedido>,
    pub fin: bool
}

impl PedidosInfo {
    pub fn new() -> PedidosInfo {
        PedidosInfo {
            cola_pedidos: VecDeque::new(),
            fin: false
        }
    }
}

pub type Pedidos = Arc<(Mutex<PedidosInfo>, Condvar)>;


/// Lee pedidos y los encola en `pedidos.0`, notificando a la condvar.
/// 
/// # Formato del archivo
/// Cada línea del archivo se interpreta como un [Pedido] con el formato `id_cuenta;puntos`.
/// El id del pedido será el número de línea.
pub fn leer_pedidos(reader: BufReader<std::fs::File>, pedidos: &Pedidos) {
    for (id_pedido, line) in reader.lines().enumerate() {
        if let Ok(line) = line {
            let mut split = line.split(';');
            if let (Some(cuenta), Some(puntos)) = (split.next(), split.next()) {
                if let (Ok(cuenta), Ok(puntos)) = (cuenta.parse::<u32>(), puntos.parse::<i32>()) {
                    let pedido = Pedido { id: id_pedido, cuenta, puntos};
                    match pedidos.0.lock() {
                        Ok(mut pedidos_data) => {
                            pedidos_data.cola_pedidos.push_back(pedido);
                            pedidos.1.notify_one();
                        },
                        Err(_) => {
                            println!("[ERROR] Error al obtener el lock de pedidos");
                            break;
                        }
                    }
                }
            }
        }
        
        thread::sleep(TIEMPO_PEDIDO);
    }

    match pedidos.0.lock() {
        Ok(mut pedidos_data) => {
            pedidos_data.fin = true;
            pedidos.1.notify_all();
        },
        Err(_) => {
            println!("[ERROR] Error al obtener el lock de pedidos");
        }
    }
}
