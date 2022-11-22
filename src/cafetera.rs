use std::sync::atomic::Ordering;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cafeteria::Cafeteria;
use crate::constantes::TIEMPO_PREPARACION_PEDIDO;
use crate::direcciones::address_data;
use crate::mensajes_data::{
    construir_paquete_data, obtener_id_transaccion, EstadoTransaccion, COMMIT_RESTAR_PUNTOS,
    PREPARE_RESTAR_PUNTOS, SUMAR_PUNTOS,
};
use crate::pedido::Pedido;

/// Ejecución de una cafetera. Espera a recibir pedidos desde `cafeteria.pedidos` y los procesa
/// hasta que `cafeteria.pedidos.0.fin` sea true.
///
/// # Argumentos
///
/// * `cafeteria` - cafeteria a la que pertenece la cafetera
pub fn cafetera(cafeteria: &mut Cafeteria) {
    loop {
        let (lock, cvar) = &*(cafeteria.pedidos);
        let mut pedido = Pedido {
            id: 0,
            cuenta: 0,
            puntos: 0,
        };
        if let Ok(mut state) = cvar.wait_while(lock.lock().unwrap(), |pedidos| {
            pedidos.cola_pedidos.is_empty() && !pedidos.fin
        }) {
            if state.fin {
                break;
            }
            pedido = state.cola_pedidos.pop_front().unwrap();
        }
        if pedido.puntos >= 0 {
            cafetera_pedido_suma(cafeteria, pedido);
        } else {
            cafetera_pedido_resta(cafeteria, pedido);
        }
    }
}

/// Prepara un pedido de suma de puntos y pushea la informacion a `cafeteria.sumas_pendientes`.
fn cafetera_pedido_suma(cafeteria: &mut Cafeteria, pedido: Pedido) {
    thread::sleep(TIEMPO_PREPARACION_PEDIDO);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let buffer = construir_paquete_data(
        SUMAR_PUNTOS,
        Some(obtener_id_transaccion(cafeteria.id as u8, pedido.id as u8)),
        Some(pedido.cuenta as u8),
        Some(pedido.puntos.unsigned_abs()),
        Some(timestamp),
    );
    cafeteria.sumas_pendientes.0.lock().unwrap().push(buffer);
    cafeteria.sumas_pendientes.1.notify_all();
}

/// Envía al coordinador un mensaje [PREPARE_RESTAR_PUNTOS] y espera que se pushee la respuesta en
/// `transacciones`. Si su estado es [EstadoTransaccion::Ok] se prepara el pedido y se envía un
/// mensaje [COMMIT_RESTAR_PUNTOS] al coordinador una vez finalizado.
fn cafetera_pedido_resta(cafeteria: &mut Cafeteria, pedido: Pedido) {
    let coordinador = cafeteria.obtener_coordinador();
    if cafeteria.en_linea.load(Ordering::SeqCst) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let id = obtener_id_transaccion(cafeteria.id as u8, pedido.id as u8);
        let buffer = construir_paquete_data(
            PREPARE_RESTAR_PUNTOS,
            Some(id),
            Some(pedido.cuenta as u8),
            Some(pedido.puntos.unsigned_abs()),
            Some(timestamp),
        );
        cafeteria
            .data_socket
            .send_to(&buffer, address_data(coordinador))
            .unwrap();

        let (lock, cvar) = &*cafeteria.transacciones;
        let mut transaccion = None;
        if let Ok(mut state) = cvar.wait_while(lock.lock().unwrap(), |transacciones_data| {
            transacciones_data.get(&id).is_none()
        }) {
            transaccion = state.remove(&id).unwrap();
        }
        if transaccion != Some(EstadoTransaccion::Ok) {
            println!(
                "[NODO {}] Transaccion abortada: pedido {}",
                cafeteria.id, pedido.id
            );
            return;
        }

        thread::sleep(TIEMPO_PREPARACION_PEDIDO);
        let buffer = construir_paquete_data(
            COMMIT_RESTAR_PUNTOS,
            Some(obtener_id_transaccion(cafeteria.id as u8, pedido.id as u8)),
            Some(pedido.cuenta as u8),
            Some(pedido.puntos.unsigned_abs()),
            Some(timestamp),
        );
        cafeteria
            .data_socket
            .send_to(&buffer, address_data(coordinador))
            .unwrap();
    } else {
        println!(
            "[NODO {}] offline - no se puede realizar el pedido {}",
            cafeteria.id, pedido.id
        );
    }
}
