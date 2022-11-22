use std::mem::size_of;

use crate::constantes::CANT_CAFETERIAS;

pub const ELECTION: u8 = b'E';
pub const COORDINATOR: u8 = b'C';
pub const ACK: u8 = b'A';

pub const BUFFER_ELECCION: usize =
    1 + size_of::<usize>() + (CANT_CAFETERIAS + 1) * size_of::<usize>();

/// Construye un buffer con los datos recibidos.
pub fn construir_paquete_eleccion(accion: u8, ids: &[usize]) -> Vec<u8> {
    let mut paquete = vec![accion];
    paquete.extend_from_slice(&ids.len().to_le_bytes());
    for id in ids {
        paquete.extend_from_slice(&id.to_le_bytes());
    }
    paquete
}

/// Parsea el buffer recibido.
pub fn obtener_ids(buf: &[u8]) -> Vec<usize> {
    let mut ids = Vec::new();
    let n = usize::from_le_bytes(buf[1..(size_of::<usize>() + 1)].try_into().unwrap());
    let mut i = size_of::<usize>() + 1;

    for _ in 0..n {
        ids.push(usize::from_le_bytes(
            buf[i..(size_of::<usize>() + i)].try_into().unwrap(),
        ));
        i += size_of::<usize>();
    }

    ids
}
