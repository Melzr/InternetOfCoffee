pub const OK: u8 = b'O';
pub const ABORT: u8 = b'X';
pub const SUMAR_PUNTOS: u8 = b'S';
pub const PREPARE_RESTAR_PUNTOS: u8 = b'R';
pub const COMMIT_RESTAR_PUNTOS: u8 = b'C';
pub const INFO: u8 = b'I';
pub const PEDIR_INFO: u8 = b'P';
pub const INFO_ACK: u8 = b'K';

pub const BUFFER: usize = 24;

#[derive(PartialEq, Eq)]
pub enum EstadoTransaccion {
    Ok,
    Abort,
}

pub struct Mensaje {
    pub accion: u8,
    pub transaccion: u16,
    pub cuenta: u8,
    pub puntos: u32,
    pub timestamp: u128,
}

pub fn cafeteria_id(transaccion: u16) -> u8 {
    (transaccion >> 8) as u8
}

pub fn pedido_id(transaccion: u16) -> u8 {
    (transaccion & 0x00FF) as u8
}

pub fn obtener_id_transaccion(cafeteria: u8, pedido: u8) -> u16 {
    u16::from_be_bytes([cafeteria, pedido])
}

pub fn construir_paquete_data(
    action: u8,
    transaccion: Option<u16>,
    cuenta: Option<u8>,
    puntos: Option<u32>,
    timestamp: Option<u128>,
) -> [u8; BUFFER] {
    let mut buffer: [u8; 24] = [0; 24];
    buffer[0] = action;
    if let Some(transaccion) = transaccion {
        buffer[1..=2].copy_from_slice(&transaccion.to_be_bytes());
    }
    if let Some(cuenta) = cuenta {
        buffer[3] = cuenta;
    }
    if let Some(puntos) = puntos {
        buffer[4..=7].copy_from_slice(&puntos.to_be_bytes());
    }
    if let Some(timestamp) = timestamp {
        buffer[8..=23].copy_from_slice(&timestamp.to_be_bytes());
    }
    buffer
}

pub fn obtener_data_paquete(buffer: &[u8; 24]) -> Mensaje {
    let accion = buffer[0];
    let transaccion = u16::from_be_bytes([buffer[1], buffer[2]]);
    let cuenta = buffer[3];
    let puntos = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
    let timestamp = u128::from_be_bytes([
        buffer[8], buffer[9], buffer[10], buffer[11], buffer[12], buffer[13], buffer[14],
        buffer[15], buffer[16], buffer[17], buffer[18], buffer[19], buffer[20], buffer[21],
        buffer[22], buffer[23],
    ]);
    Mensaje {
        accion,
        transaccion,
        cuenta,
        puntos,
        timestamp,
    }
}
