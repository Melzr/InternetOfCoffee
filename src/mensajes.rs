pub const OK: u8 = b'O';
pub const ABORT: u8 = b'X';
pub const ACK: u8 = b'A';
pub const SUMAR_PUNTOS: u8 = b'S';
pub const PREPARE_RESTAR_PUNTOS: u8 = b'R';
pub const COMMIT_RESTAR_PUNTOS: u8 = b'C';
pub const INFO: u8 = b'I';
pub const ELECTION: u8 = b'E';
pub const COORDINATOR: u8 = b'C';
pub const PEDIR_INFO: u8 = b'P';
pub const INFO_ACK: u8 = b'K';

#[derive(PartialEq, Eq)]
pub enum EstadoTransaccion {
    Ok,
    Abort
}
