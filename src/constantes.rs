use std::time::Duration;

/// Cantidad de cafeteras por nodo
pub const CANT_CAFETERAS: usize = 3;
/// Cantidad de nodos del sistema
pub const CANT_CAFETERIAS: usize = 4;
/// Tiempo que se esperan los ACKs
pub const TIMEOUT: Duration = Duration::from_secs(5);
