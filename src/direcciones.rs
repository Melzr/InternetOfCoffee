use std::net::SocketAddr;

const PUERTOS_ELECCION: usize = 8000;
const PUERTOS_DATA: usize = 9000;

/// Obtener la address donde un nodo con el id dado escucha mensajes relacionados
/// a la elección de un coordinador.
pub fn address_eleccion(id: usize) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], (PUERTOS_ELECCION + id) as u16))
}

/// Obtener la address donde un nodo con el id dado escucha mensajes relacionados
/// a la información de cuentas y transacciones.
pub fn address_data(id: usize) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], (PUERTOS_DATA + id) as u16))
}
