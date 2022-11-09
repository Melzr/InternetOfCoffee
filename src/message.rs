use actix::Message;

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Pedido(pub i32);

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct PedidoListo(pub bool);