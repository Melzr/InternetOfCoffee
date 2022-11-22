# Coffeewards

| Nombre               | Padrón  |
|----------------------|---------|
| Castro, Nahuel Elias | 106551  |
| Lazzaro, Melina      | 105931  |

### Introducción

En este trabajo práctico, se desarrolló la continuación de **Internet of Coffee.** En este caso, se debe resolver un sistema de puntos para fidelización de los clientes donde por cada compra que realizan suman puntos que luego pueden canjear por cafés gratuitos. Este sistema debe ser concurrente, ya que un cliente puede estar usando su cuenta en varias cafeterías a la vez y los puntos deben actualizarse correctamente.

### Ejecución

Existen dos maneras de ejecutar el programa:

#### Ejecución de una cafetería

```
cargo run <id_cafeteria>
```

Inicia la ejecución de una cafetería con el `<id_cafeteria> `ingresado, que puede ser un número de 0 a `CANT_CAFETERIAS` sin incluír.

#### Ejecución de todo el sistema

```
cargo run
```

Inicia la ejecución de todas las cafeterías del sistema.

## Implementación

### Cafeterías

Las cafeterías se comunican mediante el protocolo UDP donde cada una se conecta desde dos direcciones: una por donde intercambia mensajes relacionados con la elección de un coordinador y otra donde intercambia mensajes con información sobre las cuentas. Ambas direcciones de todas las cafeterías son conocidas de antemano por las demás.

Cada cafetería se encarga de procesar los pedidos leídos del archivo `/pedidos/pedidos<id>.txt` donde `<id>` es el id de la cafetería y cada línea del mismo representa un pedido con el formato `id_cuenta;puntos`.

### Sincronización

Para sincronizar el estado de las cuentas una de las cafeterías es elegida como coordinadora mediante el **algoritmo ring**, que implementan de la siguiente manera:

1. Al iniciarse una cafetería arma un mensaje **ELECTION** con su id y se lo envía a la siguiente cafetería del ring.
2. La siguiente cafetería responde con un **ACK**, agrega su id al final del mensaje y lo envía a la siguiente. En caso de no recibir un ACK luego de un timeout se envía el mensaje ELECTION a la cafetería que sigue. De esta forma el mensaje recorre todo el ring.
3. Cuando el mensaje **ELECTION** llega a la cafetería que comenzó la elección, esta detecta la mayor id de la lista y arma un mensaje **COORDINATOR** con dicha id y lo manda a la siguiente.
4. El mensaje **COORDINATOR** circula por todas las cafeterías de la misma manera que **ELECTION** y de esta forma todas las cafeterías logran identifiacar al coordinador.

Esta elección vuelve a ser iniciada por una cafetería en caso de no poder comunicarse con el coordinador.

Una cafetería pasará a funcionar en modo fuera de línea al no poder comunicarse con ninguna otra. Al volver a conectarse solicitará la información de todas las cuentas a la cafetería siguiente para seguir funcionando de manera sincronizada.

### Procesamiento de pedidos

Cada pedido requerirá realizar una de las siguientes acciones:

 - **Sumar puntos**: en este caso se prepara el café y luego se le informa al coordinador que debe realizar una suma de puntos. Este actualiza la información de la cuenta correspondiente y hace un broadcast de la misma mediante un mensaje **INFO**.
 - **Restar puntos**: antes de comenzar a preparar el café, se le envía al coordinador un mensaje **PREPARE**. Este va a responder con un **OK** o un **ABORT** dependiendo de si hay puntos suficientes para realizar el pedido. En caso de recibir un OK la cafetería prepara el café y luego envía un mensaje **COMMIT** al coordinador, que al recibirlo se va a encargar de restar los puntos correspondientes y hacer un broadcast de la cuenta actualizada mediante un mensaje **INFO**.
 
Al funcionar en modo fuera de línea una cafetería podrá realizar sumas de puntos, que serán acumulados hasta obtener un nuevo coordinador, pero no se podrán realizar pedidos que involucren restar puntos. Al volver al conectarse se enviarán todas las sumas de puntos acumuladas.
