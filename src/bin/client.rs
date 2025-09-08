use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow_array::{Array, RecordBatch};
use arrow_flight::{flight_service_client::FlightServiceClient, FlightData, Ticket};
use bytes::Bytes;
use tonic::Request;

// IPC: para reconstruir el Schema a partir del primer FlightData
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::{root_as_message, Message};
use arrow_ipc::MessageHeader;
use arrow_schema::Schema;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(true) // quita el nombre del crate si molesta
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .init();
    let mut client = FlightServiceClient::connect("http://127.0.0.1:5005").await?;

    // Enviamos un Ticket vacío (Bytes)
    let mut stream = client
        .do_get(Request::new(Ticket {
            ticket: Bytes::from("table=t1"), //ticket: Bytes::from("table=t2"),
        }))
        .await?
        .into_inner();

    // 1) Primer mensaje => Schema IPC
    let fd_schema: FlightData = stream
        .message()
        .await?
        .ok_or_else(|| anyhow!("empty stream: expected schema first"))?;

    let schema = schema_from_flightdata_ipc(&fd_schema)?;
    // Diccionarios para decodificar batches
    let mut dicts: HashMap<i64, Arc<dyn Array>> = HashMap::new();

    // 2) Siguientes mensajes => diccionarios (si los hay) + batch(es)
    while let Some(fd) = stream.message().await? {
        // En 53.x, esta utilidad devuelve RecordBatch directamente
        let batch: RecordBatch =
            arrow_flight::utils::flight_data_to_arrow_batch(&fd, schema.clone(), &mut dicts)?;
        info!("{batch:?}");
    }

    Ok(())
}

fn schema_from_flightdata_ipc(fd: &FlightData) -> Result<Arc<Schema>> {
    // El schema viene en fd.data_header (Flatbuffers)
    let msg: Message =
        root_as_message(&fd.data_header[..]).map_err(|e| anyhow!("IPC: root_as_message: {e}"))?;
    if msg.header_type() != MessageHeader::Schema {
        return Err(anyhow!(
            "expected MessageHeader::Schema, got {:?}",
            msg.header_type()
        ));
    }
    let fb_schema = msg
        .header_as_schema()
        .ok_or_else(|| anyhow!("IPC: missing schema header"))?;
    Ok(Arc::new(fb_to_schema(fb_schema)))
}
