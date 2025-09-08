use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, UInt32Array};
use arrow_flight::{flight_service_client::FlightServiceClient, FlightData, Ticket};
// IPC: para reconstruir el Schema a partir del primer FlightData
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::{root_as_message, Message, MessageHeader};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use arrow_select::{concat::concat, take::take};
use bytes::Bytes;
use tonic::{transport::Channel, Request};
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
    let dicts: HashMap<i64, Arc<dyn Array>> = HashMap::new();

    // 2) Siguientes mensajes => diccionarios (si los hay) + batch(es)
    while let Some(fd) = stream.message().await? {
        // En 53.x, esta utilidad devuelve RecordBatch directamente
        let batch: RecordBatch =
            arrow_flight::utils::flight_data_to_arrow_batch(&fd, schema.clone(), &dicts)?;
        info!("{batch:?}");
    }

    // Traer t1 y t2
    let (s1, b1) = fetch_table_batches(&mut client, "t1").await?;
    let (s2, b2) = fetch_table_batches(&mut client, "t2").await?;

    // Si vienen en varios batches, los unificamos (más simple para el join)
    let t1: RecordBatch = concat_batches(s1.clone(), &b1)?;
    let t2: RecordBatch = concat_batches(s2.clone(), &b2)?;

    // JOIN por "id"
    let joined: RecordBatch = inner_join_on_i32_id(&t1, &t2, "id", "id", "_r")?;
    println!("rows joined = {}", joined.num_rows());
    for (i, f) in joined.schema().fields().iter().enumerate() {
        println!("col {i}: {}", f.name());
    }
    // Print los valores
    let id: &Int32Array = joined
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let x: &Int32Array = joined
        .column_by_name("x")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let y: &Int32Array = joined
        .column_by_name("y")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let name: &StringArray = joined
        .column_by_name("name_r")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    for i in 0..joined.num_rows() {
        println!(
            "id={} x={} y={} name={}",
            id.value(i),
            x.value(i),
            y.value(i),
            name.value(i)
        );
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

// Trae TODOS los RecordBatch de una tabla via Flight
async fn fetch_table_batches(
    client: &mut FlightServiceClient<Channel>,
    table: &str,
) -> Result<(Arc<Schema>, Vec<RecordBatch>)> {
    let mut stream = client
        .do_get(Request::new(Ticket {
            ticket: Bytes::from(format!("table={table}")),
        }))
        .await?
        .into_inner();

    let first: FlightData = stream
        .message()
        .await?
        .ok_or_else(|| anyhow!("empty stream for {table}: expected schema first"))?;
    let schema: Arc<Schema> = schema_from_flightdata_ipc(&first)?;
    let dicts: HashMap<i64, Arc<dyn Array>> = HashMap::new();

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(fd) = stream.message().await? {
        let b: RecordBatch = arrow_flight::utils::flight_data_to_arrow_batch(&fd, schema.clone(), &dicts)?;
        batches.push(b);
    }
    Ok((schema, batches))
}

// Une por id (Int64). Devuelve left + (right sin la clave), con sufijo para
// columnas de right.
fn inner_join_on_i32_id(
    left: &RecordBatch,
    right: &RecordBatch,
    left_id: &str,
    right_id: &str,
    right_suffix: &str, // p.ej. "_r"
) -> Result<RecordBatch> {
    // 0) Columnas clave Int32
    let l_id_col = left
        .column_by_name(left_id)
        .ok_or_else(|| anyhow!("left no tiene {left_id}"))?;
    let r_id_col = right
        .column_by_name(right_id)
        .ok_or_else(|| anyhow!("right no tiene {right_id}"))?;

    if l_id_col.data_type() != &DataType::Int32 || r_id_col.data_type() != &DataType::Int32 {
        return Err(anyhow!("join por id Int32: tipos incompatibles"));
    }

    let l_id: &Int32Array = l_id_col.as_any().downcast_ref::<Int32Array>().unwrap();
    let r_id: &Int32Array = r_id_col.as_any().downcast_ref::<Int32Array>().unwrap();

    // 1) Hash de la derecha: id -> idxs
    let mut map: HashMap<i32, Vec<u32>> = HashMap::with_capacity(r_id.len());
    for j in 0..r_id.len() {
        if !r_id.is_null(j) {
            map.entry(r_id.value(j)).or_default().push(j as u32);
        }
    }

    // 2) Emparejar izquierda
    let mut left_idx = Vec::<u32>::new();
    let mut right_idx = Vec::<u32>::new();
    for i in 0..l_id.len() {
        if l_id.is_null(i) {
            continue;
        }
        if let Some(js) = map.get(&l_id.value(i)) {
            for &j in js {
                left_idx.push(i as u32);
                right_idx.push(j);
            }
        }
    }

    let l_idx = UInt32Array::from(left_idx);
    let r_idx = UInt32Array::from(right_idx);

    // 3) Construir resultado: left completo + right (sin la clave) con sufijo
    let mut out_fields: Vec<FieldRef> = Vec::new();
    let mut out_cols: Vec<ArrayRef> = Vec::new();

    // left
    for (i, f) in left.schema().fields().iter().enumerate() {
        out_fields.push(f.clone());
        out_cols.push(take(left.column(i).as_ref(), &l_idx, None)?);
    }
    // right (sin la clave)
    for (i, f) in right.schema().fields().iter().enumerate() {
        if f.name() == right_id {
            continue;
        }
        let taken = take(right.column(i).as_ref(), &r_idx, None)?;
        let nf = Arc::new(Field::new(
            format!("{}{}", f.name(), right_suffix),
            f.data_type().clone(),
            f.is_nullable(),
        ));
        out_fields.push(nf);
        out_cols.push(taken);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(out_fields)),
        out_cols,
    )?)
}

// Concatena varios batches en uno (misma schema)
fn concat_batches(schema: Arc<Schema>, batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let ncols = schema.fields().len();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(ncols);

    for c in 0..ncols {
        // tenías: Vec<Arc<dyn Array>>
        let arrays: Vec<ArrayRef> = batches.iter().map(|b| b.column(c).clone()).collect();

        // conviértelo a &[&dyn Array] para arrow-select 53.x
        let arrays_refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();

        cols.push(concat(&arrays_refs)?);
    }
    Ok(RecordBatch::try_new(schema, cols)?)
}
