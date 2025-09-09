use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tracing::info;

#[allow(dead_code)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .init();

    // 1) Definir el esquema
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
    ]));

    // 2) Crear un RecordBatch (dos columnas con 3 filas cada una)
    let batch: RecordBatch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 344440])),
        ],
    )?;

    // 3) Abrir un fichero de salida
    let file: File = File::create("/tmp/demo.parquet")?;

    // 4) Configurar el writer
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    // 5) Escribir batch
    writer.write(&batch)?;

    // 6) Cerrar (flush + footer)
    writer.close()?;

    info!("✅ Escrito Parquet en /tmp/demo.parquet");
    Ok(())
}
