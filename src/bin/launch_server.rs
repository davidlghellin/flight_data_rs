use anyhow::Result;
use std::net::SocketAddr;
use tokio::try_join;
use tracing::info;

// Importa los módulos desde la librería del propio crate
use flight_data_rs::servers::{inmem, parquet};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(true) // quita el nombre del crate si molesta
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .init();

    let addr_inmem: SocketAddr = "127.0.0.1:5005".parse()?;
    let addr_parquet: SocketAddr = "127.0.0.1:5006".parse()?;

    info!("Lanzando: inmem={} parquet={}", addr_inmem, addr_parquet);

    // Opción A: directo (debería inferir tipos bien)
    try_join!(
        inmem::serve_inmem(addr_inmem),
        parquet::serve_parquet(addr_parquet),
    )?;

    Ok(())
}
