use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use tokio::try_join;
use tracing::{debug, info};

// Importa los módulos desde la librería del propio crate

use flight_data_rs::{
    conf::args::Args,
    servers::{inmem, parquet},
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(args.log.parse().unwrap_or(tracing::Level::INFO))
        .with_target(true) // quita el nombre del crate si molesta
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .init();
    info!("Args: {args:?}");

    let addr_inmem: SocketAddr = "127.0.0.1:5005".parse()?;
    let addr_parquet: SocketAddr = "127.0.0.1:5006".parse()?;
    debug!("Lanzando: inmem={} parquet={}", addr_inmem, addr_parquet);

    try_join!(
        inmem::serve_inmem(addr_inmem),
        parquet::serve_parquet(addr_parquet),
    )?;

    Ok(())
}
