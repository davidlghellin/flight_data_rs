use clap::{command, Parser};

#[derive(Parser, Debug)]
#[command(
    name = "flight-servers",
    about = "Lanza servidores Flight: in-memory y parquet"
)]
pub struct Args {
    /// Puerto del servidor In-Memory (puede venir de env INMEM_PORT)
    #[arg(long, default_value_t = 5005)]
    pub inmem_port: u16,

    /// Puerto del servidor Parquet (puede venir de env PARQUET_PORT)
    #[arg(long, default_value_t = 5006)]
    pub parquet_port: u16,

    /// Nivel de log vía RUST_LOG (ej: debug, info, warn). Si no, usa "info".
    #[arg(long, default_value = "DEBUG")]
    pub log: String,
}
