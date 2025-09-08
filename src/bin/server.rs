use std::process::Command;
use std::thread;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::SchemaAsIpc;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaResult,
    Ticket,
};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

struct InMem {
    tables: HashMap<String, RecordBatch>,
}

#[tonic::async_trait]
impl FlightService for InMem {
    // OJO: usar los tipos públicos de tokio_stream
    type HandshakeStream = tokio_stream::Once<Result<HandshakeResponse, Status>>;
    type ListFlightsStream = tokio_stream::Empty<Result<FlightInfo, Status>>;
    type DoGetStream = ReceiverStream<Result<FlightData, Status>>;
    type DoPutStream = tokio_stream::Empty<Result<PutResult, Status>>;
    type DoActionStream = tokio_stream::Empty<Result<FlightResult, Status>>;
    type ListActionsStream = tokio_stream::Empty<Result<ActionType, Status>>;
    type DoExchangeStream = tokio_stream::Empty<Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _req: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(tokio_stream::once(Ok(HandshakeResponse {
            protocol_version: 0,
            // OJO: usar los tipos públicos de tokio_stream
            payload: Bytes::new(),
        }))))
    }

    // Métodos requeridos por el trait en Arrow 53.x
    // Mantengo sin implementar para simplificar la demo
    async fn get_schema(
        &self,
        _req: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _req: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _req: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn list_flights(
        &self,
        _req: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        // --- 1) Elegir la "tabla" desde el Ticket ---
        let raw: Bytes = req.into_inner().ticket; // Bytes
        let s: &str = std::str::from_utf8(&raw).unwrap_or_default();
        // formato esperado: "table=t1" o "table=t2"
        let table_name: &str = s.strip_prefix("table=").unwrap_or(s);

        let batch: RecordBatch = self
            .tables
            .get(table_name)
            .ok_or_else(|| Status::not_found(format!("table not found: {table_name}")))?
            .clone();

        // --- 2) Serializar: Schema -> diccionarios -> batch ---
        let opts: IpcWriteOptions = IpcWriteOptions::default();

        // schema
        let schema_fd: FlightData = SchemaAsIpc::new(batch.schema().as_ref(), &opts).into();

        // diccionarios + batch
        let gen: IpcDataGenerator = IpcDataGenerator::default();
        let mut dict_tracker: DictionaryTracker =
            DictionaryTracker::new_with_preserve_dict_id(false, opts.preserve_dict_id());
        let (encoded_dicts, encoded_batch) = gen
            .encoded_batch(&batch, &mut dict_tracker, &opts)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Convertir a FlightData
        let dict_fds: Vec<FlightData> = encoded_dicts.into_iter().map(Into::into).collect();
        let batch_fd: FlightData = encoded_batch.into();

        // --- 3) Enviar por stream ---
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send(Ok(schema_fd))
            .await
            .map_err(|_| Status::internal("tx closed"))?;
        for d in dict_fds {
            tx.send(Ok(d))
                .await
                .map_err(|_| Status::internal("tx closed"))?;
        }
        // Enviar batch
        tx.send(Ok(batch_fd))
            .await
            .map_err(|_| Status::internal("tx closed"))?;

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn do_put(
        &self,
        _req: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }

    async fn do_action(
        &self,
        _req: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }

    async fn list_actions(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }

    async fn do_exchange(
        &self,
        _req: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // basic conf logger
    //tracing_subscriber::fmt()
    //    .with_max_level(tracing::Level::DEBUG)
    //    .init();
    tracing_subscriber::fmt()
        //.with_max_level(tracing::Level::DEBUG)
        .with_target(true) // quita el nombre del crate si molesta
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .init();
    print_info();
    thread::spawn(|| {
        info!("desde un hilo sin nombre");
    });

    thread::Builder::new()
        .name("trabajador-1".into())
        .spawn(|| {
            info!("desde un hilo con nombre");
        })
        .unwrap()
        .join()
        .unwrap();

    info!("init server called");

    // --- Tabla t1: números ---
    let schema_t1: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
    ]));
    let t1: RecordBatch = RecordBatch::try_new(
        schema_t1,
        vec![
            Arc::new(Int32Array::from(vec![100, 200, 300])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )?;

    // --- Tabla t2: strings ---
    let schema_t2: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let t2: RecordBatch = RecordBatch::try_new(
        schema_t2,
        vec![
            Arc::new(Int32Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["alice", "bob"])),
        ],
    )?;

    // Registro de "tablas"
    let mut tables: HashMap<String, RecordBatch> = HashMap::new();
    tables.insert("t1".to_string(), t1);
    tables.insert("t2".to_string(), t2);

    let addr: SocketAddr = "127.0.0.1:5005".parse()?;
    info!("🚀 Flight server en {addr} (tablas: t1, t2)");

    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(InMem { tables }))
        .serve(addr)
        .await?;

    Ok(())
}

fn print_info() {
    info!(
        "cargo:rustc-env=BUILD_TIMESTAMP={}",
        chrono::Utc::now().to_rfc3339()
    );
    let git_commit = option_env!("GIT_COMMIT").unwrap_or("unknown");
    let git_branch = option_env!("GIT_BRANCH").unwrap_or("unknown");
    let rustc = option_env!("RUSTC_VERSION").unwrap_or("unknown");
    let ts = option_env!("BUILD_TIMESTAMP").unwrap_or("unknown");
    let profile = option_env!("BUILD_PROFILE").unwrap_or("unknown");
    let target = option_env!("TARGET").unwrap_or("unknown");
    info!("***********************");
    info!(
        "*{} v{}*",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );
    info!("***********************");
    debug!(
        "cargo:rustc-env=BUILD_TIMESTAMP={}",
        chrono::Utc::now().to_rfc3339()
    );
    // Git
    let rev = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok();
    if let Some(out) = rev {
        debug!(
            "cargo:rustc-env=GIT_COMMIT={}",
            String::from_utf8_lossy(&out.stdout).trim()
        );
    }
    let branch = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok();
    if let Some(out) = branch {
        debug!(
            "cargo:rustc-env=GIT_BRANCH={}",
            String::from_utf8_lossy(&out.stdout).trim()
        );
    }
    // rustc --version
    if let Ok(out) = Command::new("rustc").arg("--version").output() {
        debug!(
            "cargo:rustc-env=RUSTC_VERSION={}",
            String::from_utf8_lossy(&out.stdout).trim()
        );
    }
    // Features activas
    debug!(
        "cargo:rustc-env=BUILD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_default()
    );
    debug!(
        "cargo:rustc-env=TARGET={}",
        std::env::var("TARGET").unwrap_or_default()
    );
    debug!("build: {ts} | commit: {git_commit} ({git_branch})");
    debug!("toolchain: {rustc} | profile: {profile} | target: {target}");
}
