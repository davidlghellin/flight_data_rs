use std::{fs::File, net::SocketAddr, sync::Arc};

use anyhow::Result;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::Schema;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

struct MyParquet;

#[tonic::async_trait]
impl FlightService for MyParquet {
    type HandshakeStream = tokio_stream::Once<Result<HandshakeResponse, Status>>;
    type ListFlightsStream = tokio_stream::Empty<Result<FlightInfo, Status>>;
    type DoGetStream = ReceiverStream<Result<FlightData, Status>>;
    type DoPutStream = tokio_stream::Empty<Result<PutResult, Status>>;
    type DoActionStream = tokio_stream::Empty<Result<FlightResult, Status>>;
    type ListActionsStream = tokio_stream::Empty<Result<ActionType, Status>>;
    type DoExchangeStream = tokio_stream::Empty<Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(tokio_stream::once(Ok(HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        }))))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }

    async fn do_get(&self, req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        // Ticket: parquet=/ruta/archivo.parquet&batch=65536
        let raw = req.into_inner().ticket;
        let s = std::str::from_utf8(&raw).unwrap_or_default();

        let mut parquet_path: Option<&str> = None;
        let mut batch_size: usize = 65536;
        for kv in s.split('&') {
            if let Some(rest) = kv.strip_prefix("parquet=") {
                parquet_path = Some(rest);
            } else if let Some(n) = kv.strip_prefix("batch=") {
                if let Ok(v) = n.parse() {
                    batch_size = v;
                }
            }
        }
        let parquet_path: &str =
            parquet_path.ok_or_else(|| Status::invalid_argument("missing parquet=..."))?;

        let file: File =
            File::open(parquet_path).map_err(|e| Status::internal(format!("open: {e}")))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Status::internal(format!("builder: {e}")))?;
        builder = builder.with_batch_size(batch_size);

        let schema: Arc<Schema> = builder.schema().clone();
        let mut rb_reader = builder
            .build()
            .map_err(|e| Status::internal(format!("build: {e}")))?;

        let opts: IpcWriteOptions = IpcWriteOptions::default();
        let schema_fd: FlightData = SchemaAsIpc::new(schema.as_ref(), &opts).into();

        let gen: IpcDataGenerator = IpcDataGenerator::default();
        let mut dict_tracker =
            DictionaryTracker::new_with_preserve_dict_id(false, opts.preserve_dict_id());

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send(Ok(schema_fd))
            .await
            .map_err(|_| Status::internal("tx closed"))?;

        tokio::task::spawn(async move {
            for batch_res in &mut rb_reader {
                let batch = match batch_res {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("read batch: {e}"))))
                            .await;
                        return;
                    }
                };
                match gen.encoded_batch(&batch, &mut dict_tracker, &opts) {
                    Ok((encoded_dicts, encoded_batch)) => {
                        let dict_fds: Vec<FlightData> =
                            encoded_dicts.into_iter().map(Into::into).collect();
                        let batch_fd: FlightData = encoded_batch.into();
                        for d in dict_fds {
                            if tx.send(Ok(d)).await.is_err() {
                                return;
                            }
                        }
                        if tx.send(Ok(batch_fd)).await.is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("encode: {e}")))).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn do_put(
        &self,
        _: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }
    async fn do_exchange(
        &self,
        _: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Ok(Response::new(tokio_stream::empty()))
    }
}

pub async fn serve_parquet(addr: SocketAddr) -> Result<()> {
    tracing::info!("🚀 Parquet Flight server en {}", addr);
    let svc: FlightServiceServer<MyParquet> = FlightServiceServer::new(MyParquet);
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;
    Ok(())
}
