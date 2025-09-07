use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use arrow_array::{Int32Array, RecordBatch};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightResult, SchemaAsIpc,
    SchemaResult, Ticket,
};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

struct InMem {
    batch: RecordBatch,
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
            payload: Bytes::new(), // Bytes, no Vec<u8>
        }))))
    }

    // Métodos requeridos por el trait en Arrow 53.x
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

    async fn do_get(&self, _req: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let opts = IpcWriteOptions::default();

        // 1) Schema -> FlightData (nuevo API)
        let schema_fd: FlightData = SchemaAsIpc::new(self.batch.schema().as_ref(), &opts).into();

        let (tx, rx) = tokio::sync::mpsc::channel(8);

        // Enviar schema primero
        tx.send(Ok(schema_fd))
            .await
            .map_err(|_| Status::internal("tx closed"))?;

        // 2) Diccionarios + batch (nuevo API)
        let gen = IpcDataGenerator::default();
        // Si quieres preservar IDs de diccionario igual que antes:
        let mut dict_tracker = DictionaryTracker::new_with_preserve_dict_id(
            false,                   // no preservar reemplazos
            opts.preserve_dict_id(), // coherente con las opciones IPC
        );

        let (encoded_dicts, encoded_batch) = gen
            .encoded_batch(&self.batch, &mut dict_tracker, &opts)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Convertir a FlightData
        let dict_fds: Vec<FlightData> = encoded_dicts.into_iter().map(Into::into).collect();
        let batch_fd: FlightData = encoded_batch.into();

        // Enviar diccionarios
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
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
        Field::new("z", DataType::Int32, false),
        Field::new("w", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![3])),
            Arc::new(Int32Array::from(vec![4])),
        ],
    )?;

    let addr: SocketAddr = "127.0.0.1:5005".parse()?;
    println!("🚀 Flight server en {addr}");

    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(InMem { batch }))
        .serve(addr)
        .await?;

    Ok(())
}
