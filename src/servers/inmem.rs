use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use arrow_array::{Int32Array, RecordBatch, StringArray};
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
    tables: HashMap<String, RecordBatch>,
}

#[tonic::async_trait]
impl FlightService for InMem {
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
        // Ticket esperado: "table=t1" | "table=t2"
        let raw = req.into_inner().ticket;
        let s = std::str::from_utf8(&raw).unwrap_or_default();
        let table_name = s.strip_prefix("table=").unwrap_or(s);

        let batch = self
            .tables
            .get(table_name)
            .ok_or_else(|| Status::not_found(format!("table not found: {table_name}")))?
            .clone();

        let opts = IpcWriteOptions::default();
        let schema_fd: FlightData = SchemaAsIpc::new(batch.schema().as_ref(), &opts).into();

        let gen = IpcDataGenerator::default();
        let mut dict_tracker =
            DictionaryTracker::new_with_preserve_dict_id(false, opts.preserve_dict_id());
        let (encoded_dicts, encoded_batch) = gen
            .encoded_batch(&batch, &mut dict_tracker, &opts)
            .map_err(|e| Status::internal(e.to_string()))?;
        let dict_fds: Vec<FlightData> = encoded_dicts.into_iter().map(Into::into).collect();
        let batch_fd: FlightData = encoded_batch.into();

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send(Ok(schema_fd))
            .await
            .map_err(|_| Status::internal("tx closed"))?;
        for d in dict_fds {
            tx.send(Ok(d))
                .await
                .map_err(|_| Status::internal("tx closed"))?;
        }
        tx.send(Ok(batch_fd))
            .await
            .map_err(|_| Status::internal("tx closed"))?;

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

pub async fn serve_inmem(addr: SocketAddr) -> Result<()> {
    // t1
    let schema_t1 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Int32, false),
    ]));
    let t1 = RecordBatch::try_new(
        schema_t1,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int32Array::from(vec![100, 200, 300, 400])),
        ],
    )?;

    // t2
    let schema_t2 = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let t2 = RecordBatch::try_new(
        schema_t2,
        vec![
            Arc::new(Int32Array::from(vec![2, 4, 5])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )?;

    let mut tables = HashMap::new();
    tables.insert("t1".into(), t1);
    tables.insert("t2".into(), t2);

    let svc = FlightServiceServer::new(InMem { tables });
    tracing::info!("🚀 InMem Flight server en {}", addr);
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await?;
    Ok(())
}
