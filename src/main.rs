use std::convert::Infallible;
use std::net::SocketAddr;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;

#[derive(Debug, Deserialize, Serialize)]
struct Log {
    level: String,
    message: String,
    #[serde(alias = "resourceId")]
    resource_id: String,
    timestamp: String,
    #[serde(alias = "traceId")]
    trace_id: String,
    #[serde(alias = "spanId")]
    span_id: String,
    commit: String,
    metadata: Metadata,
}

#[derive(Debug, Deserialize, Serialize)]
struct Metadata {
    #[serde(alias = "parentResourceId")]
    parent_resource_id: String,
}

async fn ingest(
    mut req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if let Some(f) = req.frame().await.and_then(|f| f.ok()) {
        if let Some(data) = f.into_data().ok() {
            let v: Log = serde_json::from_slice(&data).unwrap();
            println!("{:?}", v);
        }
    }

    Ok(Response::new(Full::new(Bytes::from(
        "log data received successfully",
    ))))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("listening on {}", addr);

    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(ingest))
                .await
            {
                println!("error serving connection: {:?}", err);
            }
        });
    }
}
