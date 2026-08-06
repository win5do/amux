use std::path::Path;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;

pub async fn request_stream(
    socket: &Path,
    mut request: Request<Full<Bytes>>,
) -> Result<Response<hyper::body::Incoming>> {
    request.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("close"),
    );
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect {}", socket.display()))?;
    let (mut sender, connection) = http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(sender.send_request(request).await?)
}

pub async fn request(socket: &Path, request: Request<Full<Bytes>>) -> Result<Response<Bytes>> {
    let response = request_stream(socket, request).await?;
    let (parts, body) = response.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok(Response::from_parts(parts, body))
}

pub async fn json_request<T: serde::de::DeserializeOwned>(
    socket: &Path,
    method: http::Method,
    path: &str,
    body: Option<Bytes>,
) -> Result<T> {
    let req = Request::builder()
        .method(method)
        .uri(format!("http://unix{path}"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(body.unwrap_or_default()))?;
    let response = request(socket, req).await?;
    if !response.status().is_success() {
        bail!(
            "HTTP {}: {}",
            response.status(),
            String::from_utf8_lossy(response.body())
        );
    }
    Ok(serde_json::from_slice(response.body())?)
}
