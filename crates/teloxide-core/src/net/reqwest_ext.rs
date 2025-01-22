use std::str::FromStr;

use bytes::Bytes;
use futures::{future::BoxFuture, Stream, TryStreamExt};
use reqwest::Body;
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

use super::client::{self, Client, Error, Request, Response};

impl Client for reqwest::Client {
    fn send(&self, req: Request) -> BoxFuture<client::Result> {
        Box::pin(async move {
            let mut builder = self.request(
                reqwest::Method::from_str(&req.method)
                    .map_err(|e| Error { source: Box::new(e), url: Some(req.url.clone()) })?,
                req.url.clone(),
            );
            for (name, value) in req.headers {
                builder = builder.header(name, value);
            }
            if let Some(body) = req.body {
                builder = builder.body(Body::wrap_stream(ReaderStream::new(body)));
            }
            let resp = builder
                .send()
                .await
                .map_err(|e| Error { source: Box::new(e), url: Some(req.url.clone()) })?;
            Ok(Box::new(ResponseWrapper {
                status: resp.status().as_u16(),
                body: Some(ResponseOrStreamReader::Response(resp)),
            }) as Box<dyn Response>)
        })
    }
}

enum ResponseOrStreamReader {
    Response(reqwest::Response),
    StreamReader(
        StreamReader<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, Bytes>,
    ),
}

struct ResponseWrapper {
    body: Option<ResponseOrStreamReader>,
    status: u16,
}

impl Response for ResponseWrapper {
    fn body(&mut self) -> &mut (dyn AsyncRead + Unpin + Send) {
        self.body = Some(ResponseOrStreamReader::StreamReader(match self.body.take().unwrap() {
            ResponseOrStreamReader::Response(resp) => {
                let stream = StreamReader::new(Box::new(
                    resp.bytes_stream()
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                )
                    as Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>);
                stream
            }
            ResponseOrStreamReader::StreamReader(reader) => reader,
        }));

        match self.body.as_mut().unwrap() {
            ResponseOrStreamReader::StreamReader(reader) => reader,
            _ => unreachable!(),
        }
    }

    fn status(&self) -> u16 {
        self.status
    }
}
