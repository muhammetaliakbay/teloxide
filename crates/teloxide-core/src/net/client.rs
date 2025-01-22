use futures::future::BoxFuture;
use std::fmt::{Debug, Display};
use tokio::io::AsyncRead;

pub trait Client: Debug + Send + Sync {
    fn send(&self, req: Request) -> BoxFuture<Result>;
}

pub type Result = std::result::Result<Box<dyn Response>, Error>;

#[derive(Debug)]
pub struct Error {
    pub source: Box<dyn std::error::Error + Send + Sync>,
    pub url: Option<url::Url>,
}

impl Error {
    pub fn without_url(self) -> Self {
        Self { source: self.source, url: None }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {}", self.source)
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

pub struct Request {
    pub method: String,
    pub url: url::Url,
    pub headers: Vec<(String, String)>,
    pub body: Option<Box<dyn AsyncRead + Unpin + Send>>,
}

impl Request {
    pub fn get(url: url::Url) -> RequestBuilder {
        RequestBuilder::new().get().url(url)
    }

    pub fn post(url: url::Url) -> RequestBuilder {
        RequestBuilder::new().post().url(url)
    }
}

pub struct RequestBuilder {
    method: Option<String>,
    url: Option<url::Url>,
    headers: Vec<(String, String)>,
    body: Option<Box<dyn AsyncRead + Unpin + Send>>,
}

impl RequestBuilder {
    pub fn new() -> Self {
        Self { method: None, url: None, headers: Vec::new(), body: None }
    }

    pub fn method(mut self, method: String) -> Self {
        self.method = Some(method);
        self
    }

    pub fn get(self) -> Self {
        self.method("GET".to_owned())
    }

    pub fn post(self) -> Self {
        self.method("POST".to_owned())
    }

    pub fn url(mut self, url: url::Url) -> Self {
        self.url = Some(url);
        self
    }

    pub fn body(mut self, body: Box<dyn AsyncRead + Unpin + Send>) -> Self {
        self.body = Some(body);
        self
    }

    pub fn header(mut self, key: String, value: String) -> Self {
        self.headers.push((key, value));
        self
    }

    pub fn build(self) -> std::result::Result<Request, &'static str> {
        Ok(Request {
            method: self.method.ok_or("method is required")?,
            url: self.url.ok_or("url is required")?,
            headers: self.headers,
            body: self.body,
        })
    }
}

pub trait Response: Send {
    fn status(&self) -> u16;
    fn body(&mut self) -> &mut (dyn AsyncRead + Unpin + Send);
}
