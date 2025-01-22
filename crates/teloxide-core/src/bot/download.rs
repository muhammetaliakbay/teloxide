use bytes::Bytes;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, StreamExt};
use tokio::io::AsyncWrite;

use crate::{
    bot::Bot,
    net::{self, client, Download},
    DownloadError,
};

impl Download for Bot {
    type Err<'dst> = DownloadError;

    // I would like to unbox this, but my coworkers will kill me if they'll see yet
    // another hand written `Future`. (waffle)
    type Fut<'dst> = BoxFuture<'dst, Result<(), Self::Err<'dst>>>;

    fn download_file<'dst>(
        &'dst self,
        path: &str,
        destination: &'dst mut (dyn AsyncWrite + Unpin + Send),
    ) -> Self::Fut<'dst> {
        net::download_file(
            self.client.as_ref(),
            url::Url::clone(&*self.api_url),
            &self.token,
            path,
            destination,
        )
        .boxed()
    }

    type StreamErr = client::Error;

    type Stream<'dst> = BoxStream<'dst, Result<Bytes, Self::StreamErr>>;

    fn download_file_stream<'dst>(&'dst self, path: &str) -> Self::Stream<'dst> {
        net::download_file_stream(
            self.client.as_ref(),
            url::Url::clone(&*self.api_url),
            &self.token,
            path,
        )
        .map(|res| res.map_err(crate::errors::hide_token))
        .boxed()
    }
}
