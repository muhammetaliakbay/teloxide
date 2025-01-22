use std::future::Future;

use bytes::{Bytes, BytesMut};
use futures::{
    future::{ready, Either},
    stream::{once, unfold},
    FutureExt, Stream, StreamExt,
};
use tokio::io::{AsyncReadExt, AsyncWrite};

use crate::{errors::DownloadError, net::file_url};

use super::client::{self, Client, Request};

/// A trait for downloading files from Telegram.
pub trait Download {
    /// An error returned from [`download_file`](Self::download_file).
    type Err<'dst>
    where
        Self: 'dst;

    /// A future returned from [`download_file`](Self::download_file).
    type Fut<'dst>: Future<Output = Result<(), Self::Err<'dst>>> + Send
    where
        Self: 'dst;

    // NOTE: We currently only allow borrowing `dst` in the future,
    //       however we could also allow borrowing `self` or `path`.
    //       This doesn't seem useful for our current implementers of
    //       `Download`, but we could.

    /// Download a file from Telegram into `destination`.
    ///
    /// `path` can be obtained from [`GetFile`].
    ///
    /// To download as a stream of chunks, see [`download_file_stream`].
    ///
    /// ## Examples
    ///
    /// ```no_run
    /// use teloxide_core::{
    ///     net::Download,
    ///     requests::{Request, Requester},
    ///     types::File,
    ///     Bot,
    /// };
    /// use tokio::fs;
    ///
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let bot = Bot::new("TOKEN");
    ///
    /// let file = bot.get_file("*file_id*").await?;
    /// let mut dst = fs::File::create("/tmp/test.png").await?;
    /// bot.download_file(&file.path, &mut dst).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// [`GetFile`]: crate::payloads::GetFile
    /// [`download_file_stream`]: Self::download_file_stream
    fn download_file<'dst>(
        &'dst self,
        path: &str,
        destination: &'dst mut (dyn AsyncWrite + Unpin + Send),
    ) -> Self::Fut<'dst>;

    /// An error returned from
    /// [`download_file_stream`](Self::download_file_stream).
    type StreamErr;

    /// A stream returned from [`download_file_stream`].
    ///
    ///[`download_file_stream`]: (Self::download_file_stream)
    type Stream<'dst>: Stream<Item = Result<Bytes, Self::StreamErr>> + Send + 'dst
    where
        Self: 'dst;

    /// Download a file from Telegram as [`Stream`].
    ///
    /// `path` can be obtained from the [`GetFile`].
    ///
    /// To download into an [`AsyncWrite`] (e.g. [`tokio::fs::File`]), see
    /// [`download_file`].
    ///
    /// [`GetFile`]: crate::payloads::GetFile
    /// [`AsyncWrite`]: tokio::io::AsyncWrite
    /// [`tokio::fs::File`]: tokio::fs::File
    /// [`download_file`]: Self::download_file
    fn download_file_stream<'dst>(&'dst self, path: &str) -> Self::Stream<'dst>;
}

/// Download a file from Telegram into `dst`.
///
/// Note: if you don't need to use a different (from you're bot) client and
/// don't need to get *all* performance (and you don't, c'mon it's very io-bound
/// job), then it's recommended to use [`Download::download_file`].
pub fn download_file<'o, D>(
    client: &'o dyn Client,
    api_url: url::Url,
    token: &str,
    path: &str,
    dst: &'o mut D,
) -> impl Future<Output = Result<(), DownloadError>> + 'o
where
    D: ?Sized + AsyncWrite + Unpin,
{
    client.send(Request::get(file_url(api_url, token, path)).build().unwrap()).then(
        move |r| async move {
            tokio::io::copy(r?.body(), dst).await?;
            Ok(())
        },
    )
}

/// Download a file from Telegram as [`Stream`].
///
/// Note: if you don't need to use a different (from you're bot) client and
/// don't need to get *all* performance (and you don't, c'mon it's very io-bound
/// job), then it's recommended to use [`Download::download_file_stream`].
pub fn download_file_stream<'c>(
    client: &'c dyn Client,
    api_url: url::Url,
    token: &str,
    path: &str,
) -> impl Stream<Item = Result<Bytes, client::Error>> + 'c {
    let url = file_url(api_url, token, path);
    client.send(Request::get(url.clone()).build().unwrap()).into_stream().flat_map(move |res| {
        match res {
            Ok(res) => Either::Left(unfold((res, url.clone()), move |(mut res, url)| async {
                let mut buffer = BytesMut::with_capacity(1024);
                match res.body().read_buf(&mut buffer).await {
                    Err(err) => Some((
                        Err(client::Error { source: Box::new(err), url: Some(url.clone()) }),
                        (res, url),
                    )),
                    Ok(_) => Some((Ok(buffer.freeze()), (res, url))),
                }
            })),
            Err(err) => Either::Right(once(ready(Err(err)))),
        }
    })
}
