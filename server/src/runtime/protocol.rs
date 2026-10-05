use std::error::Error;
use std::str;
use tokio::io::{AsyncReadExt, AsyncRead};

pub const BIG_BUFSIZE: usize = 1024;
pub const SML_BUFSIZE: usize = 256;

pub fn resp_frame_end(buf: &[u8], start: usize) -> Option<usize> {
    let kind = *buf.get(start)?;
    let header_end = buf[start + 1..].windows(2).position(|w| w == b"\r\n")
        .map(|i| start + i + 3)?;
    match kind {
        b'+' | b'-' | b':' => Some(header_end),
        b'$' => {
            let len = str::from_utf8(&buf[start + 1..header_end - 2]).ok()?.parse::<isize>().ok()?;
            if len < 0 { return Some(header_end); }
            let end = header_end.checked_add(len as usize + 2)?;
            (buf.len() >= end).then_some(end)
        },
        b'*' => {
            let count = str::from_utf8(&buf[start + 1..header_end - 2]).ok()?.parse::<isize>().ok()?;
            if count < 0 { return Some(header_end); }
            let mut end = header_end;
            for _ in 0..count as usize {
                end = resp_frame_end(buf, end)?;
            }
            Some(end)
        },
        _ => None,
    }
}

pub async fn read_resp_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0; BIG_BUFSIZE];
        let n = stream.read(&mut chunk).await?;
        if n == 0 { return Err("Backend closed the connection before replying".into()); }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(end) = resp_frame_end(&buf, 0) {
            buf.truncate(end);
            return Ok(buf);
        }
    }
}

pub async fn read_bulk_response<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
    let mut kind = [0; 1];
    stream.read_exact(&mut kind).await?;
    if kind[0] != b'$' { return Err("Expected a bulk response from replication stream".into()); }
    let mut line = Vec::new();
    loop {
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).await?;
        line.push(byte[0]);
        if line.ends_with(b"\r\n") { break; }
    }
    let len = str::from_utf8(&line[..line.len() - 2])?.parse::<isize>()?;
    if len < 0 { return Ok(Vec::new()); }
    let mut payload = vec![0; len as usize + 2];
    stream.read_exact(&mut payload).await?;
    if !payload.ends_with(b"\r\n") { return Err("Invalid replication stream payload".into()); }
    payload.truncate(payload.len() - 2);
    Ok(payload)
}


