use std::io;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};

use crate::redis_protocol::{execute_redis_command, RespValue};

pub async fn serve_redis_wire(listener: TcpListener) -> io::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(error) = handle_connection(socket).await {
                tracing::debug!("Redis wire connection {} closed: {}", address, error);
            }
        });
    }
}

async fn handle_connection(socket: TcpStream) -> io::Result<()> {
    let mut stream = BufStream::new(socket);
    let mut selected_db = 0u32;

    loop {
        let Some(args) = read_command(&mut stream).await? else {
            stream.flush().await?;
            return Ok(());
        };

        match execute_redis_command(&mut selected_db, args).await {
            Ok(result) => {
                let response = encode_resp_value(&result.response);
                stream.write_all(&response).await?;
                stream.flush().await?;
                if result.close_connection {
                    return Ok(());
                }
            }
            Err(error) => {
                stream
                    .write_all(format!("-{}\r\n", error.resp_message()).as_bytes())
                    .await?;
                stream.flush().await?;
            }
        }
    }
}

async fn read_command(stream: &mut BufStream<TcpStream>) -> io::Result<Option<Vec<String>>> {
    let Some(line) = read_line(stream).await? else {
        return Ok(None);
    };
    if line.is_empty() {
        return Err(invalid_data("Redis command prefix was empty"));
    }

    match line.as_bytes()[0] {
        b'*' => parse_array_command(stream, &line).await.map(Some),
        _ => Ok(Some(
            line.split_whitespace()
                .map(|part| part.to_string())
                .collect::<Vec<_>>(),
        )),
    }
}

async fn parse_array_command(
    stream: &mut BufStream<TcpStream>,
    line: &str,
) -> io::Result<Vec<String>> {
    let item_count = parse_length(&line[1..], "Redis array length")?;
    let mut items = Vec::with_capacity(item_count);
    for _ in 0..item_count {
        let Some(header) = read_line(stream).await? else {
            return Err(invalid_data("Redis array item header ended early"));
        };
        if header.is_empty() {
            return Err(invalid_data("Redis array item header was empty"));
        }

        match header.as_bytes()[0] {
            b'$' => items.push(read_bulk_string(stream, &header).await?),
            b'+' => items.push(header[1..].to_string()),
            b':' => items.push(header[1..].to_string()),
            prefix => {
                return Err(invalid_data(format!(
                    "Unsupported Redis command argument prefix {}",
                    prefix as char
                )));
            }
        }
    }
    Ok(items)
}

async fn read_bulk_string(stream: &mut BufStream<TcpStream>, header: &str) -> io::Result<String> {
    let length = parse_length(&header[1..], "Redis bulk string length")?;
    let mut payload = vec![0; length + 2];
    stream.read_exact(&mut payload).await?;
    if payload[length..] != [b'\r', b'\n'] {
        return Err(invalid_data(
            "Redis bulk string was missing CRLF terminator",
        ));
    }
    String::from_utf8(payload[..length].to_vec())
        .map_err(|error| invalid_data(format!("Redis bulk string was not valid UTF-8: {}", error)))
}

async fn read_line(stream: &mut BufStream<TcpStream>) -> io::Result<Option<String>> {
    let mut line = Vec::new();
    let read = stream.read_until(b'\n', &mut line).await?;
    if read == 0 {
        return Ok(None);
    }
    if line.len() < 2 || line[line.len() - 2..] != [b'\r', b'\n'] {
        return Err(invalid_data("Redis line was not CRLF terminated"));
    }
    line.truncate(line.len() - 2);
    String::from_utf8(line)
        .map(Some)
        .map_err(|error| invalid_data(format!("Redis line was not valid UTF-8: {}", error)))
}

fn parse_length(raw: &str, field_name: &str) -> io::Result<usize> {
    let value = raw
        .parse::<i64>()
        .map_err(|error| invalid_data(format!("{} was invalid: {}", field_name, error)))?;
    if value < 0 {
        return Err(invalid_data(format!("{} was negative", field_name)));
    }
    Ok(value as usize)
}

fn encode_resp_value(value: &RespValue) -> Vec<u8> {
    match value {
        RespValue::SimpleString(text) => format!("+{}\r\n", text).into_bytes(),
        RespValue::BulkString(bytes) => {
            let mut result = format!("${}\r\n", bytes.len()).into_bytes();
            result.extend_from_slice(bytes);
            result.extend_from_slice(b"\r\n");
            result
        }
        RespValue::NullBulkString => b"$-1\r\n".to_vec(),
        RespValue::Integer(value) => format!(":{}\r\n", value).into_bytes(),
        RespValue::Array(items) => {
            let mut result = format!("*{}\r\n", items.len()).into_bytes();
            for item in items {
                result.extend_from_slice(&encode_resp_value(item));
            }
            result
        }
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_nested_arrays() {
        let encoded = encode_resp_value(&RespValue::Array(vec![
            RespValue::BulkString(b"hello".to_vec()),
            RespValue::Integer(2),
            RespValue::Array(vec![RespValue::NullBulkString]),
        ]));

        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            "*3\r\n$5\r\nhello\r\n:2\r\n*1\r\n$-1\r\n"
        );
    }
}
