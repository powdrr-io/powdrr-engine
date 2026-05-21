use std::io;
use std::sync::atomic::{AtomicI32, Ordering};

use bson::Document;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::mongodb_protocol::execute_mongodb_command_value;

const OP_MSG: i32 = 2013;
const OP_MSG_CHECKSUM_PRESENT: u32 = 1;
static MONGO_WIRE_RESPONSE_IDS: AtomicI32 = AtomicI32::new(1);

struct MessageHeader {
    message_length: usize,
    request_id: i32,
    op_code: i32,
}

pub async fn serve_mongodb_wire(listener: TcpListener) -> io::Result<()> {
    loop {
        let (socket, address) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(error) = handle_connection(socket).await {
                tracing::debug!("Mongo wire connection {} closed: {}", address, error);
            }
        });
    }
}

async fn handle_connection(mut socket: TcpStream) -> io::Result<()> {
    loop {
        let Some(header) = read_message_header(&mut socket).await? else {
            return Ok(());
        };
        if header.message_length < 16 {
            return Err(invalid_data(
                "Mongo wire message was shorter than the header",
            ));
        }

        let mut body = vec![0; header.message_length - 16];
        socket.read_exact(&mut body).await?;

        if header.op_code != OP_MSG {
            return Err(invalid_data(format!(
                "Unsupported Mongo wire opcode {}",
                header.op_code
            )));
        }

        let command = parse_op_msg_command(&body)?;
        let database = command.get_str("$db").unwrap_or("admin");
        let payload = serde_json::to_value(&command).map_err(|error| {
            invalid_data(format!("Failed to convert BSON command to JSON: {}", error))
        })?;
        let response_value = execute_mongodb_command_value(database, payload).await;
        let response_document = bson::to_document(&response_value).map_err(|error| {
            invalid_data(format!(
                "Failed to convert Mongo command response to BSON: {}",
                error
            ))
        })?;
        let response = encode_op_msg_response(header.request_id, &response_document)?;
        socket.write_all(&response).await?;
    }
}

async fn read_message_header(socket: &mut TcpStream) -> io::Result<Option<MessageHeader>> {
    let mut header_bytes = [0; 16];
    let mut bytes_read = 0usize;
    while bytes_read < header_bytes.len() {
        let read_now = socket.read(&mut header_bytes[bytes_read..]).await?;
        if read_now == 0 {
            if bytes_read == 0 {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Mongo wire header ended early",
            ));
        }
        bytes_read += read_now;
    }

    Ok(Some(MessageHeader {
        message_length: read_length(&header_bytes[0..4], "Mongo wire message length")?,
        request_id: read_i32(&header_bytes[4..8])?,
        op_code: read_i32(&header_bytes[12..16])?,
    }))
}

fn parse_op_msg_command(body: &[u8]) -> io::Result<Document> {
    if body.len() < 5 {
        return Err(invalid_data("Mongo OP_MSG body was too short"));
    }

    let flag_bits = read_u32(&body[0..4])?;
    let mut sections = &body[4..];
    if flag_bits & OP_MSG_CHECKSUM_PRESENT != 0 {
        if sections.len() < 4 {
            return Err(invalid_data(
                "Mongo OP_MSG declared a checksum without enough bytes",
            ));
        }
        sections = &sections[..sections.len() - 4];
    }

    let mut offset = 0usize;
    let mut command = None;
    while offset < sections.len() {
        match sections[offset] {
            0 => {
                offset += 1;
                let document = parse_document(sections, &mut offset)?;
                if command.replace(document).is_some() {
                    return Err(invalid_data(
                        "Mongo OP_MSG contained more than one type-0 section",
                    ));
                }
            }
            1 => {
                offset += 1;
                if offset + 4 > sections.len() {
                    return Err(invalid_data("Mongo OP_MSG section-1 header was truncated"));
                }
                let sequence_size =
                    read_length(&sections[offset..offset + 4], "Mongo OP_MSG section-1 size")?;
                if sequence_size < 4 || offset + sequence_size > sections.len() {
                    return Err(invalid_data("Mongo OP_MSG section-1 size was invalid"));
                }
                offset += sequence_size;
            }
            payload_type => {
                return Err(invalid_data(format!(
                    "Mongo OP_MSG payload type {} is unsupported",
                    payload_type
                )));
            }
        }
    }

    command.ok_or_else(|| invalid_data("Mongo OP_MSG did not include a type-0 command body"))
}

fn parse_document(sections: &[u8], offset: &mut usize) -> io::Result<Document> {
    if *offset + 4 > sections.len() {
        return Err(invalid_data(
            "Mongo BSON document length prefix was truncated",
        ));
    }
    let document_length = read_length(
        &sections[*offset..*offset + 4],
        "Mongo BSON document length",
    )?;
    if document_length < 5 || *offset + document_length > sections.len() {
        return Err(invalid_data("Mongo BSON document length was invalid"));
    }
    let document = bson::from_slice::<Document>(&sections[*offset..*offset + document_length])
        .map_err(|error| {
            invalid_data(format!("Failed to decode Mongo BSON document: {}", error))
        })?;
    *offset += document_length;
    Ok(document)
}

fn encode_op_msg_response(response_to: i32, document: &Document) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes());
    body.push(0);
    body.extend_from_slice(&bson::to_vec(document).map_err(|error| {
        invalid_data(format!("Failed to encode Mongo BSON response: {}", error))
    })?);

    let mut message = Vec::new();
    let message_length = 16 + body.len();
    message.extend_from_slice(&(message_length as i32).to_le_bytes());
    message.extend_from_slice(&next_response_id().to_le_bytes());
    message.extend_from_slice(&response_to.to_le_bytes());
    message.extend_from_slice(&OP_MSG.to_le_bytes());
    message.extend_from_slice(&body);
    Ok(message)
}

fn next_response_id() -> i32 {
    MONGO_WIRE_RESPONSE_IDS.fetch_add(1, Ordering::Relaxed)
}

fn read_i32(bytes: &[u8]) -> io::Result<i32> {
    let array =
        <[u8; 4]>::try_from(bytes).map_err(|_| invalid_data("Mongo wire integer was truncated"))?;
    Ok(i32::from_le_bytes(array))
}

fn read_u32(bytes: &[u8]) -> io::Result<u32> {
    let array =
        <[u8; 4]>::try_from(bytes).map_err(|_| invalid_data("Mongo wire integer was truncated"))?;
    Ok(u32::from_le_bytes(array))
}

fn read_length(bytes: &[u8], field_name: &str) -> io::Result<usize> {
    let value = read_i32(bytes)?;
    if value < 0 {
        return Err(invalid_data(format!("{} was negative", field_name)));
    }
    Ok(value as usize)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use bson::doc;

    use super::*;

    #[test]
    fn parse_op_msg_command_reads_type_zero_section() {
        let body = encode_test_body(
            0,
            &[type_zero_section(&doc! {
                "hello": 1,
                "$db": "admin",
            })],
        );

        let command = parse_op_msg_command(&body).unwrap();

        assert_eq!(command.get_i32("hello").unwrap(), 1);
        assert_eq!(command.get_str("$db").unwrap(), "admin");
    }

    #[test]
    fn parse_op_msg_command_skips_document_sequence_sections() {
        let body = encode_test_body(
            0,
            &[
                type_one_section(
                    "documents",
                    &[doc! { "ignored": true }, doc! { "ignored": false }],
                ),
                type_zero_section(&doc! {
                    "find": "logs",
                    "$db": "analytics",
                }),
            ],
        );

        let command = parse_op_msg_command(&body).unwrap();

        assert_eq!(command.get_str("find").unwrap(), "logs");
        assert_eq!(command.get_str("$db").unwrap(), "analytics");
    }

    #[test]
    fn parse_op_msg_command_strips_checksum_bytes() {
        let mut body = encode_test_body(
            OP_MSG_CHECKSUM_PRESENT,
            &[type_zero_section(&doc! {
                "ping": 1,
                "$db": "admin",
            })],
        );
        body.extend_from_slice(&0xAABBCCDDu32.to_le_bytes());

        let command = parse_op_msg_command(&body).unwrap();

        assert_eq!(command.get_i32("ping").unwrap(), 1);
        assert_eq!(command.get_str("$db").unwrap(), "admin");
    }

    #[test]
    fn parse_op_msg_command_rejects_multiple_type_zero_sections() {
        let body = encode_test_body(
            0,
            &[
                type_zero_section(&doc! { "ping": 1 }),
                type_zero_section(&doc! { "hello": 1 }),
            ],
        );

        let error = parse_op_msg_command(&body).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("more than one type-0 section"),
            "unexpected error: {}",
            error
        );
    }

    #[test]
    fn encode_op_msg_response_writes_valid_wire_message() {
        let response = encode_op_msg_response(
            42,
            &doc! {
                "ok": 1.0,
                "helloOk": true,
            },
        )
        .unwrap();

        assert_eq!(read_i32(&response[0..4]).unwrap(), response.len() as i32);
        assert_eq!(read_i32(&response[8..12]).unwrap(), 42);
        assert_eq!(read_i32(&response[12..16]).unwrap(), OP_MSG);
        assert_eq!(read_u32(&response[16..20]).unwrap(), 0);
        assert_eq!(response[20], 0);

        let document = bson::from_slice::<Document>(&response[21..]).unwrap();
        assert_eq!(document.get_f64("ok").unwrap(), 1.0);
        assert!(document.get_bool("helloOk").unwrap());
    }

    fn encode_test_body(flag_bits: u32, sections: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&flag_bits.to_le_bytes());
        for section in sections {
            body.extend_from_slice(section);
        }
        body
    }

    fn type_zero_section(document: &Document) -> Vec<u8> {
        let mut section = vec![0];
        section.extend_from_slice(&bson::to_vec(document).unwrap());
        section
    }

    fn type_one_section(identifier: &str, documents: &[Document]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(identifier.as_bytes());
        payload.push(0);
        for document in documents {
            payload.extend_from_slice(&bson::to_vec(document).unwrap());
        }

        let mut section = vec![1];
        section.extend_from_slice(&((payload.len() + 4) as i32).to_le_bytes());
        section.extend_from_slice(&payload);
        section
    }
}
