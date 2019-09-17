use std::collections::HashMap;
use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpListener;

use bytes::BytesMut;
use redis_protocol::prelude::*;

#[derive(Clone, Debug)]
enum RedisValue {
    String(String),
    Integer(i64),
    NULL,
}

#[derive(Debug)]
struct RedisKey(String);

impl From<RedisValue> for RedisKey {
    fn from(value: RedisValue) -> RedisKey {
        match value {
            RedisValue::String(key) => RedisKey(key),
            v => unimplemented!("Not implemend for: {:?}", v),
        }
    }
}

#[derive(Debug)]
enum RedisCmd {
    Get(RedisKey),
    Set(RedisKey, RedisValue),
    InvalidCommand(RedisValue),
}

impl RedisCmd {
    fn execute(self: Self, storage: &mut HashMap<String, String>) -> Result<Frame, ()> {
        let result = match &self {
            RedisCmd::Get(key) => {
                dbg!("Getting key: {}", key);
                let value = storage.get(&key.0).unwrap();
                Frame::BulkString(value.clone().into_bytes())
            }
            RedisCmd::Set(key, value) => {
                dbg!("Setting key: {} and value: {}", key, value);
                if let RedisValue::String(s) = value {
                    storage.insert(key.0.clone(), s.clone());
                }
                Frame::BulkString("OK".to_string().into_bytes())
            }
            c => {
                dbg!(c);
                return Err(());
            }
        };

        Ok(result)
    }
}

impl From<Vec<RedisValue>> for RedisCmd {
    fn from(values: Vec<RedisValue>) -> RedisCmd {
        let mut values = values.clone();

        if values.len() == 0 {
            return RedisCmd::InvalidCommand(RedisValue::String("Empty command".into()));
        };

        let cmd = values.remove(0);

        match cmd {
            RedisValue::String(ref v) if v == "GET" => RedisCmd::Get(values.remove(0).into()),
            RedisValue::String(ref v) if v == "SET" => {
                RedisCmd::Set(values.remove(0).into(), values.remove(0).into())
            }
            _ => RedisCmd::InvalidCommand(cmd),
        }
    }
}

fn frame_to_redis_values(frame: Frame) -> Result<Vec<RedisValue>, ()> {
    let result: Vec<_> = match frame {
        Frame::SimpleString(s) => vec![RedisValue::String(s)],
        Frame::BulkString(b) => vec![RedisValue::String(String::from_utf8(b).unwrap())],
        Frame::Integer(i) => vec![RedisValue::Integer(i)],
        Frame::Null => vec![RedisValue::NULL],
        Frame::Array(frames) => {
            let mut out = Vec::with_capacity(frames.len());
            for frame in frames {
                let mut res = frame_to_redis_values(frame)?;
                out.push(res.pop().unwrap());
            }
            out
        }

        x => unreachable!("Invalid frame: {:?}", x),
    };

    Ok(result)
}

fn parse_bytes(buffer: &BytesMut) -> Result<Vec<RedisValue>, ()> {
    let (frame, _consumed) = decode_bytes(buffer).unwrap();
    // dbg!(&frame);
    // dbg!(&consumed);

    frame_to_redis_values(frame.unwrap())
}

fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    let mut storage = HashMap::new();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buff = [0; 1024];
                let n = stream.read(&mut buff).unwrap();
                let buf: BytesMut = buff[0..n].into();
                let result = parse_bytes(&buf).unwrap();

                let cmd: RedisCmd = result.into();
                let result = cmd
                    .execute(&mut storage)
                    .expect("Can't execute the command");

                let mut wbuff: BytesMut = "".into();
                let _res = encode_bytes(&mut wbuff, &result);

                // dbg!(&result);
                stream.write(&wbuff)?;
            }
            Err(e) => {
                println!("Unable to connect: {}", e);
            }
        }
    }

    Ok(())
}
