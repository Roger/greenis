use std::collections::{HashMap, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::str;
use std::sync::{Arc, Mutex};

use futures::stream::StreamExt;
use tokio::net::TcpListener;

use {
    bytes::{buf::BufMut, Buf, BytesMut},
    combine::{
        count_min_max,
        error::{ParseError, StreamError},
        parser::{
            byte::{byte, take_until_bytes},
            choice::choice,
            combinator::{any_send_partial_state, AnySendPartialState},
            range::{range, recognize, take},
        },
        stream::{easy, PartialStream, RangeStream, StreamErrorFor},
        value, Parser,
    },
    futures::prelude::*,
    tokio,
    tokio_util::codec::{Decoder, Encoder, Framed},
};

#[macro_use]
extern crate log;

pub struct RespCodec {
    state: AnySendPartialState,
}

impl RespCodec {
    pub fn new() -> RespCodec {
        RespCodec {
            state: Default::default(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BulkString(Vec<u8>);

impl fmt::Display for BulkString {
    /// Try to display a friendly string, not a vec of u8
    /// most of the time BulkStrings are a string, but can be used to store binary data
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match String::from_utf8(self.0.to_vec()) {
            Ok(value) => write!(f, "{}", value),
            Err(_) => write!(f, "{:?}", self.0),
        }
    }
}

impl fmt::Debug for BulkString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String, Option<String>),
    Integer(i64),
    BulkString(BulkString),
    Array(VecDeque<RespValue>),
    Null,
}

impl RespValue {
    fn to_string(&self) -> Option<String> {
        use RespValue::*;
        match self {
            SimpleString(ref value) => Some(value.clone()),
            BulkString(ref value) => String::from_utf8(value.0.to_vec()).ok(),
            _ => None,
        }
    }

    // fn as_str(&self) -> Option<&str> {
    //     use RespValue::*;
    //     match *self {
    //         SimpleString(ref value) => Some(value),
    //         BulkString(ref value) => str::from_utf8(&value.0).ok(),
    //         _ => None,
    //     }
    // }
}

type RedisKey = BulkString;
type RedisValue = BulkString;

#[derive(Debug)]
enum RedisCmd {
    Ping(Option<RedisValue>),
    Get(RedisKey),
    Set(RedisKey, RedisValue),
    Keys(RedisValue),
    Command,
}

impl RedisCmd {
    /// Excecute the command and return the RespValue to reply to the client
    fn execute(
        self: Self,
        storage: Arc<Mutex<HashMap<RedisKey, RedisValue>>>,
    ) -> Result<RespValue, ()> {
        let result = match &self {
            RedisCmd::Ping(None) => RespValue::SimpleString("PONG".into()),
            RedisCmd::Ping(Some(value)) => RespValue::BulkString(value.clone()),
            RedisCmd::Get(key) => {
                // println!("Getting key: {}", key);
                let storage = storage.lock().unwrap();
                if let Some(value) = storage.get(key) {
                    RespValue::BulkString(value.clone())
                } else {
                    RespValue::Null
                }
            }
            RedisCmd::Set(key, value) => {
                // println!("Setting key: {} and value: {}", key, value);
                storage.lock().unwrap().insert(key.clone(), value.clone());
                RespValue::SimpleString("OK".into())
            }
            RedisCmd::Keys(pattern) => {
                debug!("keys: {}", pattern);
                let storage = storage.lock().unwrap();
                RespValue::Array(
                    storage
                        .keys()
                        .map(|k| RespValue::BulkString(k.clone()))
                        .collect(),
                )
            }
            // Invalid command
            _ => {
                return Err(());
            }
        };

        Ok(result)
    }
}

/// Get the next argument from a RespValue::Array
fn get_next_value(resp: &mut VecDeque<RespValue>) -> Result<BulkString, &'static str> {
    match resp.pop_front().ok_or("Not enough arguments") {
        Ok(value) => match value {
            RespValue::BulkString(value) => Ok(value),
            _ => Err("Invalid argument, must be BulkString"),
        },
        Err(err) => Err(err),
    }
}

impl TryFrom<RespValue> for RedisCmd {
    type Error = &'static str;

    /// Convert RespValues into RedisCmd
    fn try_from(resp: RespValue) -> Result<Self, Self::Error> {
        match resp {
            RespValue::Array(mut resp) => {
                let cmd = match resp.pop_front() {
                    Some(cmd) => cmd,
                    None => return Err("No command specified"),
                };

                match cmd.to_string().unwrap_or_default().to_uppercase().as_ref() {
                    "GET" => Ok(RedisCmd::Get(get_next_value(&mut resp)?)),
                    "SET" => Ok(RedisCmd::Set(
                        get_next_value(&mut resp)?,
                        get_next_value(&mut resp)?,
                    )),
                    "PING" => Ok(RedisCmd::Ping(get_next_value(&mut resp).ok())),
                    "KEYS" => Ok(RedisCmd::Keys(get_next_value(&mut resp)?)),
                    "COMMAND" => Ok(RedisCmd::Command),
                    "" => Err("No command specified"),
                    _ => Err("Invalid command"),
                }
            }
            _ => Err("Invalid command"),
        }
    }
}

/// Line parser for resp protocol, reads until `\r\n`
fn line<'a, Input>() -> impl Parser<Input, Output = &'a str, PartialState = AnySendPartialState> + 'a
where
    Input: RangeStream<Token = u8, Range = &'a [u8]> + 'a,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    any_send_partial_state(
        recognize(take_until_bytes(&b"\r\n"[..]).with(take(2).map(|_| ()))).and_then(
            |line: &[u8]| {
                str::from_utf8(&line[..line.len() - 2]).map_err(StreamErrorFor::<Input>::other)
            },
        ),
    )
}

/// Integer parser (i64) for resp protocol
/// ie. :42\r\n
fn integer<'a, Input>() -> impl Parser<Input, Output = i64, PartialState = AnySendPartialState> + 'a
where
    Input: RangeStream<Token = u8, Range = &'a [u8]> + 'a,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    any_send_partial_state(line().and_then(|line| match line.trim().parse() {
        Ok(value) => Ok(value),
        Err(_) => Err(StreamErrorFor::<Input>::message_static_message(
            "Invalid Integer",
        )),
    }))
}

/// Resp2 parser for server commands
/// clients send only command as SimpleString (simple commands easy to send from telnet/netcat) or
/// using Array of BulkStrings with the first element as the command
/// That's why we only parse a subset of the resp2 protocol here, we only need to encode the rest
/// of the spec to create anwsers to the clients
fn resp_parser<'a, Input>(
) -> impl Parser<Input, Output = RespValue, PartialState = AnySendPartialState> + 'a
where
    Input: RangeStream<Token = u8, Range = &'a [u8]> + 'a,
    Input::Error: ParseError<Input::Token, Input::Range, Input::Position>,
{
    // Simple command parser, this is just a string with args splited by whitespace
    // ie. GET key
    let simple_command = || {
        line().map(|line| {
            let values = line
                .split_whitespace()
                .map(|part| RespValue::BulkString(BulkString(part.into())))
                .collect();
            RespValue::Array(values)
        })
    };

    // Binary friendly string
    let bulk = || {
        integer().then_partial(move |&mut length| {
            if length < 0 {
                value(RespValue::Null).left()
            } else {
                take(length as usize)
                    .map(|data: &[u8]| RespValue::BulkString(BulkString(data.into())))
                    .skip(range(&b"\r\n"[..]))
                    .right()
            }
        })
    };

    // Array of bulk strings
    let array = || {
        integer().then_partial(move |&mut length| {
            if length < 0 {
                value(RespValue::Null).left()
            } else {
                let length = length as usize;
                count_min_max(length, length, byte(b'$').with(bulk()))
                    .map(|mut results: Vec<_>| {
                        // We should never hit an Err result here, because the parsing should fail
                        // before, count_min_max should get less values if a resp value fails and
                        // raising an all errors that happened
                        let results = results.drain(..).map(|x| x).collect();
                        RespValue::Array(results)
                    })
                    .right()
            }
        })
    };

    any_send_partial_state(choice((byte(b'*').with(array()), simple_command())))
}

impl Encoder for RespCodec {
    type Item = RespValue;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    /// Encode a RespValue and push it to the buffer
    fn encode(&mut self, resp: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        match resp {
            RespValue::Null => {
                buf.reserve(5);
                buf.put(&b"$-1\r\n"[..]);
            }
            RespValue::SimpleString(value) => {
                buf.reserve(value.len() + 3);
                buf.put_u8(b'+');
                buf.put(&value.into_bytes()[..]);
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Error(value, _description) => {
                buf.reserve(value.len() + 3);
                buf.put_u8(b'-');
                buf.put(&value.into_bytes()[..]);
                buf.put(&b"\r\n"[..]);
            }
            RespValue::BulkString(BulkString(value)) => {
                let len_str = value.len().to_string();
                buf.reserve(value.len() + len_str.len() + 5);
                buf.put_u8(b'$');
                buf.put(&len_str.into_bytes()[..]);
                buf.put(&b"\r\n"[..]);
                buf.put(&value[..]);
                buf.put(&b"\r\n"[..]);
            }
            RespValue::Array(mut values) => {
                let len_str = values.len().to_string();
                buf.reserve(values.len() * 2 + len_str.len());
                buf.put_u8(b'*');
                buf.put(&len_str.into_bytes()[..]);
                buf.put(&b"\r\n"[..]);
                values.drain(..).for_each(|value| {
                    self.encode(value, buf).unwrap();
                });
            }
            t => {
                return Err(format!("Unsuported Type: {:?}", t).into());
            }
        }
        Ok(())
    }
}

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        debug!("Decoding `{:?}`", str::from_utf8(src).unwrap_or("NOT UTF8"));

        let (opt, removed_len) = combine::stream::decode(
            resp_parser(),
            &mut easy::Stream(PartialStream(&src[..])),
            &mut self.state,
        )
        .map_err(|err| {
            let err = err
                .map_range(|r| {
                    str::from_utf8(r)
                        .ok()
                        .map_or_else(|| format!("{:?}", r), |s| s.to_string())
                })
                .map_position(|p| p.translate_position(&src[..]));
            format!("{}\nIn input: `{}`", err, str::from_utf8(src).unwrap())
        })?;

        debug!(
            "Accepted {} bytes: `{:?}`",
            removed_len,
            str::from_utf8(&src[..removed_len]).unwrap_or("NOT UTF8")
        );

        // Remove the input we just consumed.
        // Ideally this would be done automatically by the call to
        // `stream::decode` but it does unfortunately not work due
        // to lifetime issues (Non lexical lifetimes might fix it!)
        src.advance(removed_len);

        match opt {
            // `None` means we did not have enough input and we require that the
            // caller of `decode` supply more before calling us again
            None => {
                debug!("Requesting more input!");
                Ok(None)
            }

            // `Some` means that a message was successfully decoded
            // (and that we are ready to start decoding the next message)
            Some(output) => {
                debug!("Decoded `{:?}`", output);
                Ok(Some(output))
            }
        }
    }
}

async fn decode(
    io: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin,
    storage: Arc<Mutex<HashMap<RedisKey, RedisValue>>>,
) {
    let decoder = RespCodec::new();
    let mut framed = Framed::new(io, decoder);
    loop {
        let result = framed.try_next().await;
        match result {
            Ok(resp) => {
                debug!("Decoded: {:?}", &resp);
                match resp {
                    None => break (),
                    Some(resp) => match RedisCmd::try_from(resp) {
                        Ok(cmd) => {
                            let storage = storage.clone();
                            match cmd.execute(storage) {
                                Ok(frame) => framed.send(frame).await.unwrap(),
                                Err(err) => {
                                    let frame = RespValue::Error("NOT_IMPLEMENTED".into(), None);
                                    framed.send(frame).await.unwrap();
                                    error!("Error executing frame: {:?}", err)
                                }
                            };
                        }
                        Err(err) => {
                            let frame = RespValue::Error(err.into(), None);
                            framed.send(frame).await.unwrap();
                            error!("Error getting command: {:?}", err);
                        }
                    },
                };
            }
            Err(err) => {
                debug!("Error creating codec: {:?}", err);
                break;
            }
        };
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let addr = "127.0.0.1:6142";
    let mut listener = TcpListener::bind(addr).await.unwrap();
    let storage = Arc::new(Mutex::new(HashMap::new()));
    let server = async move {
        let mut incoming = listener.incoming();
        while let Some(conn) = incoming.next().await {
            match conn {
                Err(err) => eprintln!("Error accepting: {:?}", err),
                Ok(sock) => {
                    debug!("Connection: {:?}", sock.peer_addr());
                    let storage = storage.clone();
                    tokio::spawn(async move {
                        // let (reader, writer) = sock.split();
                        decode(sock, storage).await;
                    });
                }
            };
        }
    };

    server.await
}
