use bytes::{buf::BufMut, Buf, BytesMut};
use combine::{
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
};
use std::str;
use tokio_util::codec::{Decoder, Encoder};

use crate::types::{RespValue, BulkString};

pub struct RespCodec {
    pub state: AnySendPartialState,
}

impl RespCodec {
    pub fn new() -> RespCodec {
        RespCodec {
            state: Default::default(),
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

fn encode_string(prefix: u8, value: String, buf: &mut BytesMut) {
    buf.reserve(value.len() + 3);
    buf.put_u8(prefix);
    buf.put(&value.into_bytes()[..]);
    buf.put(&b"\r\n"[..]);
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
            RespValue::SimpleString(value) => encode_string(b'+', value, buf),
            // TODO: support description
            RespValue::Error(value, _description) => encode_string(b'-', value, buf),
            RespValue::Integer(value) => encode_string(b':', value.to_string(), buf),
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
            // t => {
            //     return Err(format!("Unsuported Type: {:?}", t).into());
            // }
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
