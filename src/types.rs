use std::collections::{HashMap, VecDeque};
use std::convert::TryFrom;
use std::fmt;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BulkString(pub Vec<u8>);

impl BulkString {
    pub fn append(&mut self, other: &mut BulkString) {
        self.0.append(&mut other.0);
    }
}

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

pub type RedisKey = BulkString;
pub type RedisValue = BulkString;

#[derive(Debug)]
pub enum RedisCmd {
    Ping(Option<RedisValue>),
    Get(RedisKey),
    Set(RedisKey, RedisValue),
    Append(RedisKey, RedisValue),
    Keys(RedisValue),
    Exists(RedisKey),
    Command,
}

impl RedisCmd {
    /// Excecute the command and return the RespValue to reply to the client
    pub fn execute(
        mut self,
        storage: Arc<Mutex<HashMap<RedisKey, RedisValue>>>,
    ) -> Result<RespValue, ()> {
        let result = match &mut self {
            RedisCmd::Ping(None) => RespValue::SimpleString("PONG".into()),
            RedisCmd::Ping(Some(value)) => RespValue::BulkString(value.clone()),
            RedisCmd::Get(key) => {
                debug!("Getting key: {}", key);
                let storage = storage.lock().unwrap();
                if let Some(value) = storage.get(key) {
                    RespValue::BulkString(value.clone())
                } else {
                    RespValue::Null
                }
            }
            RedisCmd::Set(key, value) => {
                debug!("Setting: {}: {}", key, value);
                storage.lock().unwrap().insert(key.clone(), value.clone());
                RespValue::SimpleString("OK".into())
            }
            RedisCmd::Append(key, value) => {
                debug!("Setting: {}: {}", key, value);
                let mut storage = storage.lock().unwrap();
                let current_value = storage.entry(key.clone()).or_insert(BulkString("".into()));
                current_value.append(value);
                RespValue::Integer((&current_value).0.len() as i64)
            }
            RedisCmd::Keys(pattern) => {
                debug!("pattern: {}", pattern);
                let storage = storage.lock().unwrap();
                RespValue::Array(
                    storage
                        .keys()
                        .map(|k| RespValue::BulkString(k.clone()))
                        .collect(),
                )
            }
            RedisCmd::Exists(key) => {
                debug!("exists: {}", key);
                let storage = storage.lock().unwrap();
                RespValue::Integer(storage.contains_key(key).into())
            }
            // Unimplemented command
            cmd => {
                debug!("Unimplemented command: {:?}", cmd);
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
                    "APPEND" => Ok(RedisCmd::Append(
                        get_next_value(&mut resp)?,
                        get_next_value(&mut resp)?,
                    )),
                    "PING" => Ok(RedisCmd::Ping(get_next_value(&mut resp).ok())),
                    "KEYS" => Ok(RedisCmd::Keys(get_next_value(&mut resp)?)),
                    "EXISTS" => Ok(RedisCmd::Exists(get_next_value(&mut resp)?)),
                    "COMMAND" => Ok(RedisCmd::Command),
                    "" => Err("No command specified"),
                    _ => Err("Invalid command"),
                }
            }
            _ => Err("Invalid command"),
        }
    }
}
