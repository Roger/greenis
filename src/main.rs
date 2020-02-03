mod codec;
mod types;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use futures::stream::StreamExt;
use tokio::net::TcpListener;

use futures::prelude::*;
use tokio;
use tokio_util::codec::Framed;

use codec::RespCodec;
use types::{RedisCmd, RedisKey, RedisValue, RespValue};

#[macro_use]
extern crate log;

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
                    None => break,
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
