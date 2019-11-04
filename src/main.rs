mod cache;
mod sink;
mod tools;
use sink::s3::S3Sink;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread;
use tokio::io::{AsyncRead, Error};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Greeting {
    label: BTreeMap<String, String>,
    content: HashMap<String, String>,
}

struct Receiver {
    rx: TcpStream,
    tx: SyncSender<Greeting>,
}
impl Future for Receiver {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut buffer = vec![0u8; 1250];
        'outer: while let Async::Ready(num_bytes_read) = self.rx.poll_read(&mut buffer)? {
            if num_bytes_read == 0 {
                return Ok(Async::Ready(()));
            } //socket closed
            let s = match std::str::from_utf8(&buffer[0..num_bytes_read]) {
                Ok(v) => v,
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };
            let de_serialized: Greeting = match serde_json::from_str(&s) {
                Ok(v) => v,
                Err(e) => {
                    if e.is_eof() {
                        return Ok(Async::Ready(()));
                    }
                    println!("Parse error {}", e);
                    continue 'outer;
                }
            };
            self.tx.send(de_serialized).unwrap();
        }
        return Ok(Async::NotReady);
    }
}

fn main() {
    let addr = "0.0.0.0:3333".to_string().parse::<SocketAddr>().unwrap();
    let listener = TcpListener::bind(&addr).unwrap();
    // accept connections and process them, spawning a new thread for each one
    let (tx, rx) = sync_channel::<Greeting>(2);
    let s3 = S3Sink::new();
    thread::spawn(move || {
        s3.run();
        loop {
            let rc_data = rx.recv().unwrap();
            s3.push(
                tools::common::join_btree_map(rc_data.label),
                serde_json::to_vec(&rc_data.content).unwrap(),
            )
            .unwrap();
        }
    });
    let server = listener
        .incoming()
        .map_err(|e| println!("failed to accept socket; error = {:?}", e))
        .for_each(move |stream| {
            let stream_tx = tx.clone();
            let receiver = Receiver {
                rx: stream,
                tx: stream_tx,
            };
            tokio::spawn(receiver.map_err(|e| println!("{}", e)))
        });
    tokio::run(server);
}
