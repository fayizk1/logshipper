mod cache;
mod sink;
mod tools;
use sink::s3::S3Sink;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread;

#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Greeting {
    label: BTreeMap<String, String>,
    content: HashMap<String, String>,
}

fn handle_client(mut stream: TcpStream, tx: SyncSender<Greeting>) {
    let mut data = [0 as u8; 1250]; // using 50 byte buffer
    'outer: while match stream.read(&mut data) {
        Ok(size) => {
            // echo everything!
            // stream.write(&data[0..size]).unwrap();
            let s = match std::str::from_utf8(&data[0..size]) {
                Ok(v) => v,
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };
            let de_serialized: Greeting = match serde_json::from_str(&s) {
                Ok(v) => v,
                Err(e) => {
                    if e.is_eof() {
                        return;
                    }
                    println!("Parse error {}", e);
                    continue 'outer;
                }
            };
            tx.send(de_serialized).unwrap();
            true
        }
        Err(_) => {
            println!(
                "An error occurred, terminating connection with {}",
                stream.peer_addr().unwrap()
            );
            stream.shutdown(Shutdown::Both).unwrap();
            false
        }
    } {}
}

fn main() {
    let listener = TcpListener::bind("0.0.0.0:3333").unwrap();
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
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let stream_tx = tx.clone();
                thread::spawn(move || {
                    // connection succeeded
                    handle_client(stream, stream_tx)
                });
            }
            Err(e) => {
                println!("Error: {}", e);
                /* connection failed */
            }
        }
    }
    // close the socket server
    drop(listener);
}
