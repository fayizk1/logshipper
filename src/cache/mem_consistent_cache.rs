use hash_ring::HashRing;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex, RwLock};
#[derive(Debug)]
struct CacheMember {
    data: Vec<Vec<u8>>,
    len: usize,
}

pub struct MemCache {
    store: Arc<RwLock<HashMap<i8, CacheMember>>>,
    hash: Arc<Mutex<HashRing<i8>>>,
}

impl CacheMember {
    fn new() -> Self {
        let data_cache: Vec<Vec<u8>> = Vec::new();
        CacheMember {
            data: data_cache,
            len: 0,
        }
    }

    fn push(&mut self, data: Vec<u8>) {
        self.len += data.len();
        self.data.push(data);
        self.data.push("\n".as_bytes().to_vec());
    }
}

impl MemCache {
    pub fn new() -> Self {
        let data_map: HashMap<i8, CacheMember> = HashMap::new();
        let store = Arc::new(RwLock::new(data_map));
        let mut nodes: Vec<i8> = Vec::new();
        for i in 0..5 {
            nodes.push(i)
        }
        let hash_ring: HashRing<i8> = HashRing::new(nodes, 10);
        let hash = Arc::new(Mutex::new(hash_ring));
        MemCache { store, hash }
    }

    pub fn push(&self, key: String, data: Vec<u8>) -> std::io::Result<()> {
        let hash_lock = self.hash.clone();
        let mut hash_ring = hash_lock.lock().unwrap();
        let nid = hash_ring.get_node((key).to_string()).unwrap();
        let data_map_lock = self.store.clone();
        let mut data_map = data_map_lock.write().unwrap();
        match data_map.entry(*nid) {
            Entry::Vacant(e) => {
                let mut member = CacheMember::new();
                member.push(data);
                e.insert(member);
            }
            Entry::Occupied(mut e) => {
                e.get_mut().push(data);
            }
        }
        Ok(())
    }

    pub fn pop(&self, key: i8) -> std::io::Result<Vec<Vec<u8>>> {
        let data_map_lock = self.store.clone();
        let mut data_map = data_map_lock.write().unwrap();
        match data_map.remove(&key) {
            Some(v) => Ok(v.data),
            None => Err(Error::new(ErrorKind::Other, "Empty result")),
        }
    }

    pub fn size(&self, key: i8) -> usize {
        let data_map_lock = self.store.clone();
        let data_map = data_map_lock.read().unwrap();
        match data_map.get(&key) {
            Some(values) => values.len,
            None => 0,
        }
    }

    pub fn keys(&self) -> Vec<i8> {
        let data_map_lock = self.store.clone();
        let data_map = data_map_lock.read().unwrap();
        let mut keys = Vec::new();
        for k in data_map.keys() {
            keys.push(*k);
        }
        keys
    }
}
