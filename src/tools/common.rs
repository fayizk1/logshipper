use std::collections::BTreeMap;

pub fn join_btree_map(dict: BTreeMap<String, String>) -> String {
    let mut kv = String::from("");
    for (k, v) in dict.iter() {
        kv.push_str(&format!("{}={}", k, v));
    }
    kv
}
