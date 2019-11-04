extern crate hash_ring;
use crate::cache::mem_consistent_cache::MemCache;
use crate::tools::chunked_split::ChunkedSplitTrait;
use rand::Rng;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_s3::{
    CreateBucketError, CreateBucketRequest, HeadBucketRequest, PutObjectRequest, S3Client, S3,
};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{thread, time};

pub struct S3Sink {
    cache: Arc<MemCache>,
    client: S3Client,
}

impl S3Sink {
    pub fn new() -> Self {
        let cache = Arc::new(MemCache::new());
        let region = Region::Custom {
            name: "minio".to_owned(),
            endpoint: "http://localhost:9000".to_owned(),
        };

        let p = StaticProvider::new_minimal(
            "4GPPRHVT0AN4J16KF5I0".into(),
            "N2Nmnk7cIj3rh0M3hDDxIHKl10nZppiopHXjEOya".into(),
        );
        let d = HttpClient::new().unwrap();
        let client = S3Client::new_with(d, p, region);
        S3Sink { cache, client }
    }

    pub fn run(&self) {
        let cache = self.cache.clone();
        let client = self.client.clone();
        let dur = time::Duration::from_secs(300);
        let ret_dur = time::Duration::from_secs(1);
        println!("Initializing the S3");
        thread::spawn(move || loop {
            thread::sleep(dur);
            let now = SystemTime::now();
            let date = now.duration_since(UNIX_EPOCH).unwrap().as_secs();
            let keys = cache.keys();
            for key in keys {
                let values = cache.pop(key).unwrap();
                let mut body_full: Vec<u8> = Vec::new();
                for mut value in values {
                    body_full.append(&mut value);
                }
                let key_bucket_name = format!("hash{}", key);
                let key_head_request = HeadBucketRequest {
                    bucket: key_bucket_name.to_string(),
                };
                let key_head_response = client.head_bucket(key_head_request).sync();
                let is_key_bucket_exist = match key_head_response {
                    Ok(_) => true,
                    Err(_) => false,
                };

                if !is_key_bucket_exist {
                    'create_key: loop {
                        let req = CreateBucketRequest {
                            bucket: key_bucket_name.to_string(),
                            ..Default::default()
                        };

                        let res = client.create_bucket(req);
                        match res.sync() {
                            Ok(_)
                            | Err(RusotoError::Service(CreateBucketError::BucketAlreadyExists(
                                _,
                            )))
                            | Err(RusotoError::Service(
                                CreateBucketError::BucketAlreadyOwnedByYou(_),
                            )) => break 'create_key,
                            Err(e) => {
                                println!("Error creating key bucket: {}, retrying", e);
                                thread::sleep(ret_dur);
                                continue 'create_key;
                            }
                        }
                    }
                }
                /*
                                let date_bucket_name = format!("{}/{}", key_bucket_name.to_string(), date);
                                let date_head_request = HeadBucketRequest {
                                    bucket: date_bucket_name.clone().to_string(),
                                };


                                                let date_head_response = client.head_bucket(date_head_request).sync();
                                let is_date_bucket_exist = match date_head_response {
                                    Ok(_) => true,
                                    Err(_) => false,
                                };

                                if !is_date_bucket_exist {
                                    'create_date: loop {
                                        let req = CreateBucketRequest {
                                            bucket: date_bucket_name.clone().to_string(),
                                            ..Default::default()
                                        };

                                        let res = client.create_bucket(req);
                                        match res.sync() {
                                            Ok(_) | Err(RusotoError::Service(CreateBucketError::BucketAlreadyExists(_))) => {
                                                println!("Created bucket {}",  date_bucket_name);
                                                break 'create_date
                                            },
                                            Err(e) =>  {
                                                println!("Error creating date bucket: {}, retrying",e );
                                                thread::sleep(ret_dur);
                                                continue 'create_date},
                                        }
                                    }
                                }
                */
                for body in body_full.chucked_split(10, 10000) {
                    'put_obj: loop {
                        match client
                            .put_object(PutObjectRequest {
                                bucket: key_bucket_name.clone(),
                                key: get_filename(date),
                                body: Some(body.to_vec().into()),
                                acl: Some("public-read".to_string()),
                                ..Default::default()
                            })
                            .sync()
                        {
                            Ok(_) => break 'put_obj,
                            Err(e) => {
                                println!("Error uploading, {}; retrying", e);
                                thread::sleep(ret_dur);
                                continue 'put_obj;
                            }
                        }
                    }
                }
            }
        });
    }

    pub fn push(&self, key: String, data: Vec<u8>) -> std::io::Result<()> {
        let cache = self.cache.clone();
        cache.push(key, data).unwrap();
        Ok(())
    }
}

fn get_filename(data: u64) -> String {
    let mut rng = rand::thread_rng();
    format!("{}/{}", data, rng.gen::<u32>())
}
