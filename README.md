# logshipper

#build
cargo build --release

#test

`./target/release/logshipper &`

`minio server --address localhost:9000 /tmp/s3 &`

change key in s3.rs

`for i in `seq 0 300`; do echo '{"content": {"dasdasd": "dasdas"}, "label": {"targ": "fafsdasd"}}'| nc 127.0.0.1 3333; done`
