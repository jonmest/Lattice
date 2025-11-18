pub mod kv {
    tonic::include_proto!("kv");
}

use kv::key_value_store_server::KeyValueStoreServer;
