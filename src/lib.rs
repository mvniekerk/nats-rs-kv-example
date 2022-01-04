use bytevec::{ByteDecodable, ByteEncodable};
use nats::jetstream::JetStream;
pub use nats::kv::Entry;
pub use nats::kv::Operation;
use nats::kv::{Config, Store, Watch};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tracing::{debug, error, info};

pub const HISTORY: i64 = 5;

#[derive(Clone)]
pub struct NatsConnection {
    pub context: Arc<RwLock<JetStream>>,
    pub buckets: Arc<RwLock<HashMap<String, Store>>>,
}

pub fn sanitize_nats_key(key: &str) -> String {
    key.replace('#', ".")
        .replace(':', ".")
        .replace('/', ".")
        .replace('=', "_")
}

pub fn sanitize_nats_bucket(bucket: &str) -> String {
    sanitize_nats_key(bucket).replace('.', "-")
}

pub struct KvSubscribeResult {
    pub terminate: Sender<bool>,
    pub waiting_for: Arc<RwLock<Option<HashSet<String>>>>,
}

impl NatsConnection {
    pub fn new() -> io::Result<Self> {
        let nats_host =
            std::env::var("NATS_HOST").unwrap_or_else(|_| "nats://localhost".to_string());
        let connection = {
            let username = std::env::var("NATS_USERNAME");
            let password = std::env::var("NATS_PASSWORD");
            match (username, password) {
                (Ok(username), Ok(password)) => {
                    nats::Options::with_user_pass(&*username, &*password)
                        .connect(&*nats_host)
                }
                _ => nats::connect(&*nats_host),
            }?
        };
        let context = nats::jetstream::new(connection);
        Ok(NatsConnection {
            context: Arc::new(RwLock::new(context)),
            buckets: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn kv_value_store(&self, bucket: &str) -> Result<Store, Box<dyn Error>> {
        let bucket = &*sanitize_nats_bucket(bucket);
        let kvs = {
            let r = self.buckets.clone();
            let r = r.write().unwrap();
            let bv = r.get(bucket);
            bv.cloned()
        };

        match kvs {
            Some(kvs) => Ok(kvs),
            None => {
                let w = self.context.clone();
                let context = w.write().unwrap();
                let bucket = bucket.to_string();
                let kvs = context.create_key_value(&Config {
                    bucket: bucket.clone(),
                    history: HISTORY,
                    ..Default::default()
                })?;
                let kvs_cp = kvs.clone();
                let w = self.buckets.clone();
                let mut w = w.write().unwrap();
                w.insert(bucket, kvs);
                Ok(kvs_cp)
            }
        }
    }

    pub fn kv_put<T>(&self, key: &str, val: T, bucket: &str) -> Result<(), Box<dyn Error>>
        where
            T: ByteEncodable,
    {
        let key = &*sanitize_nats_key(key);
        let bucket = &*sanitize_nats_bucket(bucket);
        let bucket_store = self.kv_value_store(bucket)?;
        let val = val.encode::<u32>()?;
        let md5 = md5::compute(val.as_slice());

        info!(?md5, "KV_PUT {} : {}", bucket, key);

        if bucket_store.put(key, &val).is_err() {
            bucket_store.create(key, val)?;
        }

        Ok(())
    }

    pub fn kv_subscribe(&self, bucket: &str) -> Result<Watch, Box<dyn Error>> {
        let bucket = &*sanitize_nats_bucket(bucket);
        let kvs = self.kv_value_store(bucket)?;
        let sub = kvs.watch()?;
        Ok(sub)
    }

    pub fn kv_key_subscribe_on_mpsc_own_thread(
        &self,
        key: &str,
        bucket: &str,
        send: UnboundedSender<Entry>,
    ) -> Result<KvSubscribeResult, Box<dyn Error>> {
        let bucket = &*sanitize_nats_bucket(bucket);
        let key = sanitize_nats_key(key);
        info!("KV_SUB_MPSC {} : {}", bucket, key);
        let mut sub = self.kv_subscribe(bucket)?;
        let mut waiting_for = HashSet::new();
        waiting_for.insert(key);
        let waiting_for = Arc::new(RwLock::new(Option::Some(waiting_for)));

        let waiting_for_update = waiting_for.clone();

        std::thread::spawn(move || {
            loop {
                if let Some(entry) = sub.next() {
                    let md5 = md5::compute(entry.value.as_slice());

                    info!(?md5, "Entry {} : {} : {}", entry.bucket, entry.key, entry.revision);
                    {
                        let r = waiting_for_update.read().unwrap();
                        if r.as_ref()
                            .filter(|k| k.len() == 1 && k.contains("__POISONED"))
                            .is_some()
                        {
                            break;
                        }
                        if r.as_ref()
                            .filter(|keys| keys.contains("*") || keys.contains(&entry.key))
                            .is_none()
                        {
                            continue;
                        }
                    }

                    if let Err(err) = send.send(entry) {
                        error!(%err, "Error sending new value");
                    }
                } else {
                    let r = waiting_for_update.read().unwrap();
                    if r.as_ref()
                        .filter(|k| k.len() == 1 && k.contains("__POISONED"))
                        .is_some()
                    {
                        break;
                    }
                }
            }
            error!("Done listening");
        });

        let waiting_for_terminate = waiting_for.clone();
        let (terminate, mut terminate_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            if terminate_rx.recv().await.is_none() {
                error!("Error listening to terminate signal");
            }
            let l = HashSet::from(["__POISONED".to_ascii_lowercase()]);
            let mut w = waiting_for_terminate.write().unwrap();
            w.replace(l);
        });

        Ok(KvSubscribeResult {
            waiting_for,
            terminate,
        })
    }

    pub fn kv_key_subscribe_on_mpsc_tokio(
        &self,
        key: &str,
        bucket: &str,
        send: Sender<Entry>,
    ) -> Result<KvSubscribeResult, Box<dyn Error>> {
        let bucket = &*sanitize_nats_bucket(bucket);
        let key = sanitize_nats_key(key);
        info!("KV_SUB_MPSC {} : {}", bucket, key);
        let mut sub = self.kv_subscribe(bucket)?;
        let mut waiting_for = HashSet::new();
        waiting_for.insert(key);
        let waiting_for = Arc::new(RwLock::new(Option::Some(waiting_for)));

        let waiting_for_update = waiting_for.clone();

        tokio::spawn(async move {
            loop {
                if let Some(entry) = sub.next() {
                    let md5 = md5::compute(entry.value.as_slice());

                    info!(?md5, "Entry {} : {} : {}", entry.bucket, entry.key, entry.revision);
                    {
                        let r = waiting_for_update.read().unwrap();
                        if r.as_ref()
                            .filter(|k| k.len() == 1 && k.contains("__POISONED"))
                            .is_some()
                        {
                            break;
                        }
                        if r.as_ref()
                            .filter(|keys| keys.contains("*") || keys.contains(&entry.key))
                            .is_none()
                        {
                            continue;
                        }
                    }

                    if let Err(err) = send.send(entry).await {
                        error!(%err, "Error sending new value");
                    }
                } else {
                    let r = waiting_for_update.read().unwrap();
                    if r.as_ref()
                        .filter(|k| k.len() == 1 && k.contains("__POISONED"))
                        .is_some()
                    {
                        break;
                    }
                }
            }
            error!("Done listening");
        });

        let waiting_for_terminate = waiting_for.clone();
        let (terminate, mut terminate_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            if terminate_rx.recv().await.is_none() {
                error!("Error listening to terminate signal");
            }
            let l = HashSet::from(["__POISONED".to_ascii_lowercase()]);
            let mut w = waiting_for_terminate.write().unwrap();
            w.replace(l);
        });

        Ok(KvSubscribeResult {
            waiting_for,
            terminate,
        })
    }

    pub fn kv_put_items(
        &self,
        items: Vec<(String, Vec<u8>)>,
        bucket: &str,
    ) -> Result<(), Box<dyn Error>> {
        for (key, val) in items {
            self.kv_put(&*key, val, bucket)?;
        }
        Ok(())
    }

    pub fn kv_keys(&self, bucket: &str) -> Result<Vec<String>, Box<dyn Error>> {
        let bucket = &*sanitize_nats_bucket(bucket);
        info!("KV_BUCKET_LIST {}", bucket);
        let bucket = self.kv_value_store(bucket)?;
        let keys = bucket.keys()?;

        let mut ret = vec![];
        for key in keys {
            if key.ends_with(".previous") || key.ends_with(".previous_ts") {
                continue;
            }
            ret.push(key.to_string());
        }

        Ok(ret)
    }
}

pub fn key_value_entry_to_value<T>(kv: &Entry) -> Result<T, Box<dyn Error>>
    where
        T: ByteDecodable,
{
    let v: T = <T>::decode::<u32>(kv.value.as_slice())?;
    Ok(v)
}

#[cfg(test)]
mod test {
    use crate::NatsConnection;
    use tracing::{debug, error, info};

    // Passes
    #[test]
    fn get_values_bucket_0() {
        let key = "k0".to_string();

        let k = key.clone();
        std::thread::spawn(move || {
            let con = NatsConnection::new().unwrap();
            for i in 0..4_000u32 {
                con.kv_put(&*k, i, &*format!("b{}", i % 2))
                    .unwrap();
            }
        });

        let mut vals = vec![];
        let (tx, mut rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let con = NatsConnection::new().unwrap();

            for entry in con.kv_subscribe("b0").unwrap() {
                info!("Entry {}:{}", entry.bucket, entry.key);
                tx.send(entry);
            }
        });

        for _i in 0..500 {
            let v = rx.recv().unwrap();
            assert_eq!(&*v.key, "k0");
            vals.push(v);
        }

        assert_eq!(vals.len(), 500);
    }

    // This fails
    #[test]
    fn get_values_bucket_1() {
        let key = "k0".to_string();

        let k = key.clone();
        std::thread::spawn(move || {
            let con = NatsConnection::new().unwrap();

            // Doesnt matter which one is chosen, both fails
            // for i in 0..4_000u32 {
            for i in 1..4_000u32 {

                // This will alternately put a value into b0 or b1
                con.kv_put(&*k, i, &*format!("b{}", i % 2))
                    .unwrap();
            }
        });

        let mut vals = vec![];
        let (tx, mut rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let con = NatsConnection::new().unwrap();

            for entry in con.kv_subscribe("b1").unwrap() {
                info!("Entry {}:{}", entry.bucket, entry.key);
                tx.send(entry);
            }
        });

        for _i in 0..500 {
            let v = rx.recv().unwrap();
            assert_eq!(&*v.key, "k0");
            vals.push(v);
        }

        assert_eq!(vals.len(), 500);
    }

    #[test]
    fn mpsc_own_thread_bucket_0() {
        std::thread::spawn(move || {
            let con = NatsConnection::new().unwrap();
            for i in 0..2_000u32 {
                con.kv_put(&*format!("k{}", i % 2), i, &*format!("b{}", i % 2))
                    .unwrap();
            }
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut vals = vec![];
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let con = NatsConnection::new().unwrap();
            let sub = con.kv_key_subscribe_on_mpsc_own_thread("k0", "b0", tx).unwrap();

            for _i in 0..500 {
                let v = rx.recv().await.unwrap();
                assert_eq!(&*v.key, "k0");
                assert_eq!(&*v.bucket, "b0");
                vals.push(v);
            }

            assert_eq!(vals.len(), 500);
        })
    }

    // Fails
    #[test]
    fn mpsc_own_thread_bucket_1() {
        std::thread::spawn(move || {
            let con = NatsConnection::new().unwrap();
            for i in 0..2_000u32 {
                con.kv_put(&*format!("k{}", i % 2), i, &*format!("b{}", i % 2))
                    .unwrap();
            }
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut vals = vec![];
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let con = NatsConnection::new().unwrap();
            let sub = con.kv_key_subscribe_on_mpsc_own_thread("k1", "b1", tx).unwrap();

            for _i in 0..500 {
                let v = rx.recv().await.unwrap();
                assert_eq!(&*v.key, "k1");
                assert_eq!(&*v.bucket, "b1");
                vals.push(v);
            }

            assert_eq!(vals.len(), 500);
        })
    }

    // Hangs
    #[test]
    fn mpsc_tokio_hang_thread_bucket_0() {
        std::thread::spawn(move || {
            let con = NatsConnection::new().unwrap();
            for i in 0..2_000u32 {
                con.kv_put(&*format!("k{}", i % 2), i, &*format!("b{}", i % 2))
                    .unwrap();
            }
        });

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let mut vals = vec![];
            let (tx, mut rx) = tokio::sync::mpsc::channel(200);
            let con = NatsConnection::new().unwrap();
            let sub = con.kv_key_subscribe_on_mpsc_tokio("k0", "b0", tx).unwrap();

            for _i in 0..500 {
                let v = rx.recv().await.unwrap();
                assert_eq!(&*v.key, "k0");
                assert_eq!(&*v.bucket, "b0");
                vals.push(v);
            }

            assert_eq!(vals.len(), 500);
        })
    }
}
