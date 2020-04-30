use std::thread;
use std::sync::Arc;
use std::collections::BTreeMap;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicUsize, Ordering};

use crc32fast::Hasher;
use fastcmp::Compare;

use r#async::rt::multi_thread::{MultiTaskPool, MultiTaskRuntime};
use hash::XHashMap;

use pi_store::log_store::log_file::{PairLoader, LogMethod, LogFile};

#[test]
fn test_crc32fast() {
    let mut hasher = Hasher::new();
    hasher.update(&vec![1, 1, 1]);
    hasher.update(&vec![10, 10, 10]);
    hasher.update(&vec![255, 10, 255, 10, 255, 10]);
    let hash = hasher.finalize();
    let mut hasher = Hasher::new();
    hasher.update(&vec![1, 1, 1, 10, 10, 10, 255, 10, 255, 10, 255, 10]);

    assert_eq!(hash, hasher.finalize());
}

#[test]
fn test_fastcmp() {
    let vec0: Vec<u8> = vec![1, 1, 1];
    let vec1: Vec<u8> = vec![1, 1, 1];

    assert!(vec0.feq(&vec1));
}

struct Counter(AtomicUsize, Instant);

impl Drop for Counter {
    fn drop(&mut self) {
        println!("!!!!!!drop counter, count: {:?}, time: {:?}", self.0.load(Ordering::Relaxed), Instant::now() - self.1);
    }
}

#[test]
fn test_log_append() {
    let pool = MultiTaskPool::new("Test-Log-Append".to_string(), 8, 1024 * 1024, 10, None);
    let rt = pool.startup(true);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::PlainAppend, key.as_slice(), value);
                        if let Err(e) = log_copy.commit(uid, true).await {
                            println!("!!!!!!append log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_remove() {
    let pool = MultiTaskPool::new("Test-Log-Remove".to_string(), 8, 1024 * 1024, 10, None);
    let rt = pool.startup(true);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::Remove, key.as_slice(), value);
                        if let Err(e) = log_copy.commit(uid, true).await {
                            println!("!!!!!!remove log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_read() {
    let pool = MultiTaskPool::new("Test-Log-Read".to_string(), 8, 1024 * 1024, 10, None);
    let rt = pool.startup(true);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let mut offset = None;
                loop {
                    if let Some(0) = offset {
                        break;
                    }

                    match log.read_block(None, offset, true).await {
                        Err(e) => {
                            println!("!!!!!!read log failed, e: {:?}", e);
                        },
                        Ok((next_offset, logs)) => {
                            offset = Some(next_offset);
                            println!("!!!!!!read log ok, offset: {:?}, len: {:?}", offset, logs.len());
                        },
                    }
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

struct TestCache {
    is_hidden_remove:   bool,
    removed:            XHashMap<Vec<u8>, ()>,
    map:                BTreeMap<Vec<u8>, Option<String>>,
}

impl PairLoader for TestCache {
    fn is_require(&self, key: &Vec<u8>) -> bool {
        !self.removed.contains_key(key) && !self.map.contains_key(key)
    }

    fn load(&mut self, _method: LogMethod, key: Vec<u8>, value: Option<Vec<u8>>) {
        if let Some(value) = value {
            unsafe {
                self.map.insert(key, Some(String::from_utf8_unchecked(value)));
            }
        } else {
            if self.is_hidden_remove {
                //忽略移除的键值对
                self.removed.insert(key, ());
            } else {
                self.map.insert(key, None);
            }
        }
    }
}

impl TestCache {
    pub fn new(is_hidden_remove: bool) -> Self {
        TestCache {
            is_hidden_remove,
            removed: XHashMap::default(),
            map: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

#[test]
fn test_log_load() {
    let pool = MultiTaskPool::new("Test-Log-Load".to_string(), 8, 1024 * 1024, 10, None);
    let rt = pool.startup(true);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let mut cache = TestCache::new(true);
                let start = Instant::now();
                match log.load(&mut cache, true).await {
                    Err(e) => {
                        println!("!!!!!!load log failed, e: {:?}", e);
                    },
                    Ok(_) => {
                        println!("!!!!!!load log ok, len: {:?}, time: {:?}", cache.len(), Instant::now() - start);
                    },
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_collect() {
    let pool = MultiTaskPool::new("Test-Log-Load".to_string(), 8, 1024 * 1024, 10, None);
    let rt = pool.startup(true);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let start = Instant::now();
                match log.collect(1024 * 1024, false).await {
                    Err(e) => {
                        println!("!!!!!!load log failed, e: {:?}", e);
                    },
                    Ok((size, len)) => {
                        println!("!!!!!!load log ok, size: {:?}, len: {:?}, time: {:?}", size, len, Instant::now() - start);
                    },
                }
            }
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_append_delay_commit() {
    let pool = MultiTaskPool::new("Test-Log-Commit".to_string(), 8, 1024 * 1024, 10, Some(10));
    let rt = pool.startup(true);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::PlainAppend, key.as_slice(), value);
                        if let Err(e) = log_copy.delay_commit(uid, 20).await {
                            println!("!!!!!!commit log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_log_remove_delay_commit() {
    let pool = MultiTaskPool::new("Test-Log-Commit".to_string(), 8, 1024 * 1024, 10, Some(10));
    let rt = pool.startup(true);

    let rt_copy = rt.clone();
    rt.spawn(rt.alloc(), async move {
        match LogFile::open(rt_copy.clone(),
                            "./log",
                            8000,
                            1024 * 1024).await {
            Err(e) => {
                println!("!!!!!!open log failed, e: {:?}", e);
            },
            Ok(log) => {
                let counter = Arc::new(Counter(AtomicUsize::new(0), Instant::now()));
                for index in 0..10000 {
                    let log_copy = log.clone();
                    let counter_copy = counter.clone();
                    rt_copy.spawn(rt_copy.alloc(), async move {
                        let key = ("Test".to_string() + index.to_string().as_str()).into_bytes();
                        let value = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".as_bytes();
                        let uid = log_copy.append(LogMethod::Remove, key.as_slice(), value);
                        if let Err(e) = log_copy.delay_commit(uid, 20).await {
                            println!("!!!!!!commit log failed, e: {:?}", e);
                        } else {
                            counter_copy.0.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                }
            },
        }
    });

    thread::sleep(Duration::from_millis(1000000000));
}