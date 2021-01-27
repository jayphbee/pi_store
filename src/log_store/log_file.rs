use std::pin::Pin;
use std::sync::Arc;
use std::mem::drop;
use std::fmt::Debug;
use std::fs::read_dir;
use std::time::SystemTime;
use std::path::{Path, PathBuf};
use std::collections::{LinkedList, VecDeque};
use std::io::{Error, Result, ErrorKind, Cursor};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use bytes::{Buf, BufMut};
use crc32fast::Hasher;
use fastcmp::Compare;
use crossbeam_channel::{Sender, Receiver, unbounded};
use log::{error, warn};

use r#async::{lock::{spin_lock::SpinLock, mutex_lock::Mutex},
              rt::{AsyncValue, AsyncRuntime, multi_thread::MultiTaskRuntime}};
use async_file::file::{AsyncFileOptions, WriteOptions, AsyncFile, create_dir, rename, remove_file};
use hash::XHashMap;

/*
* 默认的日志文件块头长度
*/
const DEFAULT_LOG_BLOCK_HEADER_LEN: usize = 16;

/*
* 默认的日志文件名宽度
*/
const DEFAULT_LOG_FILE_NAME_WIDTH: usize = 6;

/*
* 默认的初始日志文件数字
*/
const DEFAULT_INIT_LOG_FILE_NUM: usize = 1;

/*
* 默认的初始日志唯一id
*/
const DEFAULT_INIT_LOG_UID: usize = 0;

/*
* 日志块的标准长度，4KB
*/
const LOG_BLOCK_MOD: usize = 4096;

/*
* 最小日志块大小限制，32B
*/
const MIN_LOG_BLOCK_SIZE_LIMIT: usize = 32;

/*
* 最大日志块大小限制，2GB
*/
const MAX_LOG_BLOCK_SIZE_LIMIT: usize = 2 * 1024 * 1024 * 1024;

/*
* 默认的日志块大小限制，8000B
*/
const DEFAULT_LOG_BLOCK_SIZE_LIMIT: usize = 8000;

/*
* 最小日志文件大小限制，1MB
*/
const MIN_LOG_FILE_SIZE_LIMIT: usize = 1024 * 1024;

/*
* 最大日志文件大小限制，16GB
*/
const MAX_LOG_FILE_SIZE_LIMIT: usize = 16 * 1024 * 1024 * 1024;

/*
* 默认的日志文件大小限制，16MB
*/
const DEFAULT_LOG_FILE_SIZE_LIMIT: usize = 16 * 1024 * 1024;

/*
* 默认的日志合并缓冲区附加大小，1KB
*/
const DEFAULT_MERGE_LOG_BUF_SIZE: usize  = 1024;

/*
* 默认的备份日志文件扩展名
*/
const DEFAULT_BAK_LOG_FILE_EXT: &str = "bak";

/*
* 默认的临时日志文件扩展名
*/
const DEFAULT_TMP_LOG_FILE_EXT: &str = "tmp";

/*
* 默认的整理后的只读日志文件的初始扩展名
*/
const DEFAULT_COLLECTED_LOG_FILE_INIT_EXT: &str = "0";

/*
* 键值对加载器
*/
pub trait PairLoader {
    //判断是否需要加载关键字的键值对
    fn is_require(&self, log_file: Option<&PathBuf>, key: &Vec<u8>) -> bool;

    //加载指定键值对，值为None表示此关键字的键值对已被移除
    fn load(&mut self, log_file: Option<&PathBuf>, method: LogMethod, key: Vec<u8>, value: Option<Vec<u8>>);
}

/*
* 日志操作方法
*/
#[derive(Clone, Copy, Debug)]
pub enum LogMethod {
    Remove = 0,     //删除日志
    PlainAppend,    //明文追加日志
    //TODO 指定压缩算法和压缩级别的追加日志...
}

impl LogMethod {
    //将u8转换为日志操作方法
    pub fn with_tag(tag: u8) -> Self {
        match tag {
            0 => LogMethod::Remove,
            1 => LogMethod::PlainAppend,
            _ => unimplemented!(),
        }
    }
}

/*
* 日志文件块
*/
pub struct LogBlock {
    buf:    Vec<u8>,    //日志缓冲区
    hasher: Hasher,     //校验码生成器
}

impl From<LogBlock> for Vec<u8> {
    fn from(block: LogBlock) -> Self {
        let LogBlock {
            mut buf,
            mut hasher,
        } = block;

        //检查二进制长度
        let len = buf.len();
        if len == 0 {
            panic!("Convert log block to binary failed, reason: invalid binary len");
        }

        //写入日志块头
        write_header(&mut buf, hasher, len);

        buf
    }
}

impl LogBlock {
    //构建日志文件块，预分配块内存
    pub fn new(size_limit: usize) -> Self {
        let buf = Vec::with_capacity(calc_block_alloc_len(size_limit));
        LogBlock {
            buf: buf,
            hasher: Hasher::new(),
        }
    }

    //获取块大小
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    //检查当前日志块是否满足指定的同步大小限制
    pub fn is_size_limit(&self, size_limit: usize) -> bool {
        self.buf.len() >= size_limit
    }

    //追加日志内容，返回追加日志大小，结构: 1字节方法标记 + 2字节关键字长度 + 关键字 + 4字节值长度 + 值
    pub fn append(&mut self, method: LogMethod, key: &[u8], value: &[u8]) {
        let buf = &mut self.buf;
        let hasher = &mut self.hasher;
        let offset = buf.len();

        //写入缓冲区，并更新校验码
        if let LogMethod::Remove = method {
            //移除日志
            write_buf(buf, method, key, &[]);
        } else {
            //写入日志
            write_buf(buf, method, key, value);
        }
        hasher.update(&buf[offset..]);
    }
}

//获取当前系统时间，错误返回0，单位ms
fn now_unix_epoch() -> u64 {
    if let Ok(time) = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        return time.as_millis() as u64;
    }

    0
}

//获取指定块长度需要的预分配内存长度
fn calc_block_alloc_len(block_len: usize) -> usize {
    let rem = block_len % LOG_BLOCK_MOD;
    if block_len % LOG_BLOCK_MOD == 0 {
        //指定的块长度是标准块长度的正整数倍
        block_len
    } else {
        //指定的块长度不是标准块长度的正整数倍，则转换为标准块长度的正整数倍
        LOG_BLOCK_MOD - rem + block_len
    }
}

//将日志内容写入缓冲区
#[inline]
fn write_buf(buf: &mut Vec<u8>, method: LogMethod, key: &[u8], value: &[u8]) {
    let key_len = key.len();
    let value_len = value.len();

    //写入日志内容
    buf.put_slice(&[method as u8, (key_len & 0xff) as u8, (key_len >> 8 & 0xff) as u8]); //写入方法标记1字节和关键字小端序2字节
    buf.put_slice(key); //写入关键字
    if value_len > 0 {
        buf.put_u32_le(value_len as u32); //写入值长度
        buf.put_slice(value);  //写入值
    }
}

//将日志头写入缓冲区
#[inline]
fn write_header(buf: &mut Vec<u8>, mut hasher: Hasher, len: usize) {
    //填充当前系统时间，并更新校验码
    buf.put_u64_le(now_unix_epoch());
    hasher.update(&buf[buf.len() - 8..]);

    //计算并填充校验码
    let hash = hasher.finalize();
    buf.put_u32_le(hash);

    //填充负载长度
    buf.put_u32_le(len as u32);
}

/*
* 日志文件
*/
#[derive(Clone)]
pub struct LogFile(Arc<InnerLogFile>);

unsafe impl Send for LogFile {}
unsafe impl Sync for LogFile {}

/*
* 日志文件同步方法
*/
impl LogFile {
    //获取只读日志文件数量
    pub fn readable_amount(&self) -> usize {
        unsafe { (&*self.0.readable.load(Ordering::Relaxed)).len() }
    }

    //获取只读日志文件的大小，单位字节
    pub fn readable_size(&self) -> u64 {
        let mut size = 0;
        unsafe {
            let files = Box::from_raw(self.0.readable.load(Ordering::Relaxed));
            for (_, file) in files.iter() {
                size += file.get_size();
            }
            Box::into_raw(files); //避免被回收
        }

        size
    }

    //获取当前可写日志文件大小，单位字节
    pub fn writable_size(&self) -> usize {
        self.0.writable_len.load(Ordering::Relaxed)
    }

    //获取当前已提交的最大日志id
    pub fn commited_uid(&self) -> usize {
        self.0.commited_uid.load(Ordering::Relaxed)
    }

    //追加指定关键字的日志，返回日志id
    pub fn append(&self, method: LogMethod, key: &[u8], value: &[u8]) -> usize {
        let mut lock = self.0.current.lock();
        (&mut *lock).0.as_mut().unwrap().append(method, key, value);
        (*lock).1 += 1;
        (*lock).1
    }
}

/*
* 日志文件异步方法
*/
impl LogFile {
    //打开指定本地路径的日志文件
    pub async fn open<P: AsRef<Path> + Debug>(rt: MultiTaskRuntime<()>,
                                              path: P,
                                              mut block_size_limit: usize,
                                              mut log_size_limit: usize,
                                              log_file_index: Option<usize>) -> Result<Self> {
        if !path.as_ref().exists() {
            //指定的路径不存在，则线程安全的创建指定路径
            if let Err(e) = create_dir(rt.clone(), path.as_ref().to_path_buf()).await {
                //创建指定路径的目录失败，则立即返回
                return Err(e);
            }
        }

        if block_size_limit < MIN_LOG_BLOCK_SIZE_LIMIT || block_size_limit > MAX_LOG_BLOCK_SIZE_LIMIT {
            //无效的日志块大小限制，则设置为默认的日志块大小限制
            block_size_limit = DEFAULT_LOG_BLOCK_SIZE_LIMIT;
        }

        if log_size_limit < MIN_LOG_FILE_SIZE_LIMIT || log_size_limit > MAX_LOG_FILE_SIZE_LIMIT {
            //无效的日志文件大小限制，则设置为默认日志文件大小限制
            log_size_limit = DEFAULT_LOG_FILE_SIZE_LIMIT;
        }

        match open_logs(&rt, &path).await {
            Ok((mut log_files, last_log_name)) => {
                //打开所有日志文件成功
                if log_files.len() == 0 {
                    //没有任何的日志文件，则初始化日志文件
                    let mut init_log_file_index = DEFAULT_INIT_LOG_FILE_NUM; //初始化默认的日志文件序号
                    if let Some(log_file_index) = log_file_index {
                        //初始化指定的日志文件序号
                        init_log_file_index = log_file_index;
                    }

                    let init_log_file = path.as_ref().to_path_buf().join(create_log_file_name(DEFAULT_LOG_FILE_NAME_WIDTH, init_log_file_index));
                    match AsyncFile::open(rt.clone(), init_log_file.clone(), AsyncFileOptions::ReadAppend).await {
                        Err(e) => {
                            Err(Error::new(ErrorKind::Other, format!("Open init log file failed, file: {:?}, reason: {:?}", init_log_file, e)))
                        },
                        Ok(writable) => {
                            let writable = AtomicPtr::new(Box::into_raw(Box::new(Some((init_log_file, writable)))));
                            let readable = AtomicPtr::new(Box::into_raw(Box::new(Vec::new())));
                            let current = SpinLock::new((Some(LogBlock::new(block_size_limit)), DEFAULT_INIT_LOG_UID));
                            let commit_lock = Mutex::new(VecDeque::new());
                            let inner = InnerLogFile {
                                rt: rt.clone(),
                                path: path.as_ref().to_path_buf(),
                                size_limit: log_size_limit,
                                log_id: AtomicUsize::new(init_log_file_index + 1),
                                writable_len: AtomicUsize::new(0),
                                writable,
                                readable,
                                current_limit: block_size_limit,
                                current,
                                delay_commit: AtomicBool::new(false),
                                commited_uid: AtomicUsize::new(0),
                                commit_lock,
                                mutex_status: AtomicBool::new(false),
                            };

                            Ok(LogFile(Arc::new(inner)))
                        },
                    }
                } else {
                    //已存在日志文件
                    if let Some(last_log_name) = last_log_name {
                        //获取字符序最大的日志文件名成功
                        if let Some(last_log_id) = log_file_name_to_usize(&last_log_name) {
                            //获取最大的日志文件唯一id成功
                            let (last_log_path, last_log_file) = log_files.pop().unwrap();
                            let writable_len = AtomicUsize::new(last_log_file.get_size() as usize); //设置当前可写日志文件大小
                            let writable = AtomicPtr::new(Box::into_raw(Box::new(Some((last_log_path, last_log_file)))));
                            let readable = AtomicPtr::new(Box::into_raw(Box::new(log_files)));
                            let current = SpinLock::new((Some(LogBlock::new(block_size_limit)), DEFAULT_INIT_LOG_UID));
                            let commit_lock = Mutex::new(VecDeque::new());
                            let inner = InnerLogFile {
                                rt: rt.clone(),
                                path: path.as_ref().to_path_buf(),
                                size_limit: log_size_limit,
                                log_id: AtomicUsize::new(last_log_id + 1),
                                writable_len,
                                writable,
                                readable,
                                current_limit: block_size_limit,
                                current,
                                delay_commit: AtomicBool::new(false),
                                commited_uid: AtomicUsize::new(0),
                                commit_lock,
                                mutex_status: AtomicBool::new(false),
                            };

                            return Ok(LogFile(Arc::new(inner)));
                        }
                    }

                    Err(Error::new(ErrorKind::Other, "Open log file failed, reason: invalid last file"))
                }
            },
            Err(e) => Err(e),
        }
    }

    //加载日志文件的内容到指定缓存，可以指定只读日志文件的路径，需要合并日志
    pub async fn load<C: PairLoader>(&self,
                                     cache: &mut C,
                                     path: Option<PathBuf>,
                                     is_checksum: bool) -> Result<()> {
        let log_index = unsafe { get_log_index(path.as_ref(), &*self.0.readable.load(Ordering::Relaxed)) };
        let mut offset = None;

        //加载当前可写日志文件的内容
        if log_index.is_none() {
            loop {
                match load_block(self,
                                 cache,
                                 None,
                                 offset,
                                 is_checksum).await {
                    Err(e) => {
                        //读可写日志文件的指定二进制块失败，则立即返回错误
                        return Err(e);
                    },
                    Ok(None) => {
                        //读可写日志文件完成，则重置日志文件位置，并开始读只读日志文件
                        offset = None;
                        break;
                    },
                    Ok(Some(next)) => {
                        //更新日志文件位置，，并继续读当前可写日志文件的下一个日志块
                        offset = Some(next);
                    },
                }
            }
        }

        //加载只读日志文件的内容
        let readable_box = unsafe { Box::from_raw(self.0.readable.load(Ordering::Relaxed)) };
        let len = (&*readable_box).len();
        Box::into_raw(readable_box); //避免被提前释放

        let mut indexes = Vec::new();
        match log_index {
            None => {
                //加载所有只读日志文件
                for index in 0..len {
                    indexes.push(index);
                }
                indexes.reverse();
            },
            Some(mut i) => {
                //从指定只读日志文件开始，加载只读日志文件
                if i >= len {
                    //当序号大于等于只读日志文件数量，则加载所有只读日志文件
                    for index in 0..len {
                        indexes.push(index);
                    }
                    indexes.reverse();
                } else {
                    //否则只加载指定的只读日志文件
                    for index in 0..(i + 1) {
                        indexes.push(index);
                    }
                }
            },
        }

        for index in indexes {
            loop {
                match load_block(self,
                                 cache,
                                 Some(index),
                                 offset,
                                 is_checksum).await {
                    Err(e) => {
                        //加载指定日志文件的指定二进制块失败，则立即返回错误
                        return Err(e);
                    },
                    Ok(None) => {
                        //加载指定日志文件完成，则重置日志文件位置，并继续读下一个日志文件
                        offset = None;
                        break;
                    },
                    Ok(Some(next)) => {
                        //加载指定日志文件的指定日志块完成，则更新日志文件位置，并继续读当前只读日志文件的下一个日志块
                        offset = Some(next);
                    },
                }
            }
        }

        Ok(())
    }

    //从指定日志文件的指定位置开始，读取二进制块，返回读取的字节数和日志，返回字节数为0表示指定日志文件的指定位置没有二进制块
    pub async fn read_block(&self,
                            log_index: Option<usize>,
                            offset: Option<u64>,
                            is_checksum: bool) -> Result<(u64, LinkedList<(LogMethod, Vec<u8>, Option<Vec<u8>>)>)> {
        let (file_path, file) = if let Some(log_index) = log_index {
            //读取指定的只读日志文件
            unsafe {
                let readable = Box::from_raw(self.0.readable.load(Ordering::Relaxed));
                let r = (&*readable).get(log_index).unwrap().clone();
                Box::into_raw(readable); //避免被回收
                r
            }
        } else {
            //读取当前可写日志文件
            unsafe {
                let writable = Box::from_raw(self.0.writable.load(Ordering::Relaxed));
                let r = (&*writable).as_ref().unwrap().clone();
                Box::into_raw(writable); //避免被回收
                r
            }
        };

        let top = file.get_size();
        let mut next_offset = 0;
        let mut result = LinkedList::new();

        if let Some(header_offset) = offset {
            //从指定位置开始读
            match read_block_header(&file_path, &file, header_offset).await {
                Err(e) => {
                    //读块头失败，则立即返回错误
                    return Err(Error::new(ErrorKind::Other, format!("Read log block failed, path: {:?}, offset: {:?}, reason: {:?}", file_path, offset, e)));
                },
                Ok((payload_offset, payload_time, payload_checksum, payload_len)) => {
                    //读块头成功
                    if let Err(e) = read_block_payload(&mut result,
                                                       &file_path,
                                                       &file,
                                                       payload_offset,
                                                       payload_time,
                                                       payload_checksum,
                                                       payload_len,
                                                       is_checksum).await {
                        //校验块负载失败，则立即返回错误
                        return Err(Error::new(ErrorKind::Other, format!("Valid failed for read log block, path: {:?}, offset: {:?}, reason: {:?}", file_path, offset, e)));
                    }

                    //设置下一个块头的偏移
                    match payload_offset.checked_sub(DEFAULT_LOG_BLOCK_HEADER_LEN as u64) {
                        None => next_offset = 0,
                        Some(off) => next_offset = off,
                    }
                },
            }
        } else {
            //从日志文件尾开始读
            if top > 0 {
                //当前日志文件有日志
                let header_offset = top - DEFAULT_LOG_BLOCK_HEADER_LEN as u64; //获取日志文件的尾块头位置
                match read_block_header(&file_path, &file, header_offset).await {
                    Err(e) => {
                        //读块头失败，则立即返回错误
                        return Err(Error::new(ErrorKind::Other, format!("Read log tail block failed, path: {:?}, offset: {:?}, reason: {:?}", file_path, offset, e)));
                    },
                    Ok((payload_offset, payload_time, payload_checksum, payload_len)) => {
                        //读尾块头成功
                        if let Err(e) = read_block_payload(&mut result,
                                                           &file_path,
                                                           &file,
                                                           payload_offset,
                                                           payload_time,
                                                           payload_checksum,
                                                           payload_len,
                                                           is_checksum).await {
                            //校验尾块负载失败，则立即返回错误
                            return Err(Error::new(ErrorKind::Other, format!("Valid failed for read log tail block, path: {:?}, offset: {:?}, reason: {:?}", file_path, offset, e)));
                        }

                        //设置下一个块头的偏移
                        match payload_offset.checked_sub(DEFAULT_LOG_BLOCK_HEADER_LEN as u64) {
                            None => next_offset = 0,
                            Some(off) => next_offset = off,
                        }
                    },
                }
            }
        }

        Ok((next_offset, result))
    }

    //提交当前日志块，返回提交是否成功
    pub async fn commit(&self,
                        mut log_uid: usize,
                        is_forcibly: bool,
                        is_split: bool) -> Result<()> {
        let mut commit_block = None;
        let mut mutex = self.0.commit_lock.lock().await; //获取提交锁

		println!("commit 0000000, is_forcibly = {:?}, log_uid = {:?}, committed_uid = {:?}",is_forcibly, log_uid, self.0.commited_uid.load(Ordering::Relaxed));

        if log_uid as usize <= self.0.commited_uid.load(Ordering::Relaxed) {
			println!("commit 111111");
            //指定的日志已提交，则立即返回提交成功，也不需要唤醒任何的等待提交完成的任务
            return Ok(());
        }

        //日志块锁临界区
        {
            let mut lock = self.0.current.lock();
            if is_forcibly {
                //强制提交当前日志块，则交换出当前日志块
                commit_block = (&mut *lock).0.take();
				(&mut *lock).0 = Some(LogBlock::new(self.0.current_limit));
				println!("commit 22222");
				
            } else {
                //检查当前日志块是否已达大小限制
                if !(&mut *lock).0.as_ref().unwrap().is_size_limit(self.0.current_limit) {
                    //当前日志块未达同步大小限制，则等待日志块提交完成
                    let r = AsyncValue::new(AsyncRuntime::Multi(self.0.rt.clone()));
                    (&mut *mutex).push_back(r.clone());
                    drop(lock); //释放日志块锁
					drop(mutex); //释放提交锁
					println!("commit 333333");
                    return r.await;
                } else {
                    //当前日志块已达提交大小限制，则交换出当前日志块
                    commit_block = (&mut *lock).0.take();
					(&mut *lock).0 = Some(LogBlock::new(self.0.current_limit));
					println!("commit 444444");
                }
            }

            //如果有延迟提交，则设置为无延迟提交
            log_uid = (*lock).1; //重置为当前提交时当前块的最大日志id
            self.0.delay_commit.store(false, Ordering::Relaxed);
        }

        if let Some(block) = commit_block {
			println!("commit 555555555");

            //有需要提交的日志块
            if block.len() == 0 && !is_split {
				println!("commit 666666666");

                //没有需要提交的日志块且不需要强制分裂，则立即返回成功
                let waits = (&mut *mutex);
                for _ in 0..waits.len() {
                    //唤醒所有等待提交完成的任务
                    if let Some(r) = waits.pop_front() {
                        r.set(Ok(()));
                    }
                }

                return Ok(());
			}
			
			let start = std::time::Instant::now();

            //写文件
            unsafe {
                let mut async_file = Box::from_raw(self.0.writable.load(Ordering::Relaxed));
                match (*async_file).as_mut().unwrap().1.write(0,
                                                              Arc::from(Vec::from(block)),
                                                              WriteOptions::Sync(true)).await {
                    Err(e) => {
                        //同步日志块失败，则立即返回错误
                        let waits = (&mut *mutex);
                        for _ in 0..waits.len() {
                            //唤醒所有等待同步完成的任务
                            if let Some(r) = waits.pop_front() {
                                r.set(Err(Error::new(ErrorKind::Other, format!("Sync log failed, path: {:?}, reason: {:?}", self.0.path, e))));
                            }
                        }

                        Box::into_raw(async_file); //避免被释放
                        return Err(Error::new(ErrorKind::Other, format!("Sync log failed, path: {:?}, reason: {:?}", self.0.path, e)));
                    },
                    Ok(len) => {
						println!("commit 777777777, write file time = {:?}, write len = {:?}", start.elapsed(), len);

                        //提交日志块成功
                        let waits = (&mut *mutex);
                        for _ in 0..waits.len() {
                            //唤醒所有等待提交完成的任务
                            if let Some(r) = waits.pop_front() {
                                r.set(Ok(()));
                            }
                        }

                        if !self.0.mutex_status.compare_and_swap(false, true, Ordering::SeqCst) {
							println!("commit 88888888");
                            //当前没有整理，则检查是否需要创建新的可写日志文件
                            if (self.0.writable_len.fetch_add(len, Ordering::Relaxed) + len >= self.0.size_limit) || is_split {
                                //当前可写日志文件已达限制或需要强制分裂，则立即创建新的可写日志文件
                                match append_writable(self.0.rt.clone(),
                                                      self.0.path.clone(),
                                                      self.0.log_id.fetch_add(1, Ordering::Relaxed)).await {
                                    Err(e) => {
                                        //追加新的可写日志文件失败，则立即返回错误
                                        Box::into_raw(async_file); //避免释放可写文件
                                        self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                                        return Err(Error::new(ErrorKind::Other, format!("Append log file failed, path: {:?}, reason: {:?}", self.0.path, e)));
                                    },
                                    Ok((new_writable_path, new_writable)) => {
                                        //追加新的可写日志文件成功
                                        unsafe {
                                            if let Some((last_writable_path, last_writable)) = (*self.0.writable.load(Ordering::Relaxed)).take() {
                                                //将当前可写日志文件追加到只读日志文件列表中
                                                (&mut *self.0.readable.load(Ordering::Relaxed))
                                                    .push((last_writable_path, last_writable));
                                            }

                                            *self.0.writable.load(Ordering::Relaxed) = Some((new_writable_path, new_writable)); //替换当前可写日志文件
                                            self.0.writable_len.store(0, Ordering::Relaxed); //重置当前可写日志文件大小
                                        }
                                        self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                                    },
                                }
                            } else {
                                //当前可写日志文件未达限制且不需要强制分裂
                                self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                            }
						}
						println!("commit 999999");

						
                        Box::into_raw(async_file); //避免释放可写文件
                    },
                }
            }
        }

        self.0.commited_uid.store(log_uid, Ordering::Relaxed); //更新已提交完成的日志id
        Ok(())
    }

    //延迟提交，返回延迟提交是否成功
    pub async fn delay_commit(&self,
                              log_uid: usize,
                              is_split: bool,
                              timeout: usize) -> Result<()> {
        if self.0.delay_commit.compare_and_swap(false, true, Ordering::SeqCst) {
            //已经有延迟提交，则立即返回失败
            return self.commit(log_uid, false, is_split).await
        }

        let rt = self.0.rt.clone();
        let log = self.clone();
        self.0.rt.spawn(self.0.rt.alloc(), async move {
            rt.wait_timeout(timeout).await; //延迟指定时间
            log.0.delay_commit.store(false, Ordering::Relaxed); //如果有延迟提交，则设置为无延迟提交
            if let Err(e) = log.commit(log_uid, true, is_split).await {
                error!("Delay commit failed, log_uid: {:?}, timeout: {:?}, reason: {:?}", log_uid, timeout, e);
            }
        });
        return self.commit(log_uid, false, is_split).await
    }

    //立即分裂当前的日志文件
    pub async fn split(&self) -> Result<usize> {
        let mut mutex = self.0.commit_lock.lock().await; //获取提交锁

        if !self.0.mutex_status.compare_and_swap(false, true, Ordering::SeqCst) {
            //当前没有整理，则创建新的可写日志文件
            let new_log_index = self.0.log_id.fetch_add(1, Ordering::Relaxed);
            match append_writable(self.0.rt.clone(),
                                  self.0.path.clone(),
                                  new_log_index).await {
                Err(e) => {
                    //追加新的可写日志文件失败，则立即返回错误
                    self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                    Err(Error::new(ErrorKind::Other, format!("Split log file failed, path: {:?}, reason: {:?}", self.0.path, e)))
                },
                Ok((new_writable_path, new_writable)) => {
                    //追加新的可写日志文件成功
                    unsafe {
                        if let Some((last_writable_path, last_writable)) = (*self.0.writable.load(Ordering::Relaxed)).take() {
                            //将当前可写日志文件追加到只读日志文件列表中
                            (&mut *self.0.readable.load(Ordering::Relaxed))
                                .push((last_writable_path, last_writable));
                        }

                        *self.0.writable.load(Ordering::Relaxed) = Some((new_writable_path, new_writable)); //替换当前可写日志文件
                        self.0.writable_len.store(0, Ordering::Relaxed); //重置当前可写日志文件大小
                    }
                    self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁

                    Ok(new_log_index)
                },
            }
        } else {
            //当前有整理与分裂冲突，则立即返回错误
            Err(Error::new(ErrorKind::WouldBlock, format!("Split log file failed, path: {:?}, reason: collect conflict", self.0.path)))
        }
    }

    //整理只读日志文件，成功后返回整理文件的大小和日志数量
    pub async fn collect(&self, buf_len: usize, is_hidden_remove: bool) -> Result<(usize, usize)> {
        if self.0.mutex_status.compare_and_swap(false, true, Ordering::SeqCst) {
            //当前正在整理中，则立即返回错误
            return Err(Error::new(ErrorKind::Other, "Collect log failed, reason: collectting"));
        }

        //清理整理前的日志文件目录
        if let Err(e) = clean(&self.0.rt.clone(), self.0.path.clone()).await {
            //清理整理后的日志文件目录失败，则立即返回错误
            self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
            return Err(Error::new(ErrorKind::Other, format!("Clean log dir failed, path: {:?}, reason: {:?}", self.0.path, e)));
        }

        //开始只读日志文件的整理，用于清理临时整理日志文件
        let readable_box = unsafe { Box::from_raw(self.0.readable.load(Ordering::Relaxed)) };
        let readable_len = unsafe { (&*readable_box).len() };
        if readable_len == 0 {
            //没有需要整理的只读日志文件，则立即返回成功
            self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
            return Ok((0, 0));
        }

        //获取最后的只读日志文件路径
        let (last_readable_path, _) = unsafe { &(&*readable_box)[readable_len - 1] };
        let last_readable_path = last_readable_path.clone();
        Box::into_raw(readable_box); //避免被回收

        match create_tmp_log(self.0.rt.clone(), last_readable_path).await {
            Err(e) => {
                self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                Err(e)
            },
            Ok((tmp_path, tmp_file, collected_path)) => {
                let mut indexes = Vec::new();
                for index in 0..readable_len {
                    indexes.push(index);
                }
                indexes.reverse();

                //加载并合并所有只读日志文件
                let mut total_size = 0;
                let mut total_len = 0;
                let mut offset = None;
                let mut map = XHashMap::default();
                let mut hasher = Hasher::new();
                let mut buf = Vec::with_capacity(buf_len + DEFAULT_MERGE_LOG_BUF_SIZE);
                for index in indexes {
                    loop {
                        if (&buf).len() >= buf_len {
                            //合并缓冲区已满，则将缓冲区写入临时整理日志文件
                            match tmp_file.write(0, Arc::from(buf), WriteOptions::None).await {
                                Err(e) => {
                                    self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                                    return Err(Error::new(ErrorKind::Other, format!("Write tmp log block failed, path: {:?}, reason: {:?}", tmp_path.to_path_buf(), e)));
                                },
                                Ok(size) => {
                                    //写入临时整理日志文件成功，则统计写入的字节数，并重置缓冲区
                                    total_size += size;
                                    buf = Vec::with_capacity(buf_len + DEFAULT_MERGE_LOG_BUF_SIZE);
                                }
                            }
                        }

                        match merge_block(self,
                                          &mut map,
                                          &mut buf,
                                          &mut hasher,
                                          &mut total_len,
                                          Some(index),
                                          offset,
                                          true,
                                          is_hidden_remove).await {
                            Err(e) => {
                                //合并指定日志文件的指定二进制块失败，则立即返回错误
                                return Err(e);
                            },
                            Ok(None) => {
                                //合并指定日志文件完成，重置日志文件位置，并继续合并下一个日志文件
                                offset = None;
                                break;
                            },
                            Ok(Some(next)) => {
                                //合并指定日志文件的日志块完成，更新日志文件位置，并继续合并当前只读日志文件的下一个日志块
                                offset = Some(next);
                            },
                        }
                    }
                }

                if buf.len() > 0 {
                    //将缓冲区中剩余日志写入临时整理文件
                    match tmp_file.write(0, Arc::from(buf), WriteOptions::None).await {
                        Err(e) => {
                            self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                            return Err(Error::new(ErrorKind::Other, format!("Write tmp log block failed, path: {:?}, reason: {:?}", tmp_path.to_path_buf(), e)));
                        },
                        Ok(size) => {
                            //写入临时整理日志文件成功，则统计写入的字节数，并重置缓冲区
                            total_size += size;
                        }
                    }
                }

                //将合并后的日志块的头写入临时整理文件
                let mut header = Vec::with_capacity(DEFAULT_LOG_BLOCK_HEADER_LEN);
                write_header(&mut header, hasher, total_size);
                match tmp_file.write(0, Arc::from(header), WriteOptions::Sync(true)).await {
                    Err(e) => {
                        self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                        return Err(Error::new(ErrorKind::Other, format!("Write tmp log header failed, path: {:?}, reason: {:?}", tmp_path.to_path_buf(), e)));
                    },
                    Ok(size) => {
                        //写入临时整理日志文件头成功
                        if let Err(e) = rename(self.0.rt.clone(), tmp_path.clone(), collected_path.clone()).await {
                            self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                            return Err(Error::new(ErrorKind::Other, format!("Rename tmp log failed, from: {:?}, to: {:?}, reason: {:?}", tmp_path, collected_path, e)));
                        }

                        //清理整理后的日志文件目录，将被整理的日志文件改名为备份日志文件
                        if let Err(e) = clean(&self.0.rt.clone(), self.0.path.clone()).await {
                            //清理整理后的日志文件目录失败，则立即返回错误
                            self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                            return Err(Error::new(ErrorKind::Other, format!("Clean log dir failed, path: {:?}, reason: {:?}", self.0.path, e)));
                        }

                        //整理成功
                        self.0.mutex_status.store(false, Ordering::Relaxed); //解除互斥操作锁
                        Ok((total_size + size, total_len))
                    }
                }
            },
        }
    }
}

//按顺序打开所有的日志文件，根据需要将所有被整理的日志文件更新为备份文件，并移除所有临时整理日志文件
async fn open_logs<P: AsRef<Path> + Debug>(rt: &MultiTaskRuntime<()>,
                                           path: P) -> Result<(Vec<(PathBuf, AsyncFile<()>)>, Option<String>)> {
    match clean(rt, path).await {
        Err(e) => Err(e),
        Ok(log_paths) => {
            //清理日志文件目录成功，则打开所有有效的日志文件
            let mut log_files = Vec::with_capacity(log_paths.len());
            let mut last_log_name = None;
            for log_path in log_paths {
                if let Some(log_name) = log_path.file_name() {
                    if let Some(log_name_str) = log_name.to_str() {
                        //记录当前日志文件的文件名
                        last_log_name = Some(log_name_str.to_string());
                    }
                }

                match AsyncFile::open(rt.clone(), log_path.clone(), AsyncFileOptions::ReadAppend).await {
                    Err(e) => {
                        //打开日志文件失败，则立即返回错误
                        return Err(Error::new(ErrorKind::Other, format!("Open log failed, path: {:?}, reason: invalid file", &log_path)));
                    },
                    Ok(log) => {
                        //打开日志文件成功
                        log_files.push((log_path, log));
                    },
                }
            }

            Ok((log_files, last_log_name))
        },
    }
}

//清理指定日志文件目录，返回有效日志文件路径列表
async fn clean<P: AsRef<Path>>(rt: &MultiTaskRuntime<()>, path: P) -> Result<Vec<PathBuf>> {
    match read_dir(&path) {
        Err(e) => {
            //分析目录失败，则立即返回错误
            Err(Error::new(ErrorKind::Other, format!("Clean log dir failed, path: {:?}, reason: {:?}", path.as_ref(), e)))
        },
        Ok(dir) => {
            //读日志目录成功，则首先过滤目录中的备份日志文件和非日志文件
            let mut log_paths = Vec::new();
            for r in dir {
                if let Ok(entry) = r {
                    let file = entry.path();
                    if file.is_dir() {
                        //忽略目录
                        continue;
                    }

                    if let Some(ext_name) = file.extension() {
                        //有扩展名
                        match ext_name.to_str() {
                            Some(DEFAULT_BAK_LOG_FILE_EXT) => {
                                //忽略备份的日志文件
                                continue;
                            },
                            Some(ext_name_str) => {
                                if let Err(_) = ext_name_str.parse::<usize>() {
                                    //移除扩展名非备份或非整数的所有文件，并继续打开后续的文件
                                    if let Err(e) = remove_file(rt.clone(), file.clone()).await {
                                        //移除扩展名非备份或非整数的文件失败，则立即返回错误
                                        return Err(Error::new(ErrorKind::Other, format!("Remove non log file failed, path: {:?}, reason: {:?}", file, e)));
                                    }
                                    continue;
                                }
                                //扩展名为整数的文件，则继续
                            },
                            _ => (), //没有扩展名的文件，则继续
                        }
                    }

                    match entry.metadata() {
                        Err(e) => {
                            //无法获取文件元信息，则立即返回错误
                            return Err(Error::new(ErrorKind::Other, format!("Clean log file failed, path: {:?}, reason: {:?}", &file, e)));
                        },
                        Ok(meta) => {
                            if meta.is_dir() {
                                //忽略目录
                                continue;
                            }
                        },
                    }

                    //记录日志文件名
                    log_paths.push(file);
                } else {
                    //读取日志文件目录失败，则立即返回错误
                    return Err(Error::new(ErrorKind::Other, format!("Open log dir failed, path: {:?}, reason: invalid file", path.as_ref())));
                }
            }

            //打开日志文件
            let mut result = Vec::with_capacity(log_paths.len());
            //将所有被整理的日志文件改名为备份日志文件，并记录非备份日志文件
            if let Some(log_path) = log_paths.pop() {
                //记录并打开最后一个日志文件
                result.push(log_path);
            }

            let mut exist_first_collected_log_file = false; //是否存在首个整理后的日志文件
            for _ in 0..log_paths.len() {
                if let Some(log_path) = log_paths.pop() {
                    if exist_first_collected_log_file {
                        //存在首个整理后的日志文件，则当前日志文件可以改名为备份日志文件
                        let mut bak_log_path = log_path.clone();
                        if let Some(bak_log_ext) = bak_log_path.clone().extension() {
                            //当前日志文件有扩展名
                            bak_log_path.set_extension(bak_log_ext.to_string_lossy().as_ref().to_string() + "." + DEFAULT_BAK_LOG_FILE_EXT);
                        } else {
                            //当前日志文件无扩展名
                            bak_log_path.set_extension(DEFAULT_BAK_LOG_FILE_EXT);
                        }

                        match rename(rt.clone(), log_path.clone(), bak_log_path.clone()).await {
                            Err(e) => {
                                //改名为备份日志文件失败，则立即返回错误
                                return Err(Error::new(ErrorKind::Other, format!("Rename log to bak failed, from: {:?}, to: {:?}, reason: invalid file", &log_path, &bak_log_path)));
                            },
                            Ok(_) => {
                                //改名为备份日志文件成功，则忽略备份日志文件，并继续尝试打开下一个日志文件
                                continue;
                            },
                        }
                    } else {
                        //不存在首个整理后的日志文件
                        if log_path.extension().is_none() {
                            //当前日志文件，没有扩展名，则继续尝试清理下一个日志文件
                            result.push(log_path);
                            continue;
                        } else {
                            //当前日志文件，有扩展名，则记录存在首个整理后的日志文件，并尝试改名后续的日志文件为备份日志文件
                            exist_first_collected_log_file = true;
                            result.push(log_path);
                            continue;
                        }
                    }
                }
            }

            //反转已清理的日志文件路径列表，并返回
            result.reverse();
            Ok(result)
        },
    }
}

//根据文件名，判断是否是被整理的旧日志文件
#[inline]
fn is_old<P: AsRef<Path>>(path: P) -> bool {
    if let Some(ext) = path.as_ref().extension() {
        if ext.to_str().unwrap() == DEFAULT_BAK_LOG_FILE_EXT {
            return true;
        }
    }

    false
}

//根据文件名，判断是否是可整理日志文件
#[inline]
fn is_collectable<P: AsRef<Path>>(path: P) -> bool {
    if let Some(ext) = path.as_ref().extension() {
        if ext.to_str().unwrap() == DEFAULT_TMP_LOG_FILE_EXT {
            return true;
        }
    }

    false
}

//生成指定宽度的日志文件名
#[inline]
fn create_log_file_name(width: usize, id: usize) -> String {
    format!("{:0>width$}", id, width = width)
}

//将日志文件名转换为数字
#[inline]
fn log_file_name_to_usize(name: &str) -> Option<usize> {
    let vec: Vec<&str> = name.split('.').collect();
    if let Ok(n) = vec[0].parse() {
        return Some(n);
    }

    None
}

//根据文件路径获取日志文件的序号
fn get_log_index(log_path: Option<&PathBuf>,
                 readable: &Vec<(PathBuf, AsyncFile<()>)>) -> Option<usize> {
    if let Some(path) = log_path {
        //指定了文件路径
        let mut index = 0; //初始偏移
        for (p, _) in readable {
            if p.canonicalize().unwrap() == path.canonicalize().unwrap() {
                //标准化后，路径相同则立即返回偏移
                return Some(index);
            }

            //标准化后，路径不同，则继续
            index += 1;
        }
    }

    //没有指定文件路径或指定的文件路径不存在，则立即返回空
    None
}

//根据序号获取日志文件的路径
fn get_log_path(index: Option<usize>,
                writable: &Option<(PathBuf, AsyncFile<()>)>,
                readable: &Vec<(PathBuf, AsyncFile<()>)>) -> Option<PathBuf> {
    if let Some(index) = index {
        //返回只读日志文件的路径
        if let Some((readable_path, _)) = readable.get(index) {
            Some(readable_path.clone())
        } else {
            None
        }
    } else {
        //返回当前可写日志文件的路径
        if let Some((writable_path, _)) = writable {
            Some(writable_path.clone())
        } else {
            None
        }
    }
}

//加载指定日志文件的指定日志块，合并相同关键字的日志，成功返回下一个日志块头的偏移，返回空表示已加载到日志文件头
async fn load_block<C: PairLoader>(log_file: &LogFile,
                                   cache: &mut C,
                                   log_index: Option<usize>,
                                   offset: Option<u64>,
                                   is_checksum: bool) -> Result<Option<u64>> {
    match log_file.read_block(log_index, offset, is_checksum).await {
        Err(e) => {
            //读可写日志文件的指定二进制块失败，则立即返回错误
            Err(e)
        },
        Ok((next, logs)) => {
            //读日志文件的指定二进制块成功
            let log_index_file = unsafe { get_log_path(log_index,
                                                       &*log_file.0.writable.load(Ordering::Relaxed),
                                                       &*log_file.0.readable.load(Ordering::Relaxed)) };

            for (method, key, value) in logs {
                if cache.is_require(log_index_file.as_ref(), &key) {
                    //需要加入缓存
                    cache.load(log_index_file.as_ref(), method, key, value);
                }
            }

            if next == 0 {
                //已读到日志文件头，则立即返回
                Ok(None)
            } else {
                //更新日志文件位置，则返回当前日志文件的下一个日志块
                Ok(Some(next))
            }
        },
    }
}

//读二进制块头，返回块负载偏移、长度和校验码
async fn read_block_header<P: AsRef<Path>>(path: P,
                                           file: &AsyncFile<()>,
                                           header_offset: u64) -> Result<(u64, u64, u32, u32)> {
    match file.read(header_offset, DEFAULT_LOG_BLOCK_HEADER_LEN).await {
        Err(e) => {
            //读块头失败，则立即返回错误
            return Err(Error::new(ErrorKind::Other, format!("Read log block header failed, path: {:?}, reason: {:?}", path.as_ref(), e)));
        },
        Ok(bin) => {
            //读块头成功
            let mut header = Cursor::new(bin);
            let payload_time = header.get_u64_le(); //读取块同步时间
            let payload_checksum = header.get_u32_le(); //读取块校验码
            let payload_len = header.get_u32_le(); //读取块负载长度
            let payload_offset = header_offset - payload_len as u64;
            Ok((payload_offset, payload_time, payload_checksum, payload_len))
        },
    }
}

//读二进制块负载
async fn read_block_payload<P: AsRef<Path>>(list: &mut LinkedList<(LogMethod, Vec<u8>, Option<Vec<u8>>)>,
                                            path: P,
                                            file: &AsyncFile<()>,
                                            payload_offset: u64,
                                            payload_time: u64,
                                            payload_checksum: u32,
                                            payload_len: u32,
                                            is_checksum: bool) -> Result<()> {
    match file.read(payload_offset, payload_len as usize).await {
        Err(e) => {
            //读块负载失败，则立即返回错误
            return Err(Error::new(ErrorKind::Other, format!("Read log block payload failed, path: {:?}, offset: {:?}, len: {:?}, reason: {:?}", path.as_ref(), payload_offset, payload_len, e)));
        },
        Ok(bin) => {
            //读块负载成功，则解析
            let mut hasher = Hasher::new();
            let mut payload = Cursor::new(bin).to_bytes();
            hasher.update(payload.as_ref());
            hasher.update(&payload_time.to_le_bytes());

            while payload.len() > 0 {
                //解析日志方法和关键字
                let tag = LogMethod::with_tag(payload.get_u8());
                let key_len = payload.get_u16_le() as usize;
                let mut swap = payload.split_off(key_len);
                let key = payload.to_vec();
                payload = swap;

                //解析值
                if let LogMethod::Remove = tag {
                    //移除方法的日志，则忽略值解析，并继续解析下一个日志
                    list.push_back((tag, key, None));
                    continue;
                }
                let value_len = payload.get_u32_le() as usize;
                swap = payload.split_off(value_len);
                let value = payload.to_vec();
                payload = swap;
                list.push_back((tag, key, Some(value)));
            }

            if is_checksum {
                //需要校验块负载
                let hash = hasher.finalize();
                if payload_checksum != hash {
                    //校验尾块负载失败，则立即返回错误
                    return Err(Error::new(ErrorKind::Other, format!("Read log block payload failed, path: {:?}, offset: {:?}, len: {:?}, checksum: {:?}, real: {:?}, reason: Valid checksum error", path.as_ref(), payload_offset, payload_len, payload_checksum, hash)));
                }
            }
            Ok(())
        }
    }
}

//追加新的可写日志文件
async fn append_writable<P: AsRef<Path>>(rt: MultiTaskRuntime<()>, path: P, log_id: usize) -> Result<(PathBuf, AsyncFile<()>)> {
    let path_buf = path.as_ref().to_path_buf();
    let new_log = path_buf.join(create_log_file_name(DEFAULT_LOG_FILE_NAME_WIDTH, log_id));
    match AsyncFile::open(rt, new_log.clone(), AsyncFileOptions::ReadAppend).await {
        Err(e) => Err(e),
        Ok(file) => Ok((new_log, file)),
    }
}

//增加指定的临时整理日志文件，返回临时整理日志文件的路径、文件和整理完成后只读日志文件的扩展名
async fn create_tmp_log<P: AsRef<Path>>(rt: MultiTaskRuntime<()>, path: P) -> Result<(PathBuf, AsyncFile<()>, PathBuf)> {
    let mut tmp_log = path.as_ref().to_path_buf();
    if let Some(ext_name) = tmp_log.extension() {
        if let Some(ext_name_str) = ext_name.to_str() {
            if let Ok(current) = ext_name_str.parse::<usize>() {
                //临时整理日志文件的基础只读日志文件名，有扩展名且扩展名是整数，则替换扩展名
                let ok_log = tmp_log.clone();
                tmp_log = tmp_log.with_extension(DEFAULT_TMP_LOG_FILE_EXT);
                match AsyncFile::open(rt, tmp_log.clone(), AsyncFileOptions::OnlyAppend).await {
                    Err(e) => Err(e),
                    Ok(file) => Ok((tmp_log, file, ok_log.with_extension((current + 1).to_string()))),
                }
            } else {
                return Err(Error::new(ErrorKind::Other, format!("Create tmp log failed, last readable path: {:?}, reason: invalid last readable log extension", path.as_ref())));
            }
        } else {
            return Err(Error::new(ErrorKind::Other, format!("Create tmp log failed, last readable path: {:?}, reason: invalid last readable log", path.as_ref())));
        }
    } else {
        //临时整理日志文件的基础只读日志文件名没有扩展名，则增加扩展名
        let mut ok_log = tmp_log.clone();
        tmp_log.set_extension(DEFAULT_TMP_LOG_FILE_EXT);
        match AsyncFile::open(rt, tmp_log.clone(), AsyncFileOptions::OnlyAppend).await {
            Err(e) => Err(e),
            Ok(file) => {
                ok_log.set_extension(DEFAULT_COLLECTED_LOG_FILE_INIT_EXT.to_string());
                Ok((tmp_log, file, ok_log))
            },
        }
    }
}

//加载指定日志文件的指定日志块，合并相同关键字的日志，将合并后的日志写入缓冲区，并计算校验码，成功返回下一个日志块头的偏移，返回空表示已加载到日志文件头
async fn merge_block(log_file: &LogFile,
                     map: &mut XHashMap<Arc<Vec<u8>>, ()>,
                     buf: &mut Vec<u8>,
                     hasher: &mut Hasher,
                     total_len: &mut usize,
                     log_index: Option<usize>,
                     offset: Option<u64>,
                     is_checksum: bool,
                     is_hidden_remove: bool) -> Result<Option<u64>> {
    match log_file.read_block(log_index, offset, is_checksum).await {
        Err(e) => {
            //读可写日志文件的指定二进制块失败，则立即返回错误
            Err(e)
        },
        Ok((next, logs)) => {
            //读日志文件的指定二进制块成功
            for (method, key, value) in logs {
                let key = Arc::new(key);
                if let None =  map.get(&key) {
                    //没有指定关键字的日志，则记录并加入结果集
                    map.insert(key.clone(), ());
                    if let Some(value) = value {
                        //写方法的日志，则将日志写入缓冲区，并更新校验码
                        let off = buf.len();
                        write_buf(buf, method, key.as_slice(), value.as_slice());
                        hasher.update(&buf[off..]);
                        *total_len += 1;

                    } else {
                        //移除方法的日志
                        if is_hidden_remove {
                            //结果集中忽略移除方法的日志
                            continue;
                        }

                        //将日志写入缓冲区，并更新校验码
                        let off = buf.len();
                        write_buf(buf, method, key.as_slice(), &[]);
                        hasher.update(&buf[off..]);
                        *total_len += 1;
                    }
                }
            }

            if next == 0 {
                //已读到日志文件头，则立即返回
                Ok(None)
            } else {
                //更新日志文件位置，则返回当前日志文件的下一个日志块
                Ok(Some(next))
            }
        },
    }
}

/*
* 内部日志文件
*/
struct InnerLogFile {
    rt:                 MultiTaskRuntime<()>,                               //日志文件异步运行时
    path:               PathBuf,                                            //日志文件的本地路径
    size_limit:         usize,                                              //日志文件大小软限制，单位字节
    log_id:             AtomicUsize,                                        //本地可写日志唯一id
    writable_len:       AtomicUsize,                                        //本地可写日志文件大小
    writable:           AtomicPtr<Option<(PathBuf, AsyncFile<()>)>>,        //本地可写日志文件
    readable:           AtomicPtr<Vec<(PathBuf, AsyncFile<()>)>>,           //本地只读日志文件列表
    current_limit:      usize,                                              //日志文件的当前块大小软限制，单位字节
    current:            SpinLock<(Option<LogBlock>, usize)>,                //日志文件的当前块
    delay_commit:       AtomicBool,                                         //日志文件延迟提交状态
    commited_uid:       AtomicUsize,                                        //日志文件最近提交成功的最大日志id
    commit_lock:        Mutex<VecDeque<AsyncValue<(), Result<()>>>>,        //日志文件提交锁
    mutex_status:       AtomicBool,                                         //日志文件是否正在执行互斥操作，例如日志整理或创建新的可写日志文件是互斥操作
}

unsafe impl Send for InnerLogFile {}
unsafe impl Sync for InnerLogFile {}

