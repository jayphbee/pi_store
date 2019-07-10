/**
 * 日志存储的定义
 * 1、用2个以上读写日志做日志记录。2个以上的目的是为了保证有的日志记录晚到可以被记录。大小到了，要开辟新的读写日志。
 * 2、用时间起始值作为文件名，一般大小为4-64兆。文件内一般64k为1个块，默认采用lz4压缩。
 * 由外部驱动来建索引及重命名，这样可以用作索引完成的标志。
 * 加载时，先遍历目录，然后倒序加载日志文件，直到找到索引的日志文件。
 * 日志命名为： {time}.{mcount}  time为64位时间的base58，mcount为修改次数。 mcount为0表示还没建索引。1表示已建索引。每次读取日志文件时，只处理最大修改次数的，删除其余的。
 *
 * 日志需要建立索引。先修改头部的索引位置。然后创建索引。 // 一般总是用Guid来查询，所以不太需要有GuidBloom过滤器。
 * 为了自己管理缓冲，使用O_DIRECT 直接读写文件。元数据保留块索引数组。有块缓存，缓存单个的数据块。根据内存需要可单独加载和释放。缓存数据块时，可以按4k左右的大小建立简单索引。
 * 读写模式下： Ver(2字节), 配置(2字节), 整理时间-秒-总为0(4字节), 索引位置-总为0(4字节), 块索引数组长度-总为0(2字节), [{块长-块结束时写入(3字节), CRC32-块结束时写入(4字节), [{Guid(16字节), Bon长度(变长1-4字节), Bon格式数据，子表编号(2字节，可选，整理时删除)}...], }...]=数据块数组(块与块可能会出现Guid交叠)
 * 只读模式下： Ver(2字节), 配置-描述是否有子表编号(2字节), 整理时间-秒(4字节), 索引位置(4字节), 块索引数组长度(2字节), [块长(3字节), CRC32(4字节), {[{Guid(16字节), Bon长度(变长1-4字节), Bon格式数据，子表编号(4字节，可选，整理时删除)}...], }...]=数据块数组, 0xFFFFFF(3字节表示为索引), CRC32(4字节), [{MinGuidTime(8字节), MaxGuidTime(8字节), Pos(4字节), Count(4字节)}...]=块索引数组
 *
 * 管理器用sbtree记录每个文件名对应的元信息(时间和修改次数)
 *
 * 使用direct IO和pread来提升性能
 *
 */
use std::cmp::{Ord, Ordering as Order, PartialOrd};
use std::fs::{remove_file, rename, DirBuilder, File};
use std::io::{Read, Result as IoResult, Write};
use std::mem;
use std::path::{Path, PathBuf};
use std::result::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::vec::Vec;

use crc::{crc16, crc32, Hasher16, Hasher32};
use fnv::FnvHashMap;

use pi_lib::asbtree::{new, Tree};
use pi_lib::atom::Atom;
use pi_lib::base58::{FromBase58, ToBase58};
use pi_lib::bon::{ReadBuffer, WriteBuffer};
use pi_lib::data_view::{GetView, SetView};
use pi_lib::guid::Guid;
use pi_lib::ordmap::{ActionResult, Entry, OrdMap};
use pi_lib::time::{now_millis, now_nanos};

use pi_base::file::{AsynFileOptions, AsyncFile, Shared, SharedFile, WriteOptions};
use pi_base::pi_base_impl::cast_store_task;
use pi_base::task::TaskType;
use pi_base::util::now_millisecond;

const FILE_HEADE_SIZE: usize = 14;
const BLOCK_HEAD_SIZE: usize = 7;
const BLOCK_BODY_SIZE: usize = 64 * 1024 * 1024;
const LOG_HEAD_SIZE: usize = 24;

pub type Bin = Arc<Vec<u8>>;

pub type SResult<T> = Result<T, String>;
pub type LogResult = Option<SResult<()>>;

pub type Callback = Arc<Fn(SResult<()>)>;
pub type ReadCallback = Arc<Fn(SResult<Bin>)>;

/*
 * log日志
 */
#[derive(Clone)]
pub struct Log {
    dir: Atom,
    cfg: Config,
    stat: Statistics,
    rw: Arc<RwLock<LogRW>>,
}
impl Log {
    pub fn new(
        dir: Atom,
        cfg: Config,
        init_time: u64,
        cb: Arc<Fn(SResult<Self>)>,
    ) -> Option<SResult<Self>> {
        let path = Path::new(&**dir);
        if !path.exists() {
            //目录不存在，则创建目录
            DirBuilder::new().recursive(true).create(path).unwrap();
        } else if !path.is_dir() {
            return Some(Err("invalid log dir".to_string()));
        }
        let mut reads = OrdMap::new(new()); // TODO 优化成FnvHashMap
        let mut temp: Vec<(u64, PathBuf)> = Vec::new();
        // 分析目录下所有的日志文件，加载到读日志表和写日志列表中
        for entry in path.read_dir().expect("read_dir call failed") {
            if let Ok(entry) = entry {
                let file = entry.path();
                if file.is_file() {
                    let name = file.to_str().unwrap();
                    match name.find('.') {
                        Some(dot) => {
                            let file_name = Path::new(name);
                            let name_str = file_name.file_stem().unwrap().to_str().unwrap();
                            let modify_str = file_name.extension().unwrap().to_str().unwrap();
                            let (time, modify) = match u32::from_str_radix(modify_str, 16) {
                                Ok(r) => match name_str.from_base58() {
                                    Ok(vec_u8) => (vec_u8.as_slice().get_lu64(0), r),
                                    _ => {
                                        remove_file(file).expect("remove failed");
                                        continue;
                                    }
                                },
                                E => {
                                    remove_file(file).expect("remove failed");
                                    continue;
                                }
                            };
                            if modify == 0 {
                                //将首文件加入临时缓冲
                                temp.push((time, file));
                            } else {
                                //非首文件，根据时间进行排序，并加入读日志表中
                                let mut f = |v: Option<&(u32, AReader)>| match v {
                                    Some(info) => {
                                        if modify > info.0 {
                                            let mut pbuf = PathBuf::new();
                                            pbuf.push(path.clone());
                                            pbuf.push(Path::new(name_str));
                                            pbuf.push(Path::new(&info.0.to_string()));
                                            remove_file(pbuf.as_path()).expect("remove failed");
                                            ActionResult::Upsert((modify, AReader::new()))
                                        } else {
                                            remove_file(file.clone()).expect("remove failed");
                                            ActionResult::Ignore
                                        }
                                    }
                                    _ => ActionResult::Upsert((modify, AReader::new())),
                                };
                                reads.action(&time, &mut f);
                            }
                        }
                        _ => {
                            remove_file(file).expect("remove failed");
                        }
                    }
                }
            }
        }
        let len = temp.len();
        if len == 0 {
            //如果没有首文件，则初始化日志，并返回
            let log = Log {
                dir: dir.clone(),
                cfg: cfg,
                stat: Statistics::new(),
                rw: Arc::new(RwLock::new(LogRW::new(
                    &reads,
                    vec![FileInfo(init_time, 0, AWriter::new(Reader::new()))],
                ))),
            };
            let log_copy = log.clone();
            let init_rw_cb = Box::new(move |result: IoResult<AsyncFile>| {
                match result {
                    Err(e) => return cb(Err(e.to_string())),
                    Ok(f) => {
                        let vec = &mut log_copy.rw.write().unwrap().1;
                        let writer = &mut (vec[0].2).0.lock().unwrap();
                        writer.reader.file.file = Ok(Arc::new(f)); //初始化读写文件的共享文件
                        writer.reader.file.wait = None; //初始化读写文件成功
                    }
                }
                cb(Ok(log_copy));
            });
            create_rw_file(dir.clone(), init_time, 0, init_rw_cb);
            return Some(Ok(log));
        };
        // 加载所有正在写入的日志文件
        let count = Arc::new(AtomicUsize::new(len));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let c1 = cfg.clone();
        let bf = Arc::new(move |r: SResult<(usize, u64, AWriter)>| match r {
            Ok((i, time, w)) => {
                let mut vec = writes.lock().unwrap();
                vec.insert(i, FileInfo(time, 0, w));
                if count.fetch_sub(1, Ordering::SeqCst) == 1 {
                    cb(Ok(Log {
                        dir: dir.clone(),
                        cfg: cfg.clone(),
                        stat: Statistics::new(),
                        rw: Arc::new(RwLock::new(LogRW::new(
                            &reads,
                            mem::replace(&mut vec, Vec::new()),
                        ))),
                    }))
                }
            }
            Err(s) => cb(Err(s)),
        });
        let mut i = 0;
        for (time, file_path) in temp {
            println!(
                "!!!!!!!!!!!!!!!!!!!!!!load rw file, i: {}, time: {}, path: {:?}",
                i, time, file_path
            );
            load_write(i, time, c1.clone(), file_path.clone(), bf.clone());
            i += 1;
        }
        None
    }
    pub fn dir(&self) -> &Atom {
        &self.dir
    }
    pub fn cfg(&self) -> &Config {
        &self.cfg
    }
    pub fn stat(&self) -> &Statistics {
        &self.stat
    }
    // 读取指定Guid对应的数据
    pub fn read(&self, guid: Guid, callback: ReadCallback) -> Option<SResult<Bin>> {
        let time = guid.time();
        let r = {
            let wvec = &(self.rw.read().unwrap().1);
            let finfo = FileInfo(time, 0, AWriter::new(Reader::new())); // TODO 优化为裸指针创建AWriter
            match wvec[..].binary_search(&finfo) {
                Ok(i) => {}
                Err(i) => {
                    // 向前寻找

                }
            }
        };
        None
    }
    // 写入指定Guid对应的数据
    pub fn write(&self, guid: Guid, data: Bin, st_key: u32, callback: Callback) -> SResult<()> {
        if is_timeout_guid(&(self.rw.read().unwrap().1)[0], &guid) {
            return Err("guid too old".to_string());
        }

        let file_result = ((self.rw.write().unwrap().1).last().unwrap().2)
            .0
            .lock()
            .unwrap()
            .reader
            .file
            .file
            .clone(); //立即释放锁
        match file_result {
            Ok(ref f) => {
                //文件已初始化
                if is_out_of_file(f, self.cfg.limit_size, data.clone()) {
                    //需要增加新的读写文件
                    handle_create_rw_file(self.clone(), guid, data, st_key, callback)
                } else {
                    //在当前读写文件中继续写
                    sync_handle_write(
                        self.clone(),
                        (self.rw.write().unwrap().1).last().unwrap().2.clone(),
                        guid,
                        data,
                        st_key,
                        callback,
                    ) //立即释放锁
                }
            }
            Err(ref s) if s == &String::from("") => {
                //文件还未初始化，加入初始化队列，等待初始化成功后执行
                let log = self.clone();
                let cb = Box::new(move |result: SResult<()>| match result {
                    Err(s) => callback(Err("init rw file failed, ".to_string() + &s)),
                    Ok(_) => {
                        log.write(guid, data, st_key, callback);
                    }
                });
                ((self.rw.write().unwrap().1).last_mut().unwrap().2)
                    .0
                    .lock()
                    .unwrap()
                    .reader
                    .file
                    .wait
                    .as_mut()
                    .unwrap()
                    .push(cb); //立即释放锁
                Ok(())
            }
            Err(ref s) => {
                //文件初始化失败，立即返回错误
                Err(s.clone())
            }
        }
    }
    // 列出所有可以读写的日志文件名
    pub fn list_writes(&self) -> Vec<u64> {
        vec![]
    }
    // 整理指定的可读写的日志文件，为其建立索引并以只读方式打开
    pub fn collect(&self, file: u64, callback: Callback) -> SResult<()> {
        Err("file not found".to_string())
    }
}

// 统计
#[derive(Clone)]
pub struct Config {
    pub compress: usize,      // 0 无压缩， 1 lz4压缩， 2 lz4-hc高压缩
    pub limit_size: usize,    // 单个日志文件的限制大小，一般4-64兆 默认16M
    pub cache_size: usize,    // 缓冲大小，字节 默认1M
    pub cache_timeout: usize, // 最长缓冲时间，毫秒，基于最后读。默认30分钟
    pub collate_time: usize,  // 整理时间，秒。默认1天
}
impl Config {
    pub fn new() -> Self {
        Config {
            compress: 0,
            limit_size: 16 * 1024 * 1024,
            cache_size: 1 * 1024 * 1024,
            cache_timeout: 30 * 60 * 1000,
            collate_time: 24 * 60 * 60,
        }
    }
}
// 统计
#[derive(Clone)]
pub struct Statistics {
    pub read_count: Arc<AtomicUsize>,
    pub write_count: Arc<AtomicUsize>,
    pub read_time: Arc<AtomicUsize>,
    pub write_time: Arc<AtomicUsize>,
    pub cache_count: Arc<AtomicUsize>,
}
impl Statistics {
    fn new() -> Self {
        Statistics {
            read_count: Arc::new(AtomicUsize::new(0)),
            write_count: Arc::new(AtomicUsize::new(0)),
            read_time: Arc::new(AtomicUsize::new(0)),
            write_time: Arc::new(AtomicUsize::new(0)),
            cache_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

//================================ 内部结构和方法
/*
* 文件头大小
*/
const HEAD_SIZE: usize = 17;
/*
* 每条日志中是否含子表的配置
*/
const CFG_ST: u16 = 1;
/*
* 子表编号的大小
*/
const ST_SIZE: usize = 4;

/*
* 块的长度
*/
const BLOCK_LEN: usize = 3;

/*
* 最小的日志长度
*/
const MIN_LOG_SIZE: usize = 17;

/*
* 块内索引的统计长度
*/
const BLOCK_INDEX_STAT_SIZE: usize = 4096;

// 写加载函数
type WriteLoader = Arc<Fn(SResult<(usize, u64, AWriter)>)>;

#[derive(Clone)]
struct FileInfo<T: Clone>(u64, u32, T);

impl<T: Clone> Ord for FileInfo<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Order {
        if self.0 > other.0 {
            Order::Greater
        } else if self.0 < other.0 {
            Order::Less
        } else {
            Order::Equal
        }
    }
}
impl<T: Clone> PartialOrd for FileInfo<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Order> {
        Some(self.cmp(other))
    }
}
impl<T: Clone> Eq for FileInfo<T> {
    #[inline]
    fn assert_receiver_is_total_eq(&self) {}
}
impl<T: Clone> PartialEq for FileInfo<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

struct LogRW(Vec<FileInfo<AReader>>, Vec<FileInfo<AWriter>>);

impl LogRW {
    fn new(reader: &OrdMap<Tree<u64, (u32, AReader)>>, mut writer: Vec<FileInfo<AWriter>>) -> Self {
        let mut vec = Vec::with_capacity(reader.size());
        let mut f = |e: &Entry<u64, (u32, AReader)>| {
            vec.push(FileInfo(e.0, (e.1).0, (e.1).1.clone()));
        };
        // reader.select(None, false, &mut f);
        // 对reader和writer按时间排序
        vec.as_mut_slice().sort();
        writer.as_mut_slice().sort();
        LogRW(vec, writer)
    }
}

#[derive(Clone)]
struct AReader(Arc<Mutex<Reader>>);
impl AReader {
    fn new() -> Self {
        AReader(Arc::new(Mutex::new(Reader::new())))
    }
}
/*
 * 只读日志文件
 */
struct Reader {
    file: FileInit,
    ver: u16,
    config: u16,
    collect: u32, //整理时间
    info: IndexInfo,
    index: Vec<IndexInfo>,                      //块索引数组
    buffer: FnvHashMap<usize, (LogBlock, u64)>, // Pos为键， 值为日志块及超时时间
    waits: FnvHashMap<usize, Vec<Box<Fn(SResult<SharedFile>)>>>, // 单个块上的读等待队列
}
impl Reader {
    fn new() -> Self {
        Reader {
            file: FileInit {
                file: Err(String::from("")),
                wait: Some(Vec::new()),
            },
            ver: 0,
            config: 0,
            collect: 0,
            info: IndexInfo::new(),
            index: Vec::new(),
            buffer: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
            waits: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
        }
    }
    fn read_head(&mut self, data: &[u8]) -> usize {
        self.ver = data.get_lu16(0);
        self.config = data.get_lu16(2);
        self.collect = data.get_lu32(4);
        self.info.pos = data.get_lu32(8);
        println!(
            "!!!!!!read_head, ver: {}, config: {}, collect: {}, index: {:?}",
            self.ver, self.config, self.collect, self.info
        );
        data.get_lu16(12) as usize * mem::size_of::<IndexInfo>()
    }
}
/*
* 日志块
*/
#[derive(Debug)]
struct LogBlock {
    data: Vec<u8>,         // 解压后数据
    index: Vec<IndexInfo>, // 4k左右的大小建立简单索引
}
impl LogBlock {
    // 建立日志块，根据配置读取日志条目，创建简单索引，并且返回索引信息
    fn new(mut data: Vec<u8>, len: usize, c: &Config, cfg: u16) -> (Self, IndexInfo) {
        let mut info = IndexInfo::new();
        let mut index = Vec::new();
        let mut i = IndexInfo::new();
        let mut last_max = 1;
        let bf_size = if cfg & CFG_ST != 0 { ST_SIZE } else { 0 };
        let slice = &data[..];
        let len = data.len();
        let mut pos = 0;
        loop {
            if pos + MIN_LOG_SIZE + bf_size > len {
                unsafe { data.set_len(pos) };
                break;
            }
            let (pos1, time) = read_log(slice, pos, bf_size);
            if pos1 > len {
                unsafe { data.set_len(pos) };
                break;
            }
            // 更新当前索引的信息
            i.update(time);
            // 如果超过统计长度，则换新的统计
            if i.pos as usize + BLOCK_INDEX_STAT_SIZE > pos1 {
                // 如果当前的最小时间小于上一个的最大值，表示有次序颠倒，则需要进行排序
                if i.min < last_max {
                    last_max = 0;
                } else if last_max != 0 {
                    last_max = i.max
                }
                index.push(i);
                i = IndexInfo::new();
                i.pos = pos1 as u32;
            }
            // 更新本块的信息
            info.update(time);
            println!("!!!!!!new log block, index: {:?}", info);
            if pos1 == len {
                break;
            }
            pos = pos1;
        }
        // 对索引进行排序
        if last_max == 0 {
            index.as_mut_slice().sort();
        }
        (
            LogBlock {
                data: data,
                index: index,
            },
            info,
        )
    }

    //获取当前块大小
    pub fn len(&self) -> usize {
        self.data.len()
    }
}
/*
* 索引信息
*/
#[derive(Debug, Clone)]
struct IndexInfo {
    min: u64,
    max: u64,
    pos: u32,
    count: u32,
}
impl Ord for IndexInfo {
    #[inline]
    fn cmp(&self, other: &Self) -> Order {
        if self.min > other.min {
            Order::Greater
        } else if self.min < other.min {
            Order::Less
        } else if self.max > other.max {
            Order::Greater
        } else if self.max < other.max {
            Order::Less
        } else if self.pos > other.pos {
            Order::Greater
        } else if self.pos < other.pos {
            Order::Less
        } else {
            Order::Equal
        }
    }
}
impl PartialOrd for IndexInfo {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Order> {
        Some(self.cmp(other))
    }
}
impl Eq for IndexInfo {
    #[inline]
    fn assert_receiver_is_total_eq(&self) {}
}
impl PartialEq for IndexInfo {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.min == other.min
            && self.max == other.max
            && self.pos == other.pos
            && self.count == other.count
    }
}

impl IndexInfo {
    fn new() -> Self {
        IndexInfo {
            min: u64::max_value(),
            max: 0,
            pos: 0,
            count: 0,
        }
    }
    fn update(&mut self, time: u64) -> Order {
        self.count += 1;
        if self.max < time {
            self.max = time;
            Order::Greater
        } else if self.min > time {
            self.min = time;
            Order::Less
        } else {
            Order::Equal
        }
    }
}

#[derive(Clone)]
struct AWriter(Arc<Mutex<Writer>>);
impl AWriter {
    fn new(r: Reader) -> Self {
        AWriter(Arc::new(Mutex::new(Writer::new(r))))
    }
}
/*
 * 读写日志文件
 */
struct Writer {
    reader: Reader,
    block: LogBlock, // 当前正在写的日志块
    // window: LZ4的窗口和hash表
    info: IndexInfo,                  // 当前块索引
    waits: Option<Vec<Box<FnOnce()>>>, //当前块上的写等待队列，None表示当前没有写
}
impl Writer {
    fn new(r: Reader) -> Self {
        Writer {
            reader: r,
            block: LogBlock {
                data: Vec::with_capacity(BLOCK_BODY_SIZE),
                index: Vec::new(),
            },
            info: IndexInfo::new(),
            waits: None,
        }
    }
}
/*
 * 文件初始化
 */
struct FileInit {
    file: SResult<SharedFile>,
    wait: Option<Vec<Box<FnOnce(SResult<()>)>>>, // 为None表示file已经打开
}

//================================ 内部静态方法
// 加载正在写入的日志文件
fn load_write<P: AsRef<Path> + Send + 'static>(
    i: usize,
    time: u64,
    cfg: Config,
    path: P,
    cb: WriteLoader,
) {
    AsyncFile::open(
        path,
        AsynFileOptions::ReadWrite(8),
        Box::new(move |f: IoResult<AsyncFile>| match f {
            Ok(afile) => load_whead(afile, i, time, cfg, cb),
            Err(s) => cb(Err(s.to_string())),
        }),
    );
}

// 加载读写日志的头
fn load_whead(f: AsyncFile, i: usize, time: u64, cfg: Config, cb: WriteLoader) {
    let size = f.get_size();
    println!("!!!!!!load_whead, file size: {}", size);
    if size < HEAD_SIZE as u64 {
        return cb(Err("invalid file size".to_string()));
    }
    let file = Arc::new(f);
    file.pread(
        0,
        HEAD_SIZE,
        Box::new(move |f: SharedFile, r: IoResult<Vec<u8>>| match r {
            Ok(vec_u8) => {
                let data = &vec_u8[..];
                let mut reader = Reader::new();
                let index_size = reader.read_head(data);
                println!("!!!!!!load_whead, index size: {}", index_size);
                //如果索引位置加索引长度等于文件长度，则表示已建索引
                if reader.info.pos as u64 + index_size as u64 == size {
                    f.pread(
                        reader.info.pos as u64,
                        index_size,
                        Box::new(move |f: SharedFile, r: IoResult<Vec<u8>>| match r {
                            Ok(vec_u8) => {
                                reader.file.file = Ok(f);
                                reader.index = read_index_infos(vec_u8);
                                cb(Ok((i, time, AWriter::new(reader))))
                            }
                            Err(s) => cb(Err(s.to_string())),
                        }),
                    )
                } else {
                    load_windex(
                        f,
                        i,
                        time,
                        cfg,
                        Writer::new(reader),
                        size,
                        HEAD_SIZE,
                        read_3byte(data, HEAD_SIZE - BLOCK_LEN),
                        cb,
                    )
                }
            }
            Err(s) => cb(Err(s.to_string())),
        }),
    );
}
// 加载读写日志的索引
fn load_windex(
    file: SharedFile,
    i: usize,
    time: u64,
    cfg: Config,
    mut writer: Writer,
    size: u64,
    pos: usize,
    len: usize,
    cb: WriteLoader,
) {
    if len == 0 {
        // 读到最后一个块
        return load_block(file, i, time, writer, cfg, size, pos, cb);
    }
    if len == 0xFFFFFF {
        // 读到未写完的索引，丢弃索引
        writer.info.pos = (pos - BLOCK_LEN) as u32;
        writer.reader.file.file = Ok(file);
        writer.reader.index.as_mut_slice().sort();
        println!("!!!!!!load_windex, current block index: {:?}", writer.info);
        println!("!!!!!!load_windex, rw file: {:?}", writer.reader.file.file);
        return cb(Ok((i, time, AWriter(Arc::new(Mutex::new(writer))))));
    }
    if size < (pos + len) as u64 {
        return cb(Err("invalid windex".to_string()));
    }
    file.pread(
        pos as u64,
        len + BLOCK_LEN,
        Box::new(move |f: SharedFile, r: IoResult<Vec<u8>>| match r {
            Ok(vec_u8) => {
                let next_len = { read_index(vec_u8, &mut writer.reader, &cfg, pos, len) };
                load_windex(
                    f,
                    i,
                    time,
                    cfg,
                    writer,
                    size,
                    pos + len + BLOCK_LEN,
                    next_len,
                    cb,
                )
            }
            Err(s) => cb(Err(s.to_string())),
        }),
    );
}
// 加载日志的索引，返回下一个日志块的长度
fn read_index(vec_u8: Vec<u8>, reader: &mut Reader, cfg: &Config, pos: usize, len: usize) -> usize {
    let next_len = read_3byte(&vec_u8[..], len);
    let (b, info) = LogBlock::new(vec_u8, len, cfg, reader.config);
    println!("!!!!!!read_index, next len: {}", next_len);
    println!("!!!!!!read_index, log block: {:?}", b);
    reader.index.push(info);
    reader
        .buffer
        .insert(pos, (b, cfg.cache_timeout as u64 + now_millis()));
    next_len
}
// 加载读写日志的最后一个块
fn load_block(
    file: SharedFile,
    i: usize,
    time: u64,
    mut writer: Writer,
    cfg: Config,
    size: u64,
    pos: usize,
    cb: WriteLoader,
) {
    if size < pos as u64 {
        return cb(Err("invalid block".to_string()));
    }
    file.pread(
        pos as u64,
        (size - pos as u64) as usize,
        Box::new(move |f: SharedFile, r: IoResult<Vec<u8>>| match r {
            Ok(vec_u8) => {
                let len = vec_u8.len();
                let (b, info) = LogBlock::new(vec_u8, len, &cfg, writer.reader.config);
                writer.block = b;
                writer.info = info;
                writer.reader.file.file = Ok(f);
                writer.reader.index.as_mut_slice().sort();
                println!("!!!!!!load_block, len: {}", len);
                println!("!!!!!!load_block, data: {:?}", writer.block.data);
                println!("!!!!!!load_block, tmp index: {:?}", writer.block.index);
                println!("!!!!!!load_block, current block index: {:?}", writer.info);
                println!("!!!!!!load_block, rw file: {:?}", writer.reader.file.file);
                cb(Ok((i, time, AWriter(Arc::new(Mutex::new(writer))))))
            }
            Err(s) => cb(Err(s.to_string())),
        }),
    );
}
// TODO 加载读日志的索引
fn load_rindex(
    file: SharedFile,
    mut reader: Reader,
    index_size: usize,
    cb: Box<FnOnce(SResult<Reader>)>,
) {
    file.pread(
        reader.info.pos as u64,
        index_size,
        Box::new(move |f: SharedFile, r: IoResult<Vec<u8>>| match r {
            Ok(vec_u8) => {
                reader.file.file = Ok(f);
                reader.index = read_index_infos(vec_u8);
                cb(Ok(reader))
            }
            Err(s) => cb(Err(s.to_string())),
        }),
    );
}
// 读取单条日志
fn read_log(data: &[u8], pos: usize, st_size: usize) -> (usize, u64) {
    let time = Guid(data.get_lu128(pos)).time();
    println!("!!!!!!read_log, guid time: {}", time);
    let mut r = ReadBuffer::new(data, pos + 16);
    let bon_len = r.read_lengthen().expect("") as usize;
    (pos + 16 + r.head() + bon_len + st_size, time)
}
// 读取3字节的块长度
pub fn read_3byte(data: &[u8], pos: usize) -> usize {
    ((data.get_lu16(pos) as usize) << 8) + data.get_u8(pos + 2) as usize
}
// 读取索引
fn read_index_infos(vec_u8: Vec<u8>) -> Vec<IndexInfo> {
    let mut vec = unsafe {
        let len = vec_u8.len();
        let mut v = mem::transmute::<Vec<u8>, Vec<IndexInfo>>(vec_u8);
        v.set_len(len / mem::size_of::<IndexInfo>());
        v
    };
    if cfg!(target_endian = "big") {
        for e in vec.iter_mut() {
            e.min = e.min.swap_bytes();
            e.max = e.max.swap_bytes();
            e.count = e.count.swap_bytes();
            e.pos = e.pos.swap_bytes();
        }
    }
    vec
}

//判断guid是否过期
fn is_timeout_guid(info: &FileInfo<AWriter>, guid: &Guid) -> bool {
    let writer = (info.2).0.lock().unwrap();
    writer.reader.info.min <= guid.time()
}

//判断当前文件是否将超过配置限制
fn is_out_of_file(file: &SharedFile, limit: usize, data: Bin) -> bool {
    (file.get_size() + data.len() as u64 + LOG_HEAD_SIZE as u64) >= limit as u64
}

//判断当前块是否将超过限制
fn is_out_of_block(writer: &mut Writer, data: Bin) -> bool {
    (writer.block.len() + data.len() + LOG_HEAD_SIZE) >= BLOCK_BODY_SIZE
}

//同步处理写操作
fn sync_handle_write(
    log: Log,
    awriter: AWriter,
    guid: Guid,
    data: Bin,
    st_key: u32,
    callback: Callback,
) -> SResult<()> {
    let awriter_copy = awriter.clone();
    let callback_queue = Arc::new(move |result: SResult<()>| {
        //同步块
        {
            let mut waits = &mut awriter_copy.0.lock().unwrap().waits;
            match waits {
                None => (),
                Some(ref mut queue) => {
                    if queue.len() == 0 {
                        waits = &mut None; //队列写操作执行完成，声明当前没有写
                    } else {
                        let next_cb = queue.remove(0);
                        //调度队列中下一个写操作
                        cast_store_task(
                            TaskType::Sync,
                            100000000,
                            Box::new(move || {
                                next_cb();
                            }),
                            Atom::from("wait init rw file failed task"),
                        );
                    }
                }
            }
        }
        callback(result); //调用队列当前写操作的回调
    });

    let is_none = awriter.0.lock().unwrap().waits.is_none(); //立即释放互斥锁
    if is_none {
        //当前未写，则声明当前正在写, 并继续执行写操作
        awriter.0.lock().unwrap().waits = Some(Vec::new()); //声明当前正在写，并立即释放互斥锁
        handle_write(awriter.clone(), guid, data, st_key, callback_queue)
    } else {
        //当前正在写，将写操作放入队列
        let awriter_copy = awriter.clone();
        let cb = Box::new(move || {
            //执行队列当前写操作
            match handle_write(
                awriter_copy.clone(),
                guid,
                data,
                st_key,
                callback_queue.clone(),
            ) {
                Err(s) => callback_queue(Err("wait write log failed, ".to_string() + &s)),
                _ => (),
            }
        });
        awriter.0.lock().unwrap().waits.as_mut().unwrap().push(cb); //立即释放互斥锁
        Ok(())
    }
}

//处理写数据操作
fn handle_write(
    awriter: AWriter,
    guid: Guid,
    data: Bin,
    st_key: u32,
    callback: Callback,
) -> SResult<()> {
    //同步块
    {
        let mut writer = awriter.0.lock().unwrap();
        if is_out_of_block(&mut *writer, data.clone()) {
            //当前日志块已满
            let copy = writer.info.clone();
            writer.reader.index.push(copy); //将当前日志块写入块的读索引
                                            //更新文件的读索引
            if writer.reader.info.min == u64::max_value() && writer.reader.info.max == 0 {
                //当前文件首个块已满
                writer.reader.info.min = writer.info.min;
                writer.reader.info.pos = FILE_HEADE_SIZE as u32; //预留文件头的长度
            }
            writer.reader.info.max = writer.info.max;
            writer.reader.info.count += writer.info.count;
            return save_block_head(awriter.clone(), guid, data, st_key, callback);
        }
        if writer.reader.info.min == u64::max_value() && writer.reader.info.max == 0 {
            //当前文件首个块的首个日志
            writer.info.pos = FILE_HEADE_SIZE as u32; //预留文件头的长度
            writer.info.pos += BLOCK_HEAD_SIZE as u32 //预留块头的长度
        }
    }
    write_log(awriter.clone(), guid, data, st_key, callback)
}

//写当前日志
fn write_log(
    awriter: AWriter,
    guid: Guid,
    data: Bin,
    st_key: u32,
    callback: Callback,
) -> SResult<()> {
    let last: usize;

    //同步块
    {
        let mut writer = awriter.0.lock().unwrap();
        last = writer.block.data.len();
        //写guid
        match (&mut writer.block.data).write_all(&guid.0.to_le_bytes()[..]) {
            Err(e) => return Err(e.to_string()),
            _ => (),
        }
        //写data长度
        match match data.len() {
            len if len < 127 => (&mut writer.block.data).write_all(&(len as u8).to_le_bytes()[..]),
            len if len >= 127 && len < 65536 => {
                (&mut writer.block.data).write_all(&(len as u16).to_le_bytes()[..])
            }
            len => (&mut writer.block.data).write_all(&(len as u32).to_le_bytes()[..]),
        } {
            Err(e) => return Err(e.to_string()),
            _ => (),
        }
        //写data
        match (&mut writer.block.data).write_all(&(*data)[..]) {
            Err(e) => return Err(e.to_string()),
            _ => (),
        }
        if st_key != 0 {
            //写子表编号
            match (&mut writer.block.data).write_all(&st_key.to_le_bytes()[..]) {
                Err(e) => return Err(e.to_string()),
                _ => (),
            }
        }
    }

    write_tmp_index(awriter, last, guid, data, callback)
}

//写当前块临时索引
fn write_tmp_index(
    awriter: AWriter,
    last: usize,
    guid: Guid,
    data: Bin,
    callback: Callback,
) -> SResult<()> {
    let bin: Vec<u8>;
    let file_pos: u64;
    let max: u64;

    //同步块
    {
        let mut writer = awriter.0.lock().unwrap();
        max = guid.time();
        let len = writer.block.data.len();
        file_pos = writer.info.pos as u64 + last as u64;
        if writer.block.index.len() == 0 {
            //当前首个临时索引
            let mut index = IndexInfo::new();
            index.min = max;
            index.pos = file_pos as u32;
            writer.block.index.push(index);
        }
        let mut last_tmp_index = writer.block.index.last_mut().unwrap(); //获取当前最新的临时索引
        if (len - last_tmp_index.pos as usize + LOG_HEAD_SIZE) >= BLOCK_INDEX_STAT_SIZE {
            //当前临时索引已满，创建新的临时索引
            let mut index = IndexInfo::new();
            index.min = max;
            index.pos = file_pos as u32;
            writer.block.index.push(index);
            last_tmp_index = writer.block.index.last_mut().unwrap();
        }
        last_tmp_index.max = max;
        last_tmp_index.count += 1;

        bin = (&writer.block.data[last..len]).to_vec();
    }
    println!("!!!!!!file pos: {}, len: {}", file_pos, bin.len());
    save_log(awriter, file_pos, max, bin, callback)
}

//异步保存日志数据，并调用用户回调
fn save_log(
    awriter: AWriter,
    mut file_pos: u64,
    max: u64,
    mut bin: Vec<u8>,
    callback: Callback,
) -> SResult<()> {
    let mut writer = awriter.0.lock().unwrap(); //退出函数时释放互斥锁
    if writer.reader.info.min == u64::max_value()
        && writer.reader.info.max == 0
        && writer.info.min == u64::max_value()
        && writer.info.max == 0
    {
        //当前文件的首个块的首个日志，填充文件头数据
        let size = FILE_HEADE_SIZE + BLOCK_HEAD_SIZE;
        let mut head: Vec<u8> = Vec::with_capacity(size);
        head.resize(size, 0);
        head.append(&mut bin);
        file_pos = 0; //移动到文件头
        bin = head;
    }

    let awriter_copy = awriter.clone();
    let cb = Box::new(move |file: SharedFile, result: IoResult<usize>| {
        match result {
            Err(e) => callback(Err("write log failed, ".to_string() + &e.to_string())),
            Ok(len) => {
                //保存日志成功, 更新文件当前块索引和临时索引, 并执行用户回调
                {
                    let mut writer = awriter_copy.0.lock().unwrap(); //锁将在用户回调前释放
                                                                     //更新当前块的索引
                    if writer.info.min == u64::max_value() && writer.info.max == 0 {
                        //当前块的首个日志
                        writer.info.min = max;
                    }
                    writer.info.max = max;
                    writer.info.count += 1;
                }
                callback(Ok(()));
            }
        }
    });
    // println!("!!!!!!file_pos: {}, bin: {:?}", file_pos, bin);
    writer.reader.file.file.as_ref().unwrap().clone().pwrite(
        WriteOptions::Sync(true),
        file_pos,
        bin,
        cb,
    );
    Ok(())
}

//异步保存只读的块头数据，必须等待成功，再继续写
fn save_block_head(
    awriter: AWriter,
    guid: Guid,
    data: Bin,
    st_key: u32,
    callback: Callback,
) -> SResult<()> {
    let mut head: Vec<u8> = Vec::with_capacity(BLOCK_HEAD_SIZE);
    let writer = awriter.0.lock().unwrap();
    match head.write_all(&writer.block.data.len().to_le_bytes()[0..3]) {
        Err(e) => return Err(e.to_string()),
        _ => (),
    }
    match head.write_all(&crc32::checksum_ieee(&writer.block.data[..]).to_le_bytes()[..]) {
        Err(e) => return Err(e.to_string()),
        _ => (),
    }
    let file_pos = writer.reader.index.last().unwrap().pos - BLOCK_HEAD_SIZE as u32; //移动到块头, 并立即释放锁

    let awriter_copy = awriter.clone();
    let cb = Box::new(move |file: SharedFile, result: IoResult<usize>| {
        match result {
            Err(e) => callback(Err("write block head failed, ".to_string() + &e.to_string())),
            Ok(len) => {
                //保存块头成功
                let mut writer = awriter_copy.0.lock().unwrap(); //锁将在分支结束后释放
                                                                 //初始化新块的缓冲和临时索引
                writer.block.data.clear();
                writer.block.index.clear();
                //初始化新块的索引
                writer.info.min = u64::max_value();
                writer.info.max = 0;
                writer.info.pos += BLOCK_HEAD_SIZE as u32; //预留块头的长度
                writer.info.count = 0;
            }
        }

        //继续写
        match handle_write(awriter_copy, guid, data, st_key, callback.clone()) {
            Err(s) => callback(Err("write block head failed, ".to_string() + &s)),
            _ => (),
        }
    });
    writer.reader.file.file.as_ref().unwrap().clone().pwrite(
        WriteOptions::Sync(true),
        file_pos as u64,
        head,
        cb,
    );
    Ok(())
}

//处理打开新的读写文件
fn handle_create_rw_file(
    log: Log,
    guid: Guid,
    data: Bin,
    st_key: u32,
    callback: Callback,
) -> SResult<()> {
    let log_copy = log.clone();
    let open_time = now_nanos();

    //将当前写操作加入初始化队列中
    let mut reader = Reader::new();
    let cb0 = Box::new(move |result: SResult<()>| match result {
        Err(s) => callback(Err("init rw file failed, ".to_string() + &s)),
        Ok(_) => {
            log_copy.write(guid, data, st_key, callback);
        }
    });
    reader.file.wait.as_mut().unwrap().push(cb0);

    //增加新的读写文件
    let awriter = AWriter::new(reader);
    (&mut log.rw.write().unwrap().1).push(FileInfo(open_time, 0, awriter.clone())); //重复获得写锁，加入后立即释放

    //创建新的读写文件，并处理读写文件初始化队列中的用户回调
    let cb1 = Box::new(move |result: IoResult<AsyncFile>| {
        let mut writer = awriter.0.lock().unwrap(); //锁将在处理初始化等待队列后释放
        match result {
            Err(e) => {
                //创建新的读写文件失败
                match writer.reader.file.wait {
                    None => (),
                    Some(ref mut queue) => {
                        let mut cb: Box<FnOnce(SResult<()>)>;
                        let reason = &e.to_string();
                        let atom = Atom::from("init rw file failed, ".to_string() + &e.to_string());
                        let queue_len = queue.len();
                        for _ in 0..queue_len {
                            cb = queue.remove(0);
                            let reason = atom.clone();
                            cast_store_task(
                                TaskType::Sync,
                                100000000,
                                Box::new(move || {
                                    cb(Err((*reason).clone()));
                                }),
                                Atom::from("wait init rw file failed task"),
                            );
                        }
                    }
                }
            }
            Ok(f) => {
                writer.reader.file.file = Ok(Arc::new(f)); //初始化读写文件的共享文件
                match writer.reader.file.wait {
                    None => (),
                    Some(ref mut queue) => {
                        let mut cb: Box<FnOnce(SResult<()>)>;
                        let queue_len = queue.len();
                        for _ in 0..queue_len {
                            cb = queue.remove(0);
                            cast_store_task(
                                TaskType::Sync,
                                100000000,
                                Box::new(move || {
                                    cb(Ok(()));
                                }),
                                Atom::from("wait init rw file ok task"),
                            );
                        }
                    }
                }
                writer.reader.file.wait = None; //初始化读写文件成功
            }
        }
    });
    create_rw_file(log.dir, open_time, 0, cb1);
    Ok(())
}

//创建指定的读写文件
fn create_rw_file(dir: Atom, time: u64, count: usize, cb: Box<FnOnce(IoResult<AsyncFile>)>) {
    let mut path = PathBuf::new();
    path.push(&**dir);
    path.push((&time.to_le_bytes()[..]).to_base58());
    path.set_extension(count.to_string());
    AsyncFile::open(path, AsynFileOptions::ReadWrite(8), cb);
}
