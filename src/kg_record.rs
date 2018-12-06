/**
 * 	1、将叶节点单独存放在一个文件中，按4k一个叶节点。数据格式：[Count(4Byte), {Key, Guid}...]。采用COW，每次修改都重新用一个新的叶节点。空块由子节点计算获得，内存中采用位索引方式记录。
 * 	2、将子节点单独存放在一个文件中，按4k一个子节点。数据格式：[Size(4Byte), Count(4Byte), {Key, 叶节点的位置Pos(4Byte)}...]。采用COW，每次修改都重新用一个新的子节点。空块由超级块计算获得，内存中采用位索引方式记录。所有子节点全部加载进内存。
 * 分裂和合并类似Cow的B+树的方式
 */
use std::boxed::FnBox;
use std::cmp::{Ord, Ordering as Order, PartialOrd};
use std::fs::{remove_file, rename, DirBuilder, File};
use std::io::Result as IoResult;
use std::mem;
use std::path::{Path, PathBuf};
use std::result::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::vec::Vec;

use fnv::FnvHashMap;
use fnv::FnvHashSet;

use pi_lib::asbtree::{new, Tree};
use pi_lib::atom::Atom;
use pi_lib::base58::{FromBase58, ToBase58};
use pi_lib::bon::{ReadBuffer, WriteBuffer};
use pi_lib::data_view::{GetView, SetView};
use pi_lib::guid::Guid;
use pi_lib::ordmap::{ActionResult, Entry, OrdMap};
use pi_lib::time::now_millis;

use pi_base::file::{AsynFileOptions, AsyncFile};

use kg::{BinPos, ReadGuidCallback, Tab};
use kg_log::KGLog;
use kg_root::RootLog;
use log::{Bin, Callback, Config as LogCfg, Log, LogResult, ReadCallback, SResult};

/*
 * 记录文件
 */
pub struct Record {
    node: NodeFile,
    pub roots: FnvHashMap<u32, Weak<Vec<u8>>>, // Pos为键， 值为子节点的弱引用
    leaf: NodeFile,
    cache: FnvHashMap<u32, (Bin, u64)>, // Pos为键， 值为叶节点及超时时间
    waits: FnvHashMap<usize, Vec<Box<Fn(SResult<AsyncFile>)>>>, // 单个块上的读等待队列
}
impl Record {
    // 初始化记录文件，根据加载起来
    pub fn new(node_file: AsyncFile, leaf_file: AsyncFile) -> Self {
        Record {
            node: NodeFile::new(node_file),
            roots: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
            leaf: NodeFile::new(leaf_file),
            cache: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
            waits: FnvHashMap::with_capacity_and_hasher(0, Default::default()),
        }
    }
    // 初始化方法，根据所有子表的子节点集合加载子节点，返回子节点的位置表
    pub fn init(
        record: Arc<RwLock<Record>>,
        nodes: Arc<FnvHashMap<u32, FnvHashSet<u32>>>,
        cb: Box<Fn(SResult<FnvHashMap<u32, Bin>>)>,
    ) {

    }
    // 获取子节点的空块索引数组
    pub fn node_empty(&self) -> Arc<RwLock<Vec<u64>>> {
        self.node.emptys.clone()
    }
    // 分配一个子节点
    pub fn malloc_node(&mut self) -> BinPos {
        BinPos::new(self.node.emptys.clone(), Arc::new(Vec::new()), 0)
    }
    // 读取指定位置的子节点
    pub fn read_node(&mut self, pos: u32) -> Option<BinPos> {
        None
    }
    // 写入多个子节点
    pub fn write_nodes(&self, items: Vec<BinPos>, cb: Callback) {}
    // 分配一个叶节点
    pub fn malloc_leaf(&self) -> BinPos {
        BinPos::new(self.node.emptys.clone(), Arc::new(Vec::new()), 0)
    }
    // 读取指定位置的叶节点
    pub fn read_leaf(&mut self, pos: u32, cb: ReadGuidCallback) -> Option<SResult<BinPos>> {
        None
    }
    // 写入多个叶节点
    pub fn write_leafs(&self, items: Vec<BinPos>, cb: Callback) {}
}

//====================================
/*
 * 子节点文件
 */
struct NodeFile {
    file: AsyncFile,
    file_size: u64,
    emptys: Arc<RwLock<Vec<u64>>>, //空块索引数组
}
impl NodeFile {
    fn new(file: AsyncFile) -> Self {
        let size = file.get_size();
        NodeFile {
            file: file,
            file_size: size,
            emptys: Arc::new(RwLock::new(Vec::new())),
        }
    }
}
//================================ 内部静态方法
fn free<T>(_: T) {}
