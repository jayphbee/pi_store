/**
 * KG日志日志存储的定义
 * KG日志需要外部提供从Bon数据中获取key的方法和批量按键获取Guid的方法。并且写日志时需要指定子表编号。其余和一般日志一样。
 * KG日志的按键进行合并，部分垃圾回收的流程：
 * 寻找超过指定时间并且整理次数最少的块，如果块太小，会和前一个块合并到一起，一起进行整理。
 * 对块内日志进行遍历，每个条目反查key来决定是否存在。
 * 合并时，会创建一个临时文件，然后生成新的日志文件后，将临时文件命名成{time}.{mcount+1}，修改内存。最后删除原日志。
 * 
 */


use std::sync::{Arc};
use std::vec::Vec;


use pi_lib::atom::Atom;
use pi_lib::guid::Guid;
use pi_lib::data_view::{GetView, SetView};
use pi_lib::bon::{ReadBuffer, WriteBuffer};
use pi_lib::base58::{ToBase58, FromBase58};

use log::{Bin, Config, Log, SResult, Callback, ReadCallback};

/*
 * KVlog日志
 */
#[derive(Clone)]
pub struct KGLog {
	log: Log,
	key_fn: Arc<Fn(Bin) -> Bin>,
	read_fn: Arc<Fn(Vec<Bin>) -> Vec<Guid>>,
}
impl KGLog {
	pub fn new(dir: Atom, cfg: Config, cb: Arc<Fn(SResult<Self>)>) -> Option<SResult<Self>> {
		// match Log::new(dir, cfg, cb) {
		// 	Some(r) => Some(r),
		// 	_ => None
		// }
		None
	}
	pub fn log(&self) -> &Log {
		&self.log
	}
	pub fn read(&self, guid: Guid, cb: ReadCallback) -> Option<SResult<Bin>> {
		self.log.read(guid, cb)
	}
	pub fn write(&self, guid: Guid, data: Bin, st_key: u32, cb: Callback) -> SResult<()> {
		self.log.write(guid, data, st_key, cb)
	}
	// 列出所有可以读写的日志文件名
	pub fn list_writes(&self) -> Vec<u64> {
		self.log.list_writes()
	}
	// 列出所有可以读写的日志文件内的(guid: Guid, key: Bin, st_key: u16)
	pub fn list_writes_datas(&self) -> Vec<(u64, Vec<(Guid, Bin, u16)>)> {
		vec![]
	}
	// 整理指定的可读写的日志文件，为其建立索引并以只读方式打开
	pub fn collect(&self, file: u64, cb: Callback) -> SResult<()> {
		self.log.collect(file, cb)
	}
}

//====================================


//================================ 内部静态方法
