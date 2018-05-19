// 当且仅当测试套件运行时，才条件编译 `test` 模块。
#[cfg(test)]
extern crate pi_vm;
extern crate pi_store;

use std::thread;
use std::io::Result;
use std::path::PathBuf;
use std::time::Duration;
use std::fs::{File, OpenOptions};

use pi_vm::worker_pool::WorkerPool;
use pi_store::ordmap::Entry;
use pi_store::ordmap::OrdMap;
use pi_store::ordmap::ImOrdMap;
use pi_store::sbtree::Tree;
use pi_store::file::{STORE_TASK_POOL, AsyncFile, AsynFileOptions, WriteOptions};


// 需要一个辅助函数
fn show(t: &OrdMap<usize, usize, Tree<usize, usize>>) -> Vec<usize> {
	let mut v = Vec::new();
	{
		let mut f = |e:&Entry<usize, usize>| {v.push(e.key().clone()); v.push(e.value().clone())};
		t.select(None, &mut f);
	}
	v
}
// #[test]
fn sb_test() {
	let tree:Tree<usize, usize> = Tree::new();
	let mut t = OrdMap::new(tree);
	let mut tt = t.clone();
	assert!(t.is_empty());
	assert!(t.insert(1, 10));
	assert!(t.insert(2, 20));
	assert!(t.size() == 2);
	assert!(t.insert(3, 30));
	assert!(t.size() == 3);
	assert!(!(t.insert(3, 30)));
	assert!(!t.is_empty());
	assert!(t.has(&3));
	assert!(t.has(&2));
	assert!(t.has(&1));
	assert!(!t.has(&4));
	assert!(t.update(2, 21, false).is_some());
	assert!(t.update(1, 11, false).is_some());
	assert!(t.update(3, 31, false).is_some());
	assert!(!t.update(40, 40, true).is_some());
	assert!(t.size() == 3);
	assert!(t.insert(40, 40));
	assert!(t.size() == 4);
	assert!(t.get(&2) == Some(&21));
	assert!(t.get(&1) == Some(&11));
	assert!(t.get(&3) == Some(&31));
	assert!(t.get(&40) == Some(&40));
	assert!(t.get(&5) == None);
	assert!(*(t.min().unwrap()).key() == 1);
	assert!(*(t.max().unwrap()).key() == 40);
	assert!(t.rank(&1) == 1);
	assert!(t.rank(&2) == 2);
	assert!(t.rank(&3) == 3);
	assert!(t.rank(&40) == 4);
	assert!(t.rank(&30) == -4);
	assert!(t.rank(&50) == -5);
	assert!(*(t.index(1).unwrap()).key() == 1);
	assert!(*(t.index(2).unwrap()).key() == 2);
	assert!(*(t.index(3).unwrap()).key() == 3);
	assert!(*(t.index(4).unwrap()).key() == 40);
	assert!(show(&t) == vec![1,11,2,21, 3, 31, 40, 40]);
	assert!(t.insert(90, 90));
	assert!(t.insert(80, 80));
	assert!(t.insert(70, 70));
	assert!(t.insert(60, 60));
	assert!(t.insert(50, 50));
	assert!(t.delete(&70, true).unwrap().unwrap() == 70);
	assert!(show(&t) == vec![1,11,2,21, 3, 31, 40, 40, 50, 50, 60, 60, 80, 80, 90, 90]);
	assert!(t.insert(70, 71));
	assert!(show(&t) == vec![1,11,2,21, 3, 31, 40, 40, 50, 50, 60, 60,  70, 71, 80, 80, 90, 90]);
	assert!(t.pop_min(true).unwrap().unwrap().value() == &11);
	assert!(show(&t) == vec![2,21, 3, 31, 40, 40, 50, 50, 60, 60,  70, 71, 80, 80, 90, 90]);
	assert!(t.pop_max(true).unwrap().unwrap().value() == &90);
	assert!(show(&t) == vec![2,21, 3, 31, 40, 40, 50, 50, 60, 60,  70, 71, 80, 80]);
	assert!(t.remove(3, true).unwrap().unwrap().key() == &40);
	assert!(show(&t) == vec![2,21, 3, 31, 50, 50, 60, 60,  70, 71, 80, 80]);
}

// #[test]
#[should_panic]
fn failing_test() {
	assert!(1i32 == 2i32);
}

#[test]
fn test_file() {
	let mut worker_pool = Box::new(WorkerPool::new(10, 1024 * 1024, 10000));
    worker_pool.run(STORE_TASK_POOL.clone());

	let open = move |f0: Result<AsyncFile>| {
		assert!(f0.is_ok());
		let write = move |f1: AsyncFile, result: Result<()>| {
			assert!(result.is_ok());
			println!("!!!!!!write file");
			let write = move |f3: AsyncFile, result: Result<()>| {
				assert!(result.is_ok());
				println!("!!!!!!write file by sync");
			};
			f1.write(WriteOptions::SyncAll(true), 8, Vec::from("Hello World!!!!!!######你好 Rust\nHello World!!!!!!######你好 Rust\nHello World!!!!!!######你好 Rust\n".as_bytes()), Box::new(write));
		};
		f0.ok().unwrap().write(WriteOptions::Flush, 0, vec![], Box::new(write));
	};
	AsyncFile::open(PathBuf::from(r"foo.txt"), AsynFileOptions::ReadWrite(1), Box::new(open));
	thread::sleep(Duration::from_millis(5000));

	let open = move |f0: Result<AsyncFile>| {
		assert!(f0.is_ok());
		println!("!!!!!!open file, symlink: {}, file: {}, only_read: {}, size: {}, time: {:?}", 
			f0.as_ref().ok().unwrap().is_symlink(), f0.as_ref().ok().unwrap().is_file(), f0.as_ref().ok().unwrap().is_only_read(), 
			f0.as_ref().ok().unwrap().get_size(), 
			(f0.as_ref().ok().unwrap().get_modified_time(), f0.as_ref().ok().unwrap().get_accessed_time(), f0.as_ref().ok().unwrap().get_created_time()));
		let read = move |f1: AsyncFile, result: Result<Vec<u8>>| {
			assert!(result.is_ok());
			println!("!!!!!!read file1, result: {:?}", result.ok().unwrap());
			let read = move |f3: AsyncFile, result: Result<Vec<u8>>| {
				assert!(result.is_ok());
				println!("!!!!!!read file3, result: {:?}", String::from_utf8(result.ok().unwrap()).unwrap_or("invalid utf8 string".to_string()));
				let read = move |f4: AsyncFile, result: Result<Vec<u8>>| {
					assert!(result.is_ok());
					println!("!!!!!!read file4, result: {:?}", String::from_utf8(result.ok().unwrap()).unwrap_or("invalid utf8 string".to_string()));
					let read = move |f7: AsyncFile, result: Result<Vec<u8>>| {
						assert!(result.is_ok());
						println!("!!!!!!read file7, result: {:?}", String::from_utf8(result.ok().unwrap()).unwrap_or("invalid utf8 string".to_string()));
						let read = move |f11: AsyncFile, result: Result<Vec<u8>>| {
							assert!(result.is_ok());
							println!("!!!!!!read file11, result: {:?}", String::from_utf8(result.ok().unwrap()).unwrap_or("invalid utf8 string".to_string()));
							let read = move |f13: AsyncFile, result: Result<Vec<u8>>| {
								assert!(result.is_ok());
								println!("!!!!!!read file13, result: {:?}", String::from_utf8(result.ok().unwrap()).unwrap_or("invalid utf8 string".to_string()));
								
							};
							f11.read(0, 1000, Box::new(read));
						};
						f7.read(0, 34, Box::new(read));
					};
					f4.read(0, 32, Box::new(read));
				};
				f3.read(0, 16, Box::new(read));
			};
			f1.read(0, 10, Box::new(read));
		};
		f0.ok().unwrap().read(0, 0, Box::new(read));
	};
	AsyncFile::open(PathBuf::from(r"foo.txt"), AsynFileOptions::OnlyRead(1), Box::new(open));
	thread::sleep(Duration::from_millis(1000));

	let rename = move |from, to, result: Result<()>| {
		assert!(result.is_ok());
		println!("!!!!!!rename file, from: {:?}, to: {:?}", from, to);

		let remove = move |result: Result<()>| {
			assert!(result.is_ok());
			println!("!!!!!!remove file");
		};
		AsyncFile::remove(PathBuf::from(r"foo.swap"), Box::new(remove));
	};
	AsyncFile::rename(PathBuf::from(r"foo.txt"), PathBuf::from(r"foo.swap"), Box::new(rename));
	thread::sleep(Duration::from_millis(1000));
}