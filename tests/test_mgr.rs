extern crate pi_store;
extern crate pi_lib;
extern crate pi_db;
extern crate pi_base;

use pi_store::db::{DB};
use pi_lib::atom::Atom;
use std::sync::Arc;
use pi_db::db::{TabKV, Bin, SResult};
use pi_db::mgr::Mgr;
use pi_lib::sinfo::StructInfo;
use pi_lib::guid::{ GuidGen};
use std::thread;
use std::time::Duration;
use pi_base::worker_pool::WorkerPool;
use pi_base::pi_base_impl::STORE_TASK_POOL;

#[cfg(test)]
fn create_tabkv(ware: Atom,tab: Atom,key: Bin,index: usize,value: Option<Bin>,) -> TabKV{
    TabKV{ware, tab, key, index, value}
}


#[test]
fn test_file_db_mgr(){
    let ware_name = Atom::from("file_test_mgr");
    let tab_name = Atom::from("player");
    let worker_pool = Box::new(WorkerPool::new(10, 1024 * 1024, 10000));
    worker_pool.run(STORE_TASK_POOL.clone());
	let mgr = Mgr::new(GuidGen::new(1,1));
    let db = DB::new(ware_name.clone()).expect("create db fail, name is: file");
    mgr.register(ware_name.clone(), Arc::new(db));


    let key1 = Arc::new(Vec::from(String::from("key1").as_bytes()));
    let value1 = Arc::new(Vec::from(String::from("value1").as_bytes()));
    let key2 = Arc::new(Vec::from(String::from("key2").as_bytes()));
    let value2 = Arc::new(Vec::from(String::from("value2").as_bytes()));
    let key3 = Arc::new(Vec::from(String::from("key3").as_bytes()));
    let value3 = Arc::new(Vec::from(String::from("value3").as_bytes()));

    let item1 = create_tabkv(ware_name.clone(), tab_name.clone(), key1.clone(), 0, Some(value1.clone()));
    let item2 = create_tabkv(ware_name.clone(), tab_name.clone(), key2.clone(), 0, Some(value2.clone()));
    let item3 = create_tabkv(ware_name.clone(), tab_name.clone(), key3.clone(), 0, Some(value3.clone()));

    let sinfo = Arc::new(StructInfo::new(tab_name.clone(), 8888));

    //  插入元信息
	let tr = mgr.transaction(true);
	let tr1 = tr.clone();

	let alter_back = Arc::new(move|r: SResult<()>|{
        assert!(r.is_ok());
        // println!("alter_success");

        let prepare_back = Arc::new(|r: SResult<()>|{
            assert!(r.is_ok());
            //println!("alter_prepare_success");
        });
        let r = tr1.prepare(prepare_back.clone());
        if r.is_some(){
            prepare_back(r.unwrap());
        }
        thread::sleep(Duration::from_millis(300));

        let commit_back = Arc::new(|r: SResult<()>|{
            assert!(r.is_ok());
            //println!("alter_commit_success");
        });
        let r = tr1.commit(commit_back.clone());
        if r.is_some(){
            commit_back(r.unwrap());
        }
    });
    //println!("alter_start");
	let r = tr.alter(&ware_name, &tab_name, Some(sinfo), alter_back.clone());
	if r.is_some(){
		alter_back(r.unwrap());
	}

    thread::sleep(Duration::from_millis(1000));

    //写数据
    let tr = mgr.transaction(true);
    let tr1 = tr.clone();

    let arr =  vec![item1.clone(), item2.clone(), item3.clone()];
    let write_back = Arc::new(move|r: SResult<()>|{
        assert!(r.is_ok());
        //println!("write_success");

        let prepare_back = Arc::new(|r: SResult<()>|{
            assert!(r.is_ok());
            //println!("write_prepare_success");
        });
        let r = tr1.prepare(prepare_back.clone());
        if r.is_some(){
            prepare_back(r.unwrap());
        }
        thread::sleep(Duration::from_millis(300));

        let commit_back = Arc::new(|r: SResult<()>|{
            assert!(r.is_ok());
            //println!("write_commit_success");
        });
        let r = tr1.commit(commit_back.clone());
        if r.is_some(){
            commit_back(r.unwrap());
        }
    });
    let r = tr.modify(arr, Some(100), false, write_back.clone());
    if r.is_some(){
        write_back(r.unwrap());
    }

    thread::sleep(Duration::from_millis(1500));

    let tr = mgr.transaction(false);
    let tr1 = tr.clone();
    let arr =  vec![item1.clone(), item2.clone(), item2.clone()];
    let read_back = Arc::new(move|r: SResult<Vec<TabKV>>|{
        assert!(r.is_ok());
        //println!("read_success");
        // let r = r.expect("");
        // for elem in r.iter(){
        //     match elem.value {
        //         Some(ref v) => {
        //             println!("read---------------{}", String::from_utf8_lossy(v.as_slice()));
        //         },
        //         None => (),
        //     }
        // }

        let prepare_back = Arc::new(|r: SResult<()>|{
            assert!(r.is_ok());
            //println!("read_prepare_success");
        });
        let r = tr1.prepare(prepare_back.clone());
        if r.is_some(){
            prepare_back(r.unwrap());
        }
        thread::sleep(Duration::from_millis(300));

        let commit_back = Arc::new(|r: SResult<()>|{
            assert!(r.is_ok());
            //println!("read_commit_success");
        });
        let r = tr1.commit(commit_back.clone());
        if r.is_some(){
            commit_back(r.unwrap());
        }
    });

    let r = tr.query(arr, Some(100), true, read_back.clone());
    if r.is_some(){
        read_back(r.unwrap());
    }

    thread::sleep(Duration::from_millis(3000));
}
