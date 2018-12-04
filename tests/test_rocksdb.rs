//  extern crate rocksdb;

//  use rocksdb::{DB, Options, TXN_DB, TXN, ReadOptions, TransactionDBOptions, WriteOptions, TransactionOptions};

//  #[test]
//  fn test() {
//     //let db = DB::open_default("_$rocksdb/file/_$sinfo").unwrap();
//     // db.put(b"my key", b"my value");
//     // match db.put(b"my key", b"my value"){
//     //     Ok(_) => println!("yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"),
//     //     Err(_) => println!("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
//     // }
//     // db.put(b"my key1", b"my value1");
//     // &
//     // let it = DB::open_default("path/for/rocksdb/storage").expect("kkkkkkkkkkkkkkkkk").raw_iterator();
//     //let x = it.raw_iterator(); //打开元信息表可能出现异常

// //     // //let db = DB::open_default("path/for/rocksdb/storage").unwrap();

// //     // let db = DB::open_default("path/for/rocksdb/aaa").unwrap();
// //     // db.put(b"my key", b"my value");

// //     // let mut opts = Options::default();
// //     // let l = DB::list_cf(&opts, "path/for/rocksdb/aaa").expect("");
// //     //  println!("retrieved value {:?}", l);

// //     // match db.get(b"my key") {
// //     //     Ok(Some(value)) => println!("retrieved value {}", value.to_utf8().unwrap()),
// //     //     Ok(None) => println!("value not found"),
// //     //     Err(e) => println!("operational problem encountered: {}", e),
// //     // }
// //     // db.delete(b"my key").unwrap();

//  //DB::open_default("_$rocksdb/file_test/_$sinfo").expect("ffffffffffffffffffffffffffffff");

// //     let mut opts = Options::default();
// //     opts.create_if_missing(true);
// //     let db = TXN_DB::open(&opts, &TransactionDBOptions::default(), "_$rocksdb/file_test/_$sinfo").unwrap();

// //     let rocksdb_txn = TXN::begin(&db, &WriteOptions::default(), &TransactionOptions::default()).unwrap();
// //     // let rocksdb_txn1 = TXN::begin(&db, &WriteOptions::default(), &TransactionOptions::default()).unwrap();
// //     //rocksdb_txn.put(b"my key", b"my value");
// //     // rocksdb_txn.put(b"my key1", b"my value1");
// //     // match rocksdb_txn1.put(b"my key", b"my value"){
// //     //     Ok(_) => (),
// //     //     Err(e) => panic!("-----------------------------------------{:?}", e),
// //     // }
// //     match rocksdb_txn.set_name("kkkkkkkkk"){
// //             Ok(_) => (),
// //             Err(e) => println!("{:?}", e)
// //         }
// //     rocksdb_txn.put(b"my key1", b"my value1");

// //     rocksdb_txn.prepare();
// // //    // let a = rocksdb_txn1.prepare();
// // //     //rocksdb_txn.rollback();
// //     rocksdb_txn.commit();

// //     // rocksdb_txn1.commit();
// //     }
// //     //DB::drop(&opts, "path/for/rocksdb/hhhhhhh");
// //     let db = TXN_DB::open(&opts, &TransactionDBOptions::default(), "path/for/rocksdb/hhhhhhh").unwrap();
// //     let rocksdb_txn = TXN::begin(&db, &WriteOptions::default(), &TransactionOptions::default()).unwrap();

// //     //println!("----------------{:?}",  a);

// //     println!("----------------{:?}", rocksdb_txn.get(&ReadOptions::default(), b"my key").unwrap());
// //     // println!("----------------{:?}", rocksdb_txn.get(&ReadOptions::default(), b"my key3").unwrap());
//  }
