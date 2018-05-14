extern crate rocksdb;


use rocksdb::{TXN_DB, Error, DB, DBVector, TXN, Options, TransactionDBOptions, TransactionOptions, ColumnFamily, WriteOptions, ReadOptions};

#[test]
fn transaction() {
    let path = "_rust_rocksdb_transaction";
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut txn_opts = TransactionOptions::default();
        let mut txn_db_opts = TransactionDBOptions::default();
        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();
        let db = TXN_DB::open(&opts, &txn_db_opts, path).unwrap();
        
        //
        let txn = TXN::begin(&db, &write_opts, &txn_opts).unwrap();
        assert!(txn.set_name("xid111").is_ok());
        let p = txn.put(b"k1", b"v1111");
        assert!(p.is_ok());
        let r: Result<Option<DBVector>, Error> = txn.get(&read_opts, b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        assert!(txn.prepare().is_ok());
        assert!(txn.commit().is_ok());
        //事务回滚
        let txn = TXN::begin_old(&db, &write_opts, &txn_opts, &txn).unwrap();

        let r: Result<Option<DBVector>, Error> = txn.get(&read_opts, b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        let p = txn.put(b"k2", b"v22222");
        assert!(p.is_ok());
        assert!(txn.rollback().is_ok());
        //读取回滚数据
        let txn = TXN::begin_old(&db, &write_opts, &txn_opts, &txn).unwrap();
        assert!(txn.get(&read_opts, b"k2").unwrap().is_none());
        assert!(txn.commit().is_ok());
        assert!(txn.destroy().is_ok());
    }
}

#[test]
fn transaction_cf() {
    let path = "_rust_rocksdb_transaction_cf";
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let mut txn_opts = TransactionOptions::default();
        let mut txn_db_opts = TransactionDBOptions::default();
        let write_opts = WriteOptions::default();
        let read_opts = ReadOptions::default();

        let db = TXN_DB::open(&opts, &txn_db_opts, path).unwrap();
        
        let cf1 = db.cf(&opts, "cf1").unwrap();
        // let cf1 = match db.cf(&opts, "cf1") {
        //     Ok(cf) => cf,
        //     _ => panic!("value not present!"),
        // };
        
        let txn = TXN::begin(&db, &write_opts, &txn_opts).unwrap();
        assert!(txn.set_name("xid111").is_ok());
        let p = txn.put_cf(&cf1, b"k1", b"v1111");
        assert!(p.is_ok());
        let r: Result<Option<DBVector>, Error> = txn.get_cf(&read_opts, &cf1, b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        assert!(txn.prepare().is_ok());
        assert!(txn.commit().is_ok());
        //事务回滚
        let txn = TXN::begin_old(&db, &write_opts, &txn_opts, &txn).unwrap();

        let r: Result<Option<DBVector>, Error> = txn.get_cf(&read_opts, &cf1, b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");

        let p = txn.put_cf(&cf1, b"k2", b"v22222");
        assert!(p.is_ok());
        assert!(txn.rollback().is_ok());
        //读取回滚数据
        let txn = TXN::begin_old(&db, &write_opts, &txn_opts, &txn).unwrap();
        assert!(txn.get_cf(&read_opts, &cf1, b"k2").unwrap().is_none());
        assert!(txn.commit().is_ok());
        assert!(txn.destroy().is_ok());
    }
}