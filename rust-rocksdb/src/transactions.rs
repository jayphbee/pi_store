use {TXN_DB, DBVector, TXN, Error, Options, TransactionDBOptions, TransactionOptions, ReadOptions, ColumnFamily, WriteOptions, DBRawIterator};
use ffi;
use ffi::rocksdb_transaction_t;

use libc::{c_char, size_t};
use std::ffi::CString;
use std::fs;
use std::path::Path;
use std::ptr;


impl TXN_DB {
    /// Open a database with the given database options and column family names/options.
    pub fn open<P: AsRef<Path>>(opts: &Options, txn_db_options: &TransactionDBOptions, path: P) -> Result<TXN_DB, Error> {
        let path = path.as_ref();
        let cpath = match CString::new(path.to_string_lossy().as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err(Error::new(
                    "Failed to convert path to CString \
                                       when opening DB."
                        .to_owned(),
                ))
            }
        };

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB\
                                           directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_transactiondb_t;
        unsafe {
            db = ffi_try!(ffi::rocksdb_transactiondb_open(opts.inner, txn_db_options.inner, cpath.as_ptr() as *const _,));
        }

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(TXN_DB {
            inner: db,
            path: path.to_path_buf(),
        })
    }

    pub fn cf(&self, options: &Options, cf_name: &str) -> Result<ColumnFamily, Error> {

        let c_name = CString::new(cf_name).unwrap();
        unsafe {
            let cf = ffi_try!(ffi::rocksdb_transactiondb_create_column_family(
                self.inner,
                options.inner,
                c_name.as_ptr() as *const c_char,
            ));
            if cf.is_null(){
                return Err(Error::new("Could not initialize database.".to_owned()));    
            }
            Ok(ColumnFamily {
            inner: cf
        })
        }
    }
    
}

impl TXN {
    pub fn clone(&self) -> Self {
        TXN{
            inner: self.inner,
            path: self.path.clone(),
        }
    }
    pub fn begin(db: &TXN_DB, write_options: &WriteOptions, txn_options: &TransactionOptions) -> Result<TXN, Error> {

        unsafe {
            let txn_db = ffi::rocksdb_transaction_begin(
                db.inner,
                write_options.inner,
                txn_options.inner,
                ptr::null::<rocksdb_transaction_t>() as *mut rocksdb_transaction_t
            );
            if txn_db.is_null(){
                return Err(Error::new("Could not initialize database.".to_owned()));    
            }
            Ok(TXN {
            inner: txn_db,
            path: db.path.clone()
        })
        }
    }
    pub fn begin_old(db: &TXN_DB, write_options: &WriteOptions, txn_options: &TransactionOptions, old_txn: &TXN) -> Result<TXN, Error> {

        unsafe {
            let txn_db = ffi::rocksdb_transaction_begin(
                db.inner,
                write_options.inner,
                txn_options.inner,
                old_txn.inner
            );
            if txn_db.is_null(){
                return Err(Error::new("Could not initialize database.".to_owned()));    
            }
            Ok(TXN {
            inner: txn_db,
            path: db.path.clone()
        })
        }
    }

    pub fn iter(&self, readopts: &ReadOptions) -> DBRawIterator {
        unsafe {
            DBRawIterator {
                inner: ffi::rocksdb_transaction_create_iterator(self.inner, readopts.inner),
            }
        }
    }
    
    pub fn set_name(&self, name: &str) -> Result<(), Error> {
        
        let cname = CString::new(name).unwrap();
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_setName(
                self.inner,
                cname.as_ptr() as *const c_char,
            ));
            Ok(())
        }
    }

    pub fn get(&self, readopts: &ReadOptions, key: &[u8]) -> Result<Option<DBVector>, Error> {
        
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get(
                self.inner,
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }
    pub fn get_cf(&self, readopts: &ReadOptions, column_family: &ColumnFamily, key: &[u8]) -> Result<Option<DBVector>, Error> {
        
        unsafe {
            let mut val_len: size_t = 0;
            let val = ffi_try!(ffi::rocksdb_transaction_get_cf(
                self.inner,
                readopts.inner,
                column_family.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len,
            )) as *mut u8;
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }
    pub fn put_cf(&self, column_family: &ColumnFamily, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_put_cf(
                self.inner,
                column_family.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            ));
            Ok(())
        }
    }
    pub fn delete(&self, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }
    pub fn delete_cf(&self, column_family: &ColumnFamily, key: &[u8]) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_delete_cf(
                self.inner,
                column_family.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));
            Ok(())
        }
    }

    pub fn prepare(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_prepare(
                self.inner,
            ));
            Ok(())
        }
    }

    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_commit(
                self.inner,
            ));
            Ok(())
        }
    }
    pub fn rollback(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_transaction_rollback(
                self.inner,
            ));
            Ok(())
        }
    }
    pub fn destroy(&self) -> Result<(), Error> {
        unsafe {
            ffi::rocksdb_transaction_destroy(
                self.inner,
            );
            Ok(())
        }
    }
}

impl Drop for TXN_DB {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_transactiondb_close(self.inner);
        }
    }
}