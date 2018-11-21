extern crate pi_store;
extern crate pi_lib;
extern crate pi_db;
extern crate tempdir;

use pi_lib::atom::Atom;
use pi_lib::guid::Guid;

use pi_db::db::{ Tab, Txn };

use pi_store::lmdb_file::{LmdbTable};

#[test]
fn test_create_table_and_transaction() {
    let tab = LmdbTable::new(&Atom::from("test"));
    tab.transaction(&Guid(0), true);
}

#[test]
fn test_lmdb_table_txn() {

}
