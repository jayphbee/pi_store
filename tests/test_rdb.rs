// extern crate rocksdb;

// use std::thread;

// use rocksdb::*;
// use rocksdb::{DB, WriteBatch, WriteOptions};

// #[test]
// fn test_base() {
//     let mut opts = Options::default();
//     //基础配置
//     opts.create_if_missing(true);                                                               //库不存在，则创建
//     opts.set_max_open_files(4096);                                                              //最大可以打开文件数，-1表示无限制，注意应小于linux当前shell配置的最大可以打开文件数
//     opts.set_max_background_flushes(2);                                                         //设置后台最大刷新线程数
//     opts.enable_statistics();                                                                   //允许统计数据库信息
//     opts.set_stats_dump_period_sec(300);                                                        //每5分钟将统计信息写入日志文件
//     //压缩配置
//     // opts.set_compression_type(DBCompressionType::Snappy);                                       //使用Snappy进行压缩
//     // opts.set_compression_per_level(&[
//     //     DBCompressionType::None,
//     //     DBCompressionType::None,
//     //     DBCompressionType::Snappy,
//     //     DBCompressionType::Snappy,
//     //     DBCompressionType::Snappy
//     // ]);                                                                                         //设置每级压缩，低级不压缩，高级使用Snappy压缩
//     // opts.set_compaction_readahead_size(2 * 1024 * 1024);                                        //压缩预读大小，HDD应该不小于2MB，以保证尽量顺序访问磁盘，SSD可以为0
//     opts.set_compaction_style(DBCompactionStyle::Universal);                                    //压缩样式
//     opts.set_max_background_compactions(2);                                                     //设置后台最大压缩线程数
//     //写相关配置
//     opts.set_use_fsync(true);                                                                   //设置落地时使用fsync还是fdatasync，设置为false将会提高落地效率，但在ext3中最好设置为true，以防止丢失数据
//     opts.set_allow_concurrent_memtable_write(false);                                            //设置是否允许并发写Memtable，一般关闭，因为当前兼容性不好
//     opts.set_write_buffer_size(0x2000000);                                                      //写缓冲大小，可以提高写性能，但会降低库打开性能，可在运行时改变
//     // opts.set_bytes_per_sync(1024 * 1024);                                                       //限制同步速度，这会在后台用异步线程将内存数据同步到文件
//     //文件配置
//     opts.set_max_bytes_for_level_base(256 * 1024 * 1024);                                       //设置L1的大小
//     opts.set_max_bytes_for_level_multiplier(1.0);                                               //设置multiplier
//     //WAL配置
//     // opts.set_wal_dir("./wal");                                                                  //设置wal的路径，默认和数据库同路径
//     opts.set_wal_recovery_mode(DBRecoveryMode::AbsoluteConsistency);                            //通过wal恢复的模式

//     let mut bopts = BlockBasedOptions::default();
//     bopts.set_block_size(16 * 1024 * 1024);                                                     //设置块大小
//     bopts.set_lru_cache(1024 * 1024);                                                           //设置块缓存大小
//     bopts.set_bloom_filter(10, false);                                                          //使用不基于块的Bloom过滤器
//     bopts.set_cache_index_and_filter_blocks(false);                                             //不缓存索引和过滤器块

//     let mut wopts = WriteOptions::default();
//     wopts.set_sync(false);                                                                      //无需同步
//     wopts.disable_wal(false);                                                                   //需要写前导日志

//     let db = match DB::open(&opts, "./db") {
//         Err(e) => {
//             panic!("!!!!!!db start failed, err: {}", e.to_string());
//         },
//         Ok(v) => {
//             println!("!!!!!!db start ok");
//             v
//         },
//     };

//     match DB::repair(opts, "./db") {
//         Err(e) => {
//             panic!("!!!!!!db repair failed, err: {}", e.to_string());
//         },
//         Ok(_) => {
//             println!("!!!!!!db repair ok");
//         },
//     }

//     let mut key = 0x0;
//     loop {
//         // println!("!!!!!!{:?}", opts.get_statistics());
//         let mut batch = WriteBatch::default();
//         batch.put(key.to_string().as_bytes(), b"asdfasdfasdfasdfasdfasdfasdf");
//         key += 1;
//         batch.put(key.to_string().as_bytes(), b"asdfasdfasfdasdfasdfasdfasdf");
//         key += 1;
//         batch.put(key.to_string().as_bytes(), b"asdfasdfasdfasdfasdfasdfasdf");
//         key += 1;
//         match db.write_opt(batch, &wopts) {
//             Err(e) => {
//                 println!("!!!!!!db write failed, e: {}", e.to_string());
//             },
//             Ok(_) => {
//                 println!("!!!!!!write batch ok");
//             },
//         }
//         // thread::sleep_ms(10);
//     }
// }
