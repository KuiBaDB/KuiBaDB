
**KuiBaDB** is another [Postgres](http://www.postgresql.org) rewritten with Rust and multi-threading, and **KuiBaDB** focus on OLAP analysis.

**KuiBaDB** contains only the basic features necessary for implementing an OLAP Database, such as supporting transactions but not sub-transactions. It is hoped that as an experimental field, researchers can quickly implement their ideas based on the infrastructure provided by KuiBaDB.

**KuiBaDB** uses vectorization engine and is also catalog-driven. At this point, the parameter and return value type of UDF are `DatumBlock`, not `Datum`. DatumBlock is something like `Vec<Datum>`.

**KuiBaDB** uses columnar storage introduced in 'Hologres: A Cloud-Native Service for Hybrid Serving/Analytical Processing'. But I removed the Delete Map and added xmin, xmax for each row, xmin/xmax is saved in row storage.

# Roadmap

**KuiBaDB** is only developed in my free time, so the progress could be very slow.

-   [x] Add guc
-   [x] Support `select expr1, expr2`:

    ```
    $ psql -h 127.0.0.1 -p 1218 kuiba
    psql (13.1, server 0.0.1)
    Type "help" for help.

    kuiba=# select 2020 - 2 as hello, 1207 + 11 as world;
    hello  | world
    -------+-------
    2018   | 1218
    (1 row)
    ```
-   [x] Add slru and clog. The clog supports two-levels cache and vectorization.
-   [x] Add wal. We have moved all the IO operations out of the lock!
-   [x] Add crash recovery.
-   [x] Add xact system

    ```
    2021-04-10T10:35:19.424402+08:00 - INFO - start redo. ctl=Ctl { time: 2021-04-03T22:55:14+08:00, ckpt: 20181218, ckptcpy: Ckpt { redo: 20181218, curtli: 1, prevtli: 1, nextxid: 2, nextoid: 65536, time: 2021-04-03T22:55:14+08:00 } }
    2021-04-10T10:35:19.554540+08:00 - INFO - end redo because of failed read. endlsn=20711748 endtli=1 err=No such file or directory (os error 2)
    2021-04-10T10:35:19.554693+08:00 - INFO - End of redo. nextxid: 15604, nextoid: 65536
    2021-04-10T10:35:19.555442+08:00 - INFO - listen. port=1218

    kuiba=# begin;
    BEGIN
    kuiba=*# select 1;
    1
    kuiba=*# commit;
    COMMIT
    kuiba=# begin;
    BEGIN
    kuiba=*# select 1;
    1
    kuiba=*# select x;
    ERROR:  parse query failed: Parse Error. UnrecognizedToken { token: (7, Token(16, "x"), 8), expected: ["\"(\"", "\"+\"", "\"-\"", "\";\"", "DECIMAL", "INTEGER", "XB"] }
    kuiba=!# select 1;
    ERROR:  current transaction is aborted, commands ignored until end of transaction block:
    kuiba=!# commit;
    ROLLBACK
    ```
-   [x] Implement PG-style shared buffer: `SharedBuf<K: Copy, V, E: EvictPolicy>`.

    `SharedBuf<TableId, SuperVersion, LRUPolicy>` will be used to save the mapping between the table and its SuperVersion. In RocksDB, SuperVersion of ColumnFamily is memory resident. but OLAP system may have many tables, we should support swapping the SuperVersion of some infrequently used tables out to disk.

    `SharedBuf<TableId, SharedBuf<PageId, Page, FIFOPolicy>, LRUPolicy>` will be used to save the xmin/xmax/hints page for table file.

-   [ ] Add columnar storage

-   [ ] Add Copy

-   [ ] Add SeqScan

-   [ ] Add Parallel SeqScan

-   [ ] Add checkpointer

-   ~~[ ] Rewrite Greenplum based on **KuiBaDB**~~

# Run Test

```
export KUIBADB_DATADIR=/tmp/kuibadir4test
./target/debug/initdb  $KUIBADB_DATADIR
echo 'clog_l2cache_size: 1' >> $KUIBADB_DATADIR/kuiba.conf
cargo test
```

Greenplum, Postgres, Rust is all the best!!!
