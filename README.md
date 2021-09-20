
**KuiBaDB** is another [PostgreSQL](http://www.postgresql.org) rewritten with **Asynchronous Rust**, and **KuiBaDB** focus on OLAP analysis. **KuiBaDB** is also an open source implementation of [Hologres: A Cloud-Native Service for Hybrid Serving/Analytical Processing](https://www.aliyun.com/product/bigdata/hologram).

**KuiBaDB** is built on [kbio](https://github.com/KuiBaDB/kbio) and [tokio](https://docs.rs/tokio/). We only use the 'rt-multi-thread', 'rt' and 'io-util' features of tokio. All IO, including file IO and network IO, and asynchronous syscall are powered by [kbio](https://github.com/KuiBaDB/kbio).

**KuiBaDB** contains only the basic features necessary for implementing an OLAP Database, such as supporting transactions but not sub-transactions. It is hoped that as an experimental field, researchers can quickly implement their ideas based on the infrastructure provided by KuiBaDB.

**KuiBaDB** uses vectorization engine and is also catalog-driven. **KuiBaDB** uses columnar storage introduced in [Hologres](https://www.aliyun.com/product/bigdata/hologram). But I removed the Delete Map and added xmin, xmax for each row, xmin/xmax is saved in row storage.

# Roadmap

**KuiBaDB** is only developed in my free time, so the progress could be very slow.

-   [x] Add GlobalState, SessionState, WorkState. See [KuiBaDB: State](https://blog.hidva.com/2021/05/31/kuibadb-state/) for more details.
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

-   [x] Add lock manager

-   [x] Add CREATE TABLE, LOCK TABLE

    ```
    kuiba=# create table t( i int, j int );
    CREATE TABLE

    kuiba=# begin;
    BEGIN
    kuiba=*# lock table t1 in access exclusive mode;
    LOCK TABLE
    ```

-   [x] Refactoring Expression module, supporting expression result reuse, `1 + 3` will only be calculated once, and memory reuse, See [KuiBaDB: Expression](https://blog.hidva.com/2021/06/12/kuiba-expr/) for more details.

    ```
    kuiba=# select (1+3) + (1+3), 1 + 3;
     ?column? | ?column?
    ----------+----------
            8 |        4
    (1 row)
    ```

-   [x] Add [columnar storage](https://blog.hidva.com/2021/04/25/kuiba-column-storage/).

-   [x] Add Parallel Copy

    ```sql
    -- KuiBaDB
    kuiba=# create table t(col0 int,col1 int,col2 int,col3 int,col4 int,col5 int,col6 int,col7 int,col8 int,col9 int,col10 int,col11 int,col12 int,col13 int,col14 int,col15 int);
    CREATE TABLE
    Time: 92.237 ms
    -- Use one thread to parse the input and 4 threads to write data.
    -- We need to do more profiling to explain the results.
    kuiba=# copy t from '/Users/zhanyi/NOTtmp/col16row33y.csv' DELIMITERS '|' (parallel 4);
    COPY 10545903
    Time: 9142.299 ms (00:09.142)
    ```

    ```sql
    -- PostgreSQL 14beta1
    pg14beta1=# copy t from '/Users/zhanyi/NOTtmp/col16row33y.csv' DELIMITERS '|';
    COPY 10545903
    Time: 13483.658 ms (00:13.484)
    ```

-   [ ] implement the HOS introduced in [Hologres](https://www.aliyun.com/product/bigdata/hologram) with rust async/await.

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
