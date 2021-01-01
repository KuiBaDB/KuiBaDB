
**KuiBaDB** is another [Postgres](http://www.postgresql.org) rewritten with Rust and multi-threading, and **KuiBaDB** focus on OLAP analysis.

**KuiBaDB** uses vectorization engine and is also catalog-driven. At this point, the parameter and return value type of UDF are `DatumBlock`, not `Datum`. DatumBlock is something like `Vec<Datum>`.

**KuiBaDB** uses columnar storage introduced in 'Hologres: A Cloud-Native Service for Hybrid Serving/Analytical Processing'. But I removed the Delete Map and added xmin, xmax for each row, xmin/xmax is saved in row storage.

**KuiBaDB** is just a toy!

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
-   [ ] Add xact system

-   [ ] Add columnar storage

-   [ ] Add Copy

-   [ ] Add SeqScan

-   [ ] Add Parallel SeqScan

-   [ ] Add checkpointer

-   ~~[ ] Rewrite Greenplum based on **KuiBaDB**~~

Greenplum, Postgres, Rust is all the best!!!
