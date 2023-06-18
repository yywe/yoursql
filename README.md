# yoursql
Your SQL is a minimal relational database for learning the principles of relationonal databases. It was written using Rust, and the implemenation referred: 
* https://github.com/erikgrinaker/toydb
* https://github.com/risinglightdb/risinglight

However, it is much more simpler as of right now. The storage layer used Sled https://github.com/spacejam/sled, and the sql parser used https://github.com/sqlparser-rs/sqlparser-rs. The server layer used https://github.com/datafuselabs/opensrv from datafuse labs. 

Currently, it does not differentiate logic plan and physical plan, and there is no optimizer. Now it just has the basic framework setup and can perform very basic operations.

Todo list:
* Support Join.
* Support Aggregation and other operator like order and limit, etc.
* Support MVCC concurrent control.
* Enrich the expressions, currently it is very limited.
* refactor the codebase, some design is not nessesary, and some design needs to change.

Quick Simple Demo:

```console
cargo run --package yoursql --bin server
    Finished dev [unoptimized + debuginfo] target(s) in 3.71s
     Running `target/debug/server`
```

In another window:

```console
mysql -h 127.0.0.1
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 8
Server version: 5.1.10-alpha-msql-proxy

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> create table tb1(id int, age int);
Query OK, 0 rows affected (0.01 sec)
table tb1 is created

mysql> insert into tb1 values (1, 10);
Query OK, 1 row affected (0.00 sec)

mysql> select * from tb1;
+----+-----+
| id | age |
+----+-----+
| 1  | 10  |
+----+-----+
1 row in set (0.00 sec)

mysql> drop table tb1;
Query OK, 0 rows affected (0.00 sec)
table tb1 is dropped


```