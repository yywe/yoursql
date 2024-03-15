# Motivation of Yoursql
Yoursql is a learning project to explore how SQL query engine is implemented in Rust language. 

# Limitations
Currently DDL and DML implementation is very minimal, just support creating table and inserting data (In-Memory). They are simply provided so we can insert data and play with it. Also, there are no query optimizers yet.

# Setup and Run

## Run as a Server
```console
cargo run --package yoursql --bin server
   Compiling yoursql v0.2.0 (/home/yy/Learning/Database/yoursql)
    Finished dev [unoptimized + debuginfo] target(s) in 4.27s
     Running `target/debug/server`
```
In another windows, connect with mysql:
```
mysql -h 127.0.0.1
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 8
Server version: 5.1.10-alpha-msql-proxy

Copyright (c) 2000, 2023, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> create table t1(id int, name varchar(30), age int);
Query OK, 0 rows affected (0.00 sec)

mysql> insert into t1 values (1, 'hello', 20), (2, 'world', 30), (10,'database', 70);
Query OK, 3 rows affected (0.00 sec)

mysql> select * from t1 where age >25 order by age desc;
+----+----------+-----+
| id | name     | age |
+----+----------+-----+
| 10 | database | 70  |
| 2  | world    | 30  |
+----+----------+-----+
2 rows in set (0.00 sec)

mysql>
```

## Run as an imbeded database
```console
cargo run --package yoursql --bin memdb
    Finished dev [unoptimized + debuginfo] target(s) in 0.12s
     Running `target/debug/memdb`
Welcome to yoursql, please type in SQL statment, or ? to show help.
yoursql>
```
## Run sqllogictest
```console
cargo test --package yoursql --lib -- test::test::sqllogicatest
    Finished test [unoptimized + debuginfo] target(s) in 0.12s
     Running unittests src/lib.rs (target/debug/deps/yoursql-9653f55cb9053950)

running 1 test
test test::test::sqllogicatest ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 23 filtered out; finished in 0.01s
```


# Todo List
- [x] Basic Scaffold: Catalog, Table Scan
- [x] Build Logical Plan: Logical Expression and Logical Operation
- [x] Physical Plan: Projection, Filter
- [x] Physical Plan: Join [Nested Loop; todo: HashJoin]
- [x] Physical Plan: Aggregate
- [x] Physical Plan: Sort
- [x] Physical Plan: Limit & Offset
- [ ] Logical Optimizer
- [ ] Physical Optimizer
- [ ] Storage Layer / Transaction

# References
While learning database implemenations, I have investigated some other database projects in Rust: 
* [toydb](https://github.com/erikgrinaker/toydb) : toydb is an excellant toy relational database with ACID feature. There are mainly two parts, one is Query Engine other is Raft. For beginners to learn relational database principle, can only focus on the query part. Although it is a toy, code quality is great. 
* [arrow-datafusion](https://github.com/apache/arrow-datafusion): arrow-datafusion is a SQL query engine. Currently, most of the code in yoursql is copied from there. However, one major difference is that arrow-datafusion is based on apache arrow, and storage is column based. While here for simplicity, I decoupled the dependency and the storage model is row-based, actually a row is just a vector of values. arrow-datafusion is a great project which modularized database component with great quality.
* [databend](https://github.com/datafuselabs/databend): the query engine of databend is interesting, it is not a classic volcano model. The query plan is converted into pipline, and the pipeline is connected as a graph. The execution is driven by the state machine with explicit scheduling. The execution engine is inspired from clickhouse.
* [risingwave](https://github.com/risingwavelabs/risingwave): risingwave is a stream database, but it can be used as a regular batch database as well. risingwave execution engine heavily used RPC, the physical plan is split to fragment and sent to different nodes for distributed execution. I assume most of the concepts (e.g, fragment) comes from Presto.

# Note
Developing a DBMS requires a huge amount of work. I'm still curious how excellant DBMS products grow from the 1st line of code to millions lines of code. Every time when I look at some database code repo, the amount of code is overwhelming. Although currently yoursql includes around 10,000 lines of code, the branches recorded how they grow to the current status:
* MILESTONE1-scaffold: this is very first step, setup the folder organization and memory table
* ...
* MILESTONE11-server: this is the last step for a minimal working toy, integrating the query engine with a server so we can use mysql client to send queries.

By follow the milestones, it is easier to undersand without worrying about overwhelmed.


# Future Learning/Investigation Direction
- [ ] learn databend and make a pipelined engine
- [ ] learn risingwave and make a distributed fragmented execution engine
- [ ] is it possible to compile the plan to native machine code?
