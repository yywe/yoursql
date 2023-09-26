# Motivation of Yoursql
I have been a user of SQL for a long time (more than a decade) and has always been curious about the internals of a DBMS. Although database books talked about query parser, query plan, query execution, etc, I find there is a large gap between theory and implementation. As someone said, "talk is cheap, show me the code", I belive the best way to grasp the idea of DBMS is to implement one by yourself, then you will be able to uncover the magic.

* Why use Rust?
Just want to say Rust is very suitable for database development.

# Setup and Run for Fun
You will need Rust environment. Then clone the repo and start from test cases in: src/session/mod.rs, e.g, to run a simple SQL:

```console
 cargo test --package yoursql --lib -- test_physical_planner --nocapture
 running 1 test
the input sql: SELECT id, address from testdb.student where name = 'Andy'
todo: implement logical optimizer
todo: implement physical optimizer here
id|address
2|121 hunter street
test session::test::test_physical_planner ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 16 filtered out; finished in 0.00s
```

# Your Takeaway
Build a database is a tough job with a huge amount of code. The main takeaway is to learn how a database is built gradually. You can use the skeleton code here to do your experiments. In the latest revision, I'm keep each dev branch named as "MILSTONEn-**". So if you want to start from scratch, start from MILSTONE1, if you want learn how to build logical plan, start from MILSTONE2, etc.

# Todo List
- [x] Basic Scaffold: Catalog, Table Scan
- [x] Build Logical Plan: Logical Expression and Logical Operation
- [x] Physical Plan: Projection, Filter
- [ ] Physical Plan: Join
- [ ] Physical Plan: Aggregate
- [ ] Physical Plan: Sort
- [ ] Physical Plan: Limit & Offset
- [ ] Logical Optimizer
- [ ] Physical Optimizer
- [ ] Storage Layer

other:  finlaize create_name for physical expr, display for physical plan


# References
While learning database implemenations, I have investigated some other database projects in Rust: 
* [toydb](https://github.com/erikgrinaker/toydb) : toydb is an excellant toy relational database with ACID feature. There are mainly two parts, one is Query Engine other is Raft. For beginners to learn relational database principle, can only focus on the query part. Although it is a tody, code quality is great. 
* [arrow-datafusion](https://github.com/apache/arrow-datafusion): arrow-datafusion is a SQL query engine. Currently, most of the code in yoursql is copied from there. However, one major difference is that arrow-datafusion is based on apache arrow, and storage is column based. While here for simplicity, I decoupled the dependency and the storage model is row-based, actually a row is just a vector of values. arrow-datafusion is a great project which modularized database component with great quality.

Other Rust open source database implementation (to comment in future):
* [databend](https://github.com/datafuselabs/databend):
* [risingwave](https://github.com/risingwavelabs/risingwave):

Books (to read in future):
* Database Systems - The Complete Book

# Miscellaneous
Original PoC branch: https://github.com/yywe/yoursql/tree/main
