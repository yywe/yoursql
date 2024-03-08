# Motivation of Yoursql
Yoursql is a learning project to explore how SQL query engine is implemented in Rust language. 

# Limitations
Currently DDL and DML implementation is very minimal, just support creating table and inserting data (In-Memory). They are simply provided so we can insert data and play with it. Also, there are no query optimizers yet.

# Setup and Run
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
- [ ] Integrate Query Engine into Server based on https://github.com/datafuselabs/opensrv

# References
While learning database implemenations, I have investigated some other database projects in Rust: 
* [toydb](https://github.com/erikgrinaker/toydb) : toydb is an excellant toy relational database with ACID feature. There are mainly two parts, one is Query Engine other is Raft. For beginners to learn relational database principle, can only focus on the query part. Although it is a toy, code quality is great. 
* [arrow-datafusion](https://github.com/apache/arrow-datafusion): arrow-datafusion is a SQL query engine. Currently, most of the code in yoursql is copied from there. However, one major difference is that arrow-datafusion is based on apache arrow, and storage is column based. While here for simplicity, I decoupled the dependency and the storage model is row-based, actually a row is just a vector of values. arrow-datafusion is a great project which modularized database component with great quality.
* [databend](https://github.com/datafuselabs/databend): the query engine of databend is interesting, it is not a classic volcano model. The query plan is converted into pipline, and the pipeline is connected as a graph. The execution is driven by the state machine with explicit scheduling. The execution engine is inspired from clickhouse.
* [risingwave](https://github.com/risingwavelabs/risingwave): risingwave is a stream database, but it can be used as a regular batch database as well. risingwave execution engine heavily used RPC, the physical plan is split to fragment and sent to different nodes for distributed execution. I assume most of the concepts (e.g, fragment) comes from Presto.

# Future Learning/Investigation
- [ ] learn databend and make a pipelined engine
- [ ] learn risingwave and make a distributed fragmented execution engine
- [ ] is it possible to compile the plan to native code?

# Miscellaneous
Original PoC branch: https://github.com/yywe/yoursql/tree/main
