use std::io;
use std::io::Write;

use anyhow::Result;
use yoursql::common::types::DataValue;
use yoursql::session::SessionContext;

/// cargo run --package yoursql --bin memdb

#[tokio::main]
async fn main() -> Result<()> {
    let session = SessionContext::default();
    println!("Welcome to yoursql, please type in SQL statment, or ? to show help.");
    loop {
        print!("yoursql>");
        io::stdout().flush().unwrap();
        let mut command = String::new();
        io::stdin().read_line(&mut command)?;
        let mut command = command.trim().to_owned();
        if command.ends_with(';') {
            command.pop();
        }
        // builtin command
        if command.eq("?") {
            println!("quit -- quit the console, note data are not saved");
            println!("show tables --  show tables in default database (master)");
        } else if command.eq("quit") {
            println!("Goodbye!");
            break;
        } else if command.eq("show tables") {
            let dbname = session
                .state
                .read()
                .config()
                .catalog
                .default_database
                .clone();
            println!("tables in database: {}", dbname);
            session.database(&dbname.clone()).map(|db| {
                let table_names = db.table_names();
                if table_names.is_empty() {
                    println!("The database contains no tables.");
                } else {
                    table_names.iter().for_each(|s| println!("{}\n", s));
                }
            });
        }
        // SQL statement
        else {
            let record_batches = match session.state.read().run(&command).await {
                Ok(record_batches) => record_batches,
                Err(err) => {
                    println!("Error executing query {}: {}", command, err);
                    continue;
                }
            };
            if record_batches.is_empty() {
                println!("Empty ResultSet");
            } else {
                if command
                    .trim_start()
                    .to_ascii_lowercase()
                    .starts_with("insert")
                {
                    let row = record_batches[0].rows[0].clone();
                    let affected_rows = match row[1] {
                        DataValue::UInt64(num) => num,
                        _ => None,
                    }
                    .unwrap();
                    println!("rows affected {}", affected_rows);
                } else {
                    session.state.read().print_record_batches(&record_batches);
                }
            }
        }
    }
    Ok(())
}
