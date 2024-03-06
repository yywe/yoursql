use anyhow::Result;
use std::io;
use std::io::Write;
use yoursql::parser::parse;
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
            let statement = match parse(&command) {
                Ok(statement) => statement,
                Err(err) => {
                    println!("Error parsing SQL: {}", err);
                    continue;
                }
            };
            let logical_plan = match session.state.read().make_logical_plan(statement).await {
                Ok(logical_plan) => logical_plan,
                Err(err) => {
                    println!("Error making logical plan: {}", err);
                    continue;
                }
            };
            //println!("logical plan:{:?}", logical_plan);
            let physical_plan = match session
                .state
                .read()
                .create_physical_plan(&logical_plan)
                .await
            {
                Ok(physical_plan) => physical_plan,
                Err(err) => {
                    println!("Error making physical plan: {}", err);
                    continue;
                }
            };
            let record_batches = match session
                .state
                .read()
                .execute_physical_plan(physical_plan)
                .await
            {
                Ok(record_batches) => record_batches,
                Err(err) => {
                    println!("Error executing the physical plan: {}", err);
                    continue;
                }
            };
            if record_batches.is_empty() {
                println!("Empty ResultSet");
            } else {
                session.state.read().print_record_batches(&record_batches);
            }
        }
    }
    Ok(())
}
