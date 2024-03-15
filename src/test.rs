use sqllogictest::{DBOutput, DefaultColumnType};
use tokio::runtime::Runtime;

use crate::session::SessionContext;

/// cargo test --package yoursql --lib -- test::test::sqllogicatest --exact --nocapture

struct Database {
    session: SessionContext,
}
impl Default for Database {
    fn default() -> Self {
        Database {
            session: SessionContext::default(),
        }
    }
}
impl sqllogictest::DB for Database {
    type Error = std::io::Error;
    type ColumnType = DefaultColumnType;
    fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        let rt = Runtime::new().unwrap();
        let session_state = self.session.state.read().clone();
        let batches = match rt.block_on(session_state.run(sql)) {
            Ok(batches) => batches,
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("execution error {}:{}", sql, e),
                ));
            }
        };
        let lowered_sql = sql.trim_start().to_ascii_lowercase();
        if !lowered_sql.starts_with("select") {
            return Ok(DBOutput::StatementComplete(0));
        }
        let mut types = vec![];
        let mut rows = vec![];
        for batch in batches {
            if types.len() == 0 {
                for _ in 0..batch.schema.all_fields().len() {
                    types.push(DefaultColumnType::Any)
                }
            }
            for row in batch.rows {
                let mut logi_row = vec![];
                for value in row {
                    logi_row.push(format!("{value}"))
                }
                rows.push(logi_row);
            }
        }
        Ok(DBOutput::Rows {
            types: types,
            rows: rows,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{env, fs};

    use super::*;
    #[test]
    fn sqllogictest() {
        let tests_dir = env::current_dir().unwrap().join("tests");
        let mut tscripts = Vec::new();
        if let Ok(entries) = fs::read_dir(&tests_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let file_name = entry.file_name();
                    if file_name.to_str().unwrap().ends_with(".slt") {
                        tscripts.push(tests_dir.join(file_name));
                    }
                }
            }
        } else {
            eprintln!("Failed to read directory: {:?}", tests_dir);
        }
        for ts in tscripts {
            let db = Database::default();
            let mut tester = sqllogictest::Runner::new(db);
            println!("=====Run Test {} ========", ts.to_str().unwrap());
            tester.run_file(ts).unwrap();
        }
    }
}
