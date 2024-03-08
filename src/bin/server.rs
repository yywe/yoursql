use std::io;

use mysql_common::constants::ColumnFlags;
use mysql_common::constants::ColumnType::MYSQL_TYPE_STRING;
use opensrv_mysql::{Column, *};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use yoursql::common::record_batch::RecordBatch;
use yoursql::common::schema::RESP_SCHEMA_REF;
use yoursql::common::types::DataValue;
use yoursql::expr::logical_plan::LogicalPlan;
use yoursql::parser::parse;
use yoursql::session::SessionContext;

/// cargo run --package yoursql --bin server
/// mysql -h 127.0.0.1

struct Backend {
    session: SessionContext,
}
impl Default for Backend {
    fn default() -> Self {
        Backend {
            session: SessionContext::default(),
        }
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(42, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        println!("execute sql {:?}", sql);
        // note we cannot map error and ? like below:
        // let statement = parse(sql).map_err(|err| {
        // std::io::Error::new(
        // std::io::ErrorKind::Other,
        // format!("Parser error: {:?}", err),
        // )
        // })?;
        // , the error will cause connection lost
        // like:
        // mysql> create t1;
        // ERROR 2013 (HY000): Lost connection to MySQL server during query
        // No connection. Trying to reconnect...
        // Connection id:    8
        // Current database: *** NONE ***
        // instead, we need to handle the error explicity.
        // FOR SIMPLICITY, ALL LATER ERROR ARE SET AS: ErrorKind::ER_PARSE_ERROR
        let statement = match parse(sql) {
            Ok(statement) => statement,
            Err(e) => {
                results
                    .error(
                        ErrorKind::ER_PARSE_ERROR,
                        AsRef::<[u8]>::as_ref(&e.to_string()),
                    )
                    .await?;
                return Ok(());
            }
        };
        let session_state = self.session.state.read().clone();
        let logical_plan = match session_state.make_logical_plan(statement).await {
            Ok(logical_plan) => logical_plan,
            Err(e) => {
                results
                    .error(
                        ErrorKind::ER_PARSE_ERROR,
                        AsRef::<[u8]>::as_ref(&e.to_string()),
                    )
                    .await?;
                return Ok(());
            }
        };

        let physical_plan = match session_state.create_physical_plan(&logical_plan).await {
            Ok(physical_plan) => physical_plan,
            Err(e) => {
                results
                    .error(
                        ErrorKind::ER_PARSE_ERROR,
                        AsRef::<[u8]>::as_ref(&e.to_string()),
                    )
                    .await?;
                return Ok(());
            }
        };

        let record_batches = match session_state.execute_physical_plan(physical_plan).await {
            Ok(record_batches) => record_batches,
            Err(e) => {
                results
                    .error(
                        ErrorKind::ER_PARSE_ERROR,
                        AsRef::<[u8]>::as_ref(&e.to_string()),
                    )
                    .await?;
                return Ok(());
            }
        };
        if record_batches.len() == 0 {
            return results.completed(OkResponse::default()).await;
        } else {
            if matches!(logical_plan, LogicalPlan::Insert(_)) {
                let response = match parse_response_row(&record_batches) {
                    Ok(response) => response,
                    Err(e) => {
                        results
                            .error(
                                ErrorKind::ER_PARSE_ERROR,
                                AsRef::<[u8]>::as_ref(&e.to_string()),
                            )
                            .await?;
                        return Ok(());
                    }
                };

                return results.completed(response).await;
            } else {
                let fields = record_batches[0].schema.all_fields();
                let header: Vec<Column> = fields
                    .iter()
                    .map(|f| Column {
                        table: "unkown".into(),
                        column: f.name().clone(),
                        coltype: MYSQL_TYPE_STRING,
                        colflags: ColumnFlags::NOT_NULL_FLAG,
                    })
                    .collect();
                let mut row_writer = results.start(&header).await?;
                for batch in record_batches.into_iter() {
                    for row in batch.rows {
                        let mut mysql_row = Vec::new();
                        for val in row {
                            mysql_row.push(format!("{}", val));
                        }
                        row_writer.write_row(mysql_row).await?;
                    }
                }
                return row_writer.finish().await;
            }
        }
    }
}

/// assume the batches contain query execution response (1row), extract okResponse
//  Ref:
// Field::new("header", DataType::UInt8, true, None),
// Field::new("affected_rows", DataType::UInt64, true, None),
// Field::new("last_insert_id", DataType::UInt64, true, None),
// Field::new("warnings", DataType::UInt16, true, None),
// Field::new("info", DataType::Utf8, true, None),
// Field::new("session_state_info", DataType::Utf8, true, None),
macro_rules! get_inner_value {
    ($value:expr, $variant:ident) => {
        match $value {
            DataValue::$variant(Some(inner)) => Ok(inner.clone()),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Expected {} variant with Some value", stringify!($varint)),
            )),
        }
    };
}

fn parse_response_row(batches: &Vec<RecordBatch>) -> io::Result<OkResponse> {
    if batches.len() != 1
        || batches[0].rows.len() != 1
        || batches[0].rows[0].len() != RESP_SCHEMA_REF.all_fields().len()
    {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("Invalid Query Response Row: {:?}", batches),
        ));
    }
    let row = batches[0].rows[0].clone();
    let mut response = OkResponse::default();
    response.header = get_inner_value!(row[0], UInt8)?;
    response.affected_rows = get_inner_value!(row[1], UInt64)?;
    response.last_insert_id = get_inner_value!(row[2], UInt64)?;
    response.warnings = get_inner_value!(row[3], UInt16)?;
    response.info = get_inner_value!(&row[4], Utf8)?;
    response.session_state_info = get_inner_value!(&row[5], Utf8)?;
    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:3306").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        let backend = Backend::default();
        tokio::spawn(async move { AsyncMysqlIntermediary::run_on(backend, r, w).await });
    }
}
