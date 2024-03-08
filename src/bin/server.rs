use mysql_common::constants::{ColumnFlags, ColumnType::MYSQL_TYPE_STRING};
use opensrv_mysql::{Column, *};
use std::io;
use tokio::{io::AsyncWrite, net::TcpListener};
use yoursql::{parser::parse, session::SessionContext};

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
        results.start(&[]).await?.finish().await
        /* 
        let statement = parse(sql).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Parser error: {:?}", err),
            )
        })?;
        let session_state = self.session.state.read().clone();
        let logical_plan = session_state
            .make_logical_plan(statement)
            .await
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("logical plan error: {:?}", err),
                )
            })?;
        let physical_plan = session_state
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("physical plan error: {:?}", err),
                )
            })?;
        let record_batches = session_state
            .execute_physical_plan(physical_plan)
            .await
            .map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("execution error: {:?}", err),
                )
            })?;
        if record_batches.len() == 0 {
            return results
                .completed(OkResponse {
                    info: format!("empty resultset"),
                    ..Default::default()
                })
                .await;
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
         */
    }
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
