use yoursql::storage::SledStore;
use tracing::{debug, info, span, Level};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

fn main(){
    tracing_subscriber::registry()
        .with(fmt::layer())
        .init();
    info!("starting the database...");

    let db = SledStore::new("/home/yy/Learning/Database/yoursql/tempdb");

}