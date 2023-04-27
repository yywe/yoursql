use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("operation must be in master database, current dbid:{0}")]
    MustInMaster(u32),
    #[error("the database {0} has already been registered")]
    DBRegistered(String),
    #[error("did not find the next databae id")]
    NextDBIDNotFound,
}