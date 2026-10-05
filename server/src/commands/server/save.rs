use std::{fs::{create_dir_all, write}, path::Path};
use crate::core::config::Config;
use crate::core::db::Database;
use crate::persistence::rdb::RDBFile;
use crate::core::client::{Client, Response};

pub async fn cmd_save(
    args: &[String], client: &Client, config: Config, db: Database,
) {
    if !args.is_empty() {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    let path = Path::new(&config.dir).join(&config.dbfilename);
    let result = async {
        create_dir_all(Path::new(&config.dir))?;
        let mut rdb = RDBFile::default();
        rdb._gen_database(Some(vec![db])).await;
        rdb.gen_eof();
        write(path, rdb.to_vec())?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }.await;

    match result {
        Ok(()) => client.tx.send(Response::Ok.into()).unwrap(),
        Err(error) => client.tx.send(
            format!("-ERR failed to save database: {error}\r\n").into_bytes()
        ).unwrap(),
    }
}


