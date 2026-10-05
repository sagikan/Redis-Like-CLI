mod core;
mod persistence;
mod runtime;
mod commands;

use std::env;
use std::error::Error;
use tokio::net::TcpListener;
use crate::core::{bundle::Bundle, config::Config_};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = Config_::from(env::args().skip(1).collect());
    if config.load_balancer {
        return runtime::load_balancer::run(config).await;
    }
    let bundle = Bundle::from_config(config).await?;
    let listener = TcpListener::bind(format!(
        "{}:{}", bundle.config.bind_addr, bundle.config.port
    )).await?;

    let tokio_bundle = bundle.clone();
    let server_handler = tokio::spawn(async move {
        runtime::server::run(listener, tokio_bundle).await;
    });

    if !bundle.config.is_master {
        match runtime::replica::send_handshake(
            bundle.config.clone(),
            bundle.repl_state.clone(),
            bundle.db.clone()
        ).await {
            Ok((stream, attached_data)) => runtime::replica::run(stream, attached_data, bundle).await,
            Err(e) => eprintln!("Error: {e}"),
        }
    }

    server_handler.await?;
    unreachable!()
}
