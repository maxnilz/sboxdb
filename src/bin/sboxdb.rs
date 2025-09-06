use sboxdb::config::Config;
use sboxdb::error::Result;
use sboxdb::server::ClusterServer;
use sboxdb::server::Server;
use sboxdb::server::StandaloneServer;

#[tokio::main]
async fn main() -> Result<()> {
    let args = clap::command!()
        .arg(
            clap::Arg::new("config")
                .short('c')
                .long("config")
                .help("Configuration file path for the server")
                .default_value("config/sboxdb.yaml"),
        )
        .get_matches();
    let cfg = Config::new(args.get_one::<String>("config").unwrap().as_ref())?;
    let loglevel = cfg.log_level.parse::<simplelog::LevelFilter>()?;
    let mut logconfig = simplelog::ConfigBuilder::new();
    simplelog::SimpleLogger::init(loglevel, logconfig.build())?;
    let server: &mut dyn Server = if cfg.peers.is_empty() {
        &mut StandaloneServer::try_new(cfg).await?
    } else {
        &mut ClusterServer::try_new(cfg).await?
    };
    server.serve().await
}
