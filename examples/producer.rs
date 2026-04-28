use std::time::Duration;

use anyhow::{bail, Context, Result};
use rs_observer::config::AppConfig;
use rs_observer::producer;

#[tokio::main]
async fn main() -> Result<()> {
    let (config_path, interval) = args()?;
    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config file `{config_path}`"))?;
    let config = AppConfig::from_yaml(&contents)?;
    producer::run_loop(config, interval).await
}

fn args() -> Result<(String, Duration)> {
    let mut config_path = None;
    let mut interval = Duration::from_millis(500);
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(args.next().context("missing value after `--config`")?);
            }
            "--interval-ms" => {
                let value = args.next().context("missing value after `--interval-ms`")?;
                interval = Duration::from_millis(value.parse()?);
            }
            _ => bail!("usage: cargo run --example producer -- --config <path> [--interval-ms N]"),
        }
    }

    Ok((
        config_path
            .context("usage: cargo run --example producer -- --config <path> [--interval-ms N]")?,
        interval,
    ))
}
