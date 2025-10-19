use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    EnvFilter, fmt,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

fn get_default_loglevel(verbose: u8, quiet: u8) -> LevelFilter {
    match (verbose, quiet) {
        (1, _) => LevelFilter::DEBUG,
        (x, _) if x >= 2 => LevelFilter::TRACE,
        (_, 1) => LevelFilter::WARN,
        (_, 2) => LevelFilter::ERROR,
        (_, x) if x >= 3 => LevelFilter::OFF,
        _ => LevelFilter::INFO,
    }
}

pub fn set_up_logging(verbose: u8, quiet: u8) -> Result<(), TryInitError> {
    let filter = EnvFilter::builder()
        .with_default_directive(get_default_loglevel(verbose, quiet).into())
        .from_env_lossy();

    // Use the tracing subscriber `Registry`, or any other subscriber
    // that impls `LookupSpan`
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::Layer::default())
        .try_init()
}
