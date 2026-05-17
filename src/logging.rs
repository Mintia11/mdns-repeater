use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init() {
    // LOG_FORMAT=json  → structured JSON (for log aggregators, Loki, etc.)
    // LOG_FORMAT=pretty → human-readable with colours (default for local dev)
    // LOG_LEVEL=debug/info/warn/error  (default: info)
    let format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".into());
    let filter = EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));

    match format.as_str() {
        "json" => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().json().with_current_span(true))
                .init();
        }
        _ => {
            tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().with_target(false))
                .init();
        }
    }
}
