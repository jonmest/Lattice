use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::ObservabilityConfig;

pub fn init_tracing(config: &ObservabilityConfig) -> Result<(), Box<dyn std::error::Error>> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();

    if config.enable_tracing {
        // Enable structured tracing
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_target(true))
            .init();

        if let Some(endpoint) = &config.otlp_endpoint {
            println!("Tracing enabled with OTLP endpoint: {}", endpoint);
            println!("Note: OTLP export not yet fully integrated, using local logging");
        } else {
            println!("Tracing enabled (local logging only)");
        }
    } else {
        // Basic console logging
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    Ok(())
}
