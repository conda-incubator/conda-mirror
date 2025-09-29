use clap::Parser;
use miette::IntoDiagnostic;

use conda_mirror::{
    config::{
        CliConfig, CondaMirrorConfig, CondaMirrorYamlConfig, MirrorMode, S3Config, S3Credentials,
    },
    mirror,
};

/* -------------------------------------------- MAIN ------------------------------------------- */

/// The main entrypoint for the conda-mirror CLI.
#[tokio::main]
async fn main() -> miette::Result<()> {
    let cli_config = CliConfig::parse();

    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(cli_config.verbose)
        .init();

    tracing::debug!("Starting conda-mirror CLI");
    tracing::debug!("Parsed CLI options: {:?}", cli_config);

    let yaml_config = if let Some(config_path) = cli_config.config {
        let config_str = std::fs::read_to_string(config_path).into_diagnostic()?;
        serde_yml::from_str::<CondaMirrorYamlConfig>(&config_str).into_diagnostic()?
    } else {
        Default::default()
    };

    tracing::debug!("Parsed YAML configuration: {:?}", yaml_config);

    let (source, destination) = match (cli_config.source, cli_config.destination) {
        (Some(source), Some(destination)) => (source, destination),
        (None, None) => {
            if let (Some(source), Some(destination)) =
                (yaml_config.source.clone(), yaml_config.destination.clone())
            {
                (source, destination)
            } else {
                return Err(miette::miette!("Source and target must be specified"));
            }
        }
        _ => unreachable!("prevented by clap"),
    };

    let subdirs = if let Some(subdirs) = cli_config.subdir {
        Some(subdirs)
    } else {
        yaml_config.subdirs.clone()
    };

    let max_retries = if let Some(max_retries) = yaml_config.max_retries {
        max_retries
    } else {
        cli_config.max_retries
    };

    let max_parallel = if let Some(max_parallel) = yaml_config.max_parallel {
        max_parallel
    } else {
        cli_config.max_parallel
    };

    let mode = match (yaml_config.include, yaml_config.exclude) {
        (Some(include), Some(exclude)) => MirrorMode::IncludeExclude(include, exclude),
        (Some(include), None) => MirrorMode::OnlyInclude(include),
        (None, Some(exclude)) => MirrorMode::AllButExclude(exclude),
        (None, None) => MirrorMode::All,
    };

    // Resolve append mode: CLI overrides YAML (default false)
    let append = if cli_config.append {
        true
    } else {
        yaml_config.append.unwrap_or(false)
    };

    let s3_config_destination = if let (Some(endpoint_url), Some(region), Some(force_path_style)) = (
        cli_config.s3_endpoint_url_destination,
        cli_config.s3_region_destination,
        cli_config.s3_force_path_style_destination,
    ) {
        Some(S3Config {
            endpoint_url,
            region,
            force_path_style,
        })
    } else if let Some(s3_config_source_dest) = yaml_config.s3_config.clone() {
        if let Some(s3_config) = s3_config_source_dest.destination {
            Some(S3Config {
                endpoint_url: s3_config.endpoint_url,
                region: s3_config.region,
                force_path_style: s3_config.force_path_style,
            })
        } else {
            None
        }
    } else {
        None
    };
    let s3_config_source = if let (Some(endpoint_url), Some(region), Some(force_path_style)) = (
        cli_config.s3_endpoint_url_source,
        cli_config.s3_region_source,
        cli_config.s3_force_path_style_source,
    ) {
        Some(S3Config {
            endpoint_url,
            region,
            force_path_style,
        })
    } else if let Some(s3_config_source_dest) = yaml_config.s3_config {
        if let Some(s3_config) = s3_config_source_dest.source {
            Some(S3Config {
                endpoint_url: s3_config.endpoint_url,
                region: s3_config.region,
                force_path_style: s3_config.force_path_style,
            })
        } else {
            None
        }
    } else {
        None
    };

    let s3_credentials_destination = if let (Some(access_key_id), Some(secret_access_key)) = (
        cli_config.s3_access_key_id_destination,
        cli_config.s3_secret_access_key_destination,
    ) {
        Some(S3Credentials {
            access_key_id,
            secret_access_key,
            session_token: cli_config.s3_session_token_destination,
        })
    } else {
        None
    };

    let s3_credentials_source = if let (Some(access_key_id), Some(secret_access_key)) = (
        cli_config.s3_access_key_id_source,
        cli_config.s3_secret_access_key_source,
    ) {
        Some(S3Credentials {
            access_key_id,
            secret_access_key,
            session_token: cli_config.s3_session_token_source,
        })
    } else {
        None
    };

    let config = CondaMirrorConfig {
        source,
        destination,
        subdirs,
        mode,
        max_retries,
        max_parallel,
        s3_config_source,
        s3_config_destination,
        s3_credentials_source,
        s3_credentials_destination,
        append,
    };

    tracing::info!("Using configuration: {:?}", config);

    mirror(config).await
}
