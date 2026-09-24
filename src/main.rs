use clap::Parser;
use miette::IntoDiagnostic;

use conda_mirror::{
    config::{
        CliConfig, CondaMirrorConfig, CondaMirrorYamlConfig, MirrorMode, S3Config, S3Credentials,
    },
    mirror,
};

/* -------------------------------------------- MAIN ------------------------------------------- */

/// Merge the S3 settings from the CLI with the ones from the configuration file,
/// preferring the CLI. Returns `None` if nothing is configured at all.
fn merge_s3_config(cli: S3Config, yaml: Option<S3Config>) -> Option<S3Config> {
    let merged = cli.merge(yaml.unwrap_or_default());
    (!merged.is_empty()).then_some(merged)
}

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

    let precondition_checks = cli_config
        .precondition_checks
        .or(yaml_config.precondition_checks)
        .unwrap_or(true);

    let mode = match (yaml_config.include, yaml_config.exclude) {
        (Some(include), Some(exclude)) => MirrorMode::IncludeExclude(
            include.into_iter().map(|spec| spec.0).collect(),
            exclude.into_iter().map(|spec| spec.0).collect(),
        ),
        (Some(include), None) => {
            MirrorMode::OnlyInclude(include.into_iter().map(|spec| spec.0).collect())
        }
        (None, Some(exclude)) => {
            MirrorMode::AllButExclude(exclude.into_iter().map(|spec| spec.0).collect())
        }
        (None, None) => MirrorMode::All,
    };

    // The CLI takes precedence over the configuration file, per setting. Anything
    // that is left unset is resolved through the AWS SDK.
    let s3_config_destination = merge_s3_config(
        S3Config {
            endpoint_url: cli_config.s3_endpoint_url_destination,
            region: cli_config.s3_region_destination,
            force_path_style: cli_config.s3_force_path_style_destination,
        },
        yaml_config
            .s3_config
            .clone()
            .and_then(|s3_config| s3_config.destination),
    );
    let s3_config_source = merge_s3_config(
        S3Config {
            endpoint_url: cli_config.s3_endpoint_url_source,
            region: cli_config.s3_region_source,
            force_path_style: cli_config.s3_force_path_style_source,
        },
        yaml_config.s3_config.and_then(|s3_config| s3_config.source),
    );

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
    let config = CondaMirrorConfig::new(
        source,
        destination,
        subdirs,
        mode,
        max_retries,
        max_parallel,
        cli_config.no_progress,
        s3_config_source,
        s3_config_destination,
        s3_credentials_source,
        s3_credentials_destination,
        precondition_checks,
    )?;

    tracing::info!("Using configuration: {:?}", config);

    mirror(config).await
}
