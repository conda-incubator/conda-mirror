use miette::IntoDiagnostic;
use rattler_conda_types::{
    Channel, ChannelConfig, MatchSpec, NamedChannelOrUrl, ParseStrictness,
    ParseStrictnessWithNameMatcher, Platform,
};
use serde::{Deserialize, Deserializer};
use std::{env::current_dir, path::PathBuf};

use clap::Parser;
use clap_verbosity_flag::Verbosity;
use url::Url;

/* -------------------------------------------- CLI ------------------------------------------- */

/// The conda-mirror CLI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CliConfig {
    /// The source channel to mirror from.
    #[arg(long, requires_all = ["destination"])]
    pub source: Option<NamedChannelOrUrl>,

    /// The destination channel to mirror to.
    #[arg(long, requires_all = ["source"])]
    pub destination: Option<NamedChannelOrUrl>,

    /// The subdirectories to mirror.
    #[arg(long)]
    pub subdir: Option<Vec<Platform>>,

    /// The configuration file to use.
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Maximum number of retries.
    #[arg(long, default_value_t = 10)]
    pub max_retries: u8,

    /// Maximum number of concurrent connections.
    #[arg(long, default_value_t = 32)]
    pub max_parallel: u8,

    /// The S3 endpoint URL.
    #[arg(long, requires_all = ["s3_region_source", "s3_force_path_style_source"])]
    pub s3_endpoint_url_source: Option<Url>,

    /// The S3 region.
    #[arg(long, requires_all = ["s3_endpoint_url_source", "s3_force_path_style_source"])]
    pub s3_region_source: Option<String>,

    /// Whether to use path style or not in S3 requests.
    #[arg(long, requires_all = ["s3_endpoint_url_source", "s3_region_source"])]
    pub s3_force_path_style_source: Option<bool>,

    /// The S3 endpoint URL.
    #[arg(long, requires_all = ["s3_region_destination", "s3_force_path_style_destination"])]
    pub s3_endpoint_url_destination: Option<Url>,

    /// The S3 region.
    #[arg(long, requires_all = ["s3_endpoint_url_destination", "s3_force_path_style_destination"])]
    pub s3_region_destination: Option<String>,

    /// Whether to use path style or not in S3 requests.
    #[arg(long, requires_all = ["s3_endpoint_url_destination", "s3_region_destination"])]
    pub s3_force_path_style_destination: Option<bool>,

    /// The access key ID for the S3 bucket.
    #[arg(long, env = "S3_ACCESS_KEY_ID_SOURCE", requires_all = ["s3_secret_access_key_source"])]
    pub s3_access_key_id_source: Option<String>,

    /// The secret access key for the S3 bucket.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY_SOURCE", requires_all = ["s3_access_key_id_source"])]
    pub s3_secret_access_key_source: Option<String>,

    /// The session token for the S3 bucket.
    #[arg(long, env = "S3_SESSION_TOKEN_SOURCE", requires_all = ["s3_access_key_id_source", "s3_secret_access_key_source"])]
    pub s3_session_token_source: Option<String>,

    /// The access key ID for the S3 bucket.
    #[arg(long, env = "S3_ACCESS_KEY_ID_DESTINATION", requires_all = ["s3_secret_access_key_destination"])]
    pub s3_access_key_id_destination: Option<String>,

    /// The secret access key for the S3 bucket.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY_DESTINATION", requires_all = ["s3_access_key_id_destination"])]
    pub s3_secret_access_key_destination: Option<String>,

    /// The session token for the S3 bucket.
    #[arg(long, env = "S3_SESSION_TOKEN_DESTINATION", requires_all = ["s3_access_key_id_destination", "s3_secret_access_key_destination"])]
    pub s3_session_token_destination: Option<String>,

    /// Disable progress bar.
    #[arg(long, default_value_t = false)]
    pub no_progress: bool,

    // todo: add --force option
    #[command(flatten)]
    pub verbose: Verbosity,
}

#[derive(Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key_id", &"***")
            .field("secret_access_key", &"***")
            .field("session_token", &{
                if self.session_token.is_some() {
                    "Some(***)"
                } else {
                    "None"
                }
            })
            .finish()
    }
}

/* -------------------------------------------- YAML ------------------------------------------- */

// impl<'de> Deserialize<'de> for GlobPattern {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         glob::Pattern::from_str(&s)
//             .map(GlobPattern)
//             .map_err(serde::de::Error::custom)
//     }
// }

// #[derive(Debug, Clone)]
// #[repr(transparent)]
// pub struct NamelessMatchSpecWrapper(NamelessMatchSpec);

// impl<'de> Deserialize<'de> for NamelessMatchSpecWrapper {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let s = String::deserialize(deserializer)?;
//         let wrapper = NamelessMatchSpecWrapper(s.parse().map_err(serde::de::Error::custom)?);
//         tracing::trace!("Deserialized NamelessMatchSpec: {wrapper:?}");
//         Ok(wrapper)
//     }
// }

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct MatchSpecWrapper(pub MatchSpec);

impl<'de> Deserialize<'de> for MatchSpecWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let wrapper = MatchSpecWrapper(
            MatchSpec::from_str(
                &s,
                ParseStrictnessWithNameMatcher {
                    parse_strictness: ParseStrictness::Strict,
                    exact_names_only: false,
                },
            )
            .map_err(serde::de::Error::custom)?,
        );
        tracing::trace!("Deserialized MatchSpec: {wrapper:?}");
        Ok(wrapper)
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct S3Config {
    pub endpoint_url: Url,
    pub region: String,
    pub force_path_style: bool,
}

// TODO: allow setting it in .s3-config globally for both source and dest
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct S3ConfigSourceDest {
    pub source: Option<S3Config>,
    pub destination: Option<S3Config>,
}

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct CondaMirrorYamlConfig {
    pub source: Option<NamedChannelOrUrl>,
    pub destination: Option<NamedChannelOrUrl>,
    pub subdirs: Option<Vec<Platform>>,

    pub max_retries: Option<u8>,
    pub max_parallel: Option<u8>,

    pub include: Option<Vec<MatchSpecWrapper>>,
    pub exclude: Option<Vec<MatchSpecWrapper>>,
    pub s3_config: Option<S3ConfigSourceDest>,
}

/* -------------------------------------------- CONFIG ------------------------------------------- */

#[derive(Debug, Clone)]
pub enum MirrorMode {
    /// Mirror all packages.
    All,
    /// Mirror all packages except those matching the given patterns.
    AllButExclude(Vec<MatchSpec>),
    /// Mirror only packages matching the given patterns.
    OnlyInclude(Vec<MatchSpec>),
    /// Mirror all packages except those matching the given patterns.
    /// Override excludes with include patterns.
    IncludeExclude(Vec<MatchSpec>, Vec<MatchSpec>),
}

#[derive(Clone, Debug)]
pub struct CondaMirrorConfig {
    pub source: NamedChannelOrUrl,
    pub destination: NamedChannelOrUrl,
    pub subdirs: Option<Vec<Platform>>,
    pub mode: MirrorMode,
    pub max_retries: u8,
    pub max_parallel: u8,
    pub no_progress: bool,
    pub s3_config_source: Option<S3Config>,
    pub s3_config_destination: Option<S3Config>,
    pub s3_credentials_source: Option<S3Credentials>,
    pub s3_credentials_destination: Option<S3Credentials>,
    pub channel_source: Channel,
}

impl CondaMirrorConfig {
    // this needs to be refactored anyway in https://github.com/conda-incubator/conda-mirror/issues/97
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: NamedChannelOrUrl,
        destination: NamedChannelOrUrl,
        subdirs: Option<Vec<Platform>>,
        mode: MirrorMode,
        max_retries: u8,
        max_parallel: u8,
        no_progress: bool,
        s3_config_source: Option<S3Config>,
        s3_config_destination: Option<S3Config>,
        s3_credentials_source: Option<S3Credentials>,
        s3_credentials_destination: Option<S3Credentials>,
    ) -> miette::Result<Self> {
        let channel_source = source
            .clone()
            .into_channel(&ChannelConfig::default_with_root_dir(
                current_dir().into_diagnostic()?,
            ))
            .into_diagnostic()?;
        Ok(Self {
            source,
            destination,
            subdirs,
            mode,
            max_retries,
            max_parallel,
            no_progress,
            s3_config_source,
            s3_config_destination,
            s3_credentials_source,
            s3_credentials_destination,
            channel_source,
        })
    }

    fn platform_url(&self, platform: Platform) -> Url {
        self.channel_source.platform_url(platform)
    }

    pub(crate) fn repodata_url(&self, platform: Platform) -> Url {
        self.platform_url(platform)
            .join("repodata.json")
            .expect("repodata.json can be joined")
    }

    pub(crate) fn package_url(
        &self,
        filename: &str,
        platform: Platform,
    ) -> Result<Url, url::ParseError> {
        self.platform_url(platform).join(filename)
    }
}
