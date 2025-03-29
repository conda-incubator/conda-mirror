use miette::IntoDiagnostic;
use rattler_conda_types::{ChannelConfig, NamedChannelOrUrl, PackageRecord, Platform, VersionSpec};
use serde::{Deserialize, Deserializer};
use std::{env::current_dir, path::PathBuf, str::FromStr};

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

    // /// The access key ID for the S3 bucket.
    // #[arg(long, env = "S3_ACCESS_KEY_ID_SOURCE", requires_all = ["s3_secret_access_key_source"])]
    // pub s3_access_key_id_source: Option<String>,

    // /// The secret access key for the S3 bucket.
    // #[arg(long, env = "S3_SECRET_ACCESS_KEY_SOURCE", requires_all = ["s3_access_key_id_source"])]
    // pub s3_secret_access_key_source: Option<String>,

    // /// The session token for the S3 bucket.
    // #[arg(long, env = "S3_SESSION_TOKEN_SOURCE", requires_all = ["s3_access_key_id_source", "s3_secret_access_key_source"])]
    // pub s3_session_token_source: Option<String>,
    /// The access key ID for the S3 bucket.
    #[arg(long, env = "S3_ACCESS_KEY_ID_DESTINATION", requires_all = ["s3_secret_access_key_destination"])]
    pub s3_access_key_id_destination: Option<String>,

    /// The secret access key for the S3 bucket.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY_DESTINATION", requires_all = ["s3_access_key_id_destination"])]
    pub s3_secret_access_key_destination: Option<String>,

    /// The session token for the S3 bucket.
    #[arg(long, env = "S3_SESSION_TOKEN_DESTINATION", requires_all = ["s3_access_key_id_destination", "s3_secret_access_key_destination"])]
    pub s3_session_token_destination: Option<String>,

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

/* -------------------------------------------- YAML ------------------------------------------- */

#[derive(Debug, Clone)]
pub struct GlobPattern(glob::Pattern);

impl<'de> Deserialize<'de> for GlobPattern {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        glob::Pattern::from_str(&s)
            .map(GlobPattern)
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum PackageConfig {
    PackageGlob {
        name: GlobPattern,
        version: Option<VersionSpec>,
    },
    License {
        license: String,
    },
}

impl PackageConfig {
    pub(crate) fn matches(&self, package_record: PackageRecord) -> bool {
        match self {
            PackageConfig::PackageGlob { name, version } => {
                let name_match = name.0.matches(package_record.name.as_normalized());
                if let Some(version) = version {
                    name_match && version.matches(package_record.version.as_ref())
                } else {
                    name_match
                }
            }
            PackageConfig::License { license } => package_record
                .license
                .is_some_and(|l| l.as_str() == license),
        }
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

    pub include: Option<Vec<PackageConfig>>,
    pub exclude: Option<Vec<PackageConfig>>,
    pub s3_config: Option<S3ConfigSourceDest>,
}

/* -------------------------------------------- CONFIG ------------------------------------------- */

#[derive(Debug, Clone)]
pub enum MirrorMode {
    /// Mirror all packages.
    All,
    /// Mirror all packages except those matching the given patterns.
    AllButExclude(Vec<PackageConfig>),
    /// Mirror only packages matching the given patterns.
    OnlyInclude(Vec<PackageConfig>),
    /// Mirror all packages except those matching the given patterns.
    /// Override excludes with include patterns.
    IncludeExclude(Vec<PackageConfig>, Vec<PackageConfig>),
}

#[derive(Clone)]
pub struct CondaMirrorConfig {
    pub source: NamedChannelOrUrl,
    pub destination: NamedChannelOrUrl,
    pub subdirs: Option<Vec<Platform>>,
    pub mode: MirrorMode,
    pub s3_config_source: Option<S3Config>,
    pub s3_config_destination: Option<S3Config>,
    // pub s3_credentials_source: Option<S3Credentials>,
    pub s3_credentials_destination: Option<S3Credentials>,
}

impl std::fmt::Debug for CondaMirrorConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CondaMirrorConfig")
            .field("source", &self.source)
            .field("destination", &self.destination)
            .field("subdirs", &self.subdirs)
            .field("mode", &self.mode)
            .field("s3_config_source", &self.s3_config_source)
            .field("s3_config_destination", &self.s3_config_destination)
            // .field("s3_credentials_source", &{
            //     if self.s3_credentials_source.is_some() {
            //         "Some(***)"
            //     } else {
            //         "None"
            //     }
            // })
            .field("s3_credentials_destination", &{
                if self.s3_credentials_destination.is_some() {
                    "Some(***)"
                } else {
                    "None"
                }
            })
            .finish()
    }
}

impl CondaMirrorConfig {
    fn platform_url(&self, platform: Platform) -> miette::Result<Url> {
        let channel = self
            .source
            .clone()
            .into_channel(&ChannelConfig::default_with_root_dir(
                current_dir().into_diagnostic()?,
            ))
            .into_diagnostic()?;

        Ok(channel.platform_url(platform))
    }

    pub(crate) fn repodata_url(&self, platform: Platform) -> miette::Result<Url> {
        let repodata_url = self
            .platform_url(platform)?
            .join("repodata.json")
            .into_diagnostic()?;
        Ok(repodata_url)
    }

    pub(crate) fn package_url(&self, filename: &str, platform: Platform) -> miette::Result<Url> {
        let package_url = self
            .platform_url(platform)?
            .join(filename)
            .into_diagnostic()?;
        Ok(package_url)
    }
}
