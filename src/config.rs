use jiff::{Timestamp, civil::Date, tz::TimeZone};
use miette::IntoDiagnostic;
use rattler_conda_types::{
    Channel, ChannelConfig, MatchSpec, NamedChannelOrUrl, PackageRecord, ParseStrictness,
    ParseStrictnessWithNameMatcher, Subdir, utils::TimestampMs,
};
use serde::{Deserialize, Deserializer};
use std::{env::current_dir, path::PathBuf, str::FromStr};
use thiserror::Error;

use clap::{Parser, ValueEnum};
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
    pub subdir: Option<Vec<Subdir>>,

    /// The configuration file to use.
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Maximum number of retries.
    #[arg(long, default_value_t = 10)]
    pub max_retries: u8,

    /// Maximum number of concurrent connections.
    #[arg(long, default_value_t = 32)]
    pub max_parallel: u8,

    /// The S3 endpoint URL. Resolved through the AWS SDK if not set.
    #[arg(long, requires_all = ["s3_region_source", "s3_force_path_style_source"])]
    pub s3_endpoint_url_source: Option<Url>,

    /// The S3 region. Resolved through the AWS SDK if not set.
    #[arg(long, requires_all = ["s3_endpoint_url_source", "s3_force_path_style_source"])]
    pub s3_region_source: Option<String>,

    /// Whether to use path style or not in S3 requests. Resolved through the AWS
    /// SDK if not set.
    #[arg(long, requires_all = ["s3_endpoint_url_source", "s3_region_source"])]
    pub s3_force_path_style_source: Option<bool>,

    /// The S3 endpoint URL. Resolved through the AWS SDK if not set.
    #[arg(long, requires_all = ["s3_region_destination", "s3_force_path_style_destination"])]
    pub s3_endpoint_url_destination: Option<Url>,

    /// The S3 region. Resolved through the AWS SDK if not set.
    #[arg(long, requires_all = ["s3_endpoint_url_destination", "s3_force_path_style_destination"])]
    pub s3_region_destination: Option<String>,

    /// Whether to use path style or not in S3 requests. Resolved through the AWS
    /// SDK if not set.
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

    /// Enable precondition checks when uploading repodata files.
    #[arg(long)]
    pub precondition_checks: Option<bool>,

    /// Exclude packages whose timestamp is after this cutoff. Accepts a
    /// duration relative to now (7d, 1h30m), a date (2025-10-01, which
    /// includes the whole day in UTC) or an RFC 3339 timestamp
    /// (2025-10-01T00:00:00Z). Same formats as pixi's `exclude-newer`.
    #[arg(long, value_name = "CUTOFF")]
    pub exclude_newer: Option<TimeCutoff>,

    /// Exclude packages whose timestamp is before this cutoff. Accepts the
    /// same formats as --exclude-newer.
    #[arg(long, value_name = "CUTOFF")]
    pub exclude_older: Option<TimeCutoff>,

    /// Which timestamp to compare against the cutoffs and what to do with
    /// packages that do not have one.
    #[arg(long, value_enum)]
    pub timestamp_policy: Option<TimestampPolicy>,
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

/* ----------------------------------------- TIME FILTER ---------------------------------------- */

/// Which timestamp of a package record is compared against the cutoffs and
/// what happens to records that do not have one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TimestampPolicy {
    /// Prefer `indexed_timestamp` (CEP 47), fall back to `timestamp`. Records
    /// with neither pass the time filter.
    AllowMissing,
    /// Prefer `indexed_timestamp` (CEP 47), fall back to `timestamp`. Records
    /// with neither are excluded.
    #[default]
    RequireTimestamp,
    /// Only use `indexed_timestamp` (CEP 47). Records without it are excluded.
    RequireIndexedTimestamp,
}

impl std::fmt::Display for TimestampPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            TimestampPolicy::AllowMissing => "allow-missing",
            TimestampPolicy::RequireTimestamp => "require-timestamp",
            TimestampPolicy::RequireIndexedTimestamp => "require-indexed-timestamp",
        };
        f.write_str(name)
    }
}

/// Error returned when a cutoff is in none of the supported formats.
#[derive(Debug, Error)]
#[error(
    "`{input}` is neither a valid duration ({duration}), date ({date}), nor timestamp ({timestamp})"
)]
pub struct ParseTimeCutoffError {
    input: String,
    duration: humantime::DurationError,
    date: jiff::Error,
    timestamp: jiff::Error,
}

/// Which bound of the time window a cutoff is resolved for. This matters for
/// date-only cutoffs, which always include the whole day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CutoffBound {
    /// The cutoff is an `exclude-newer` bound.
    ExcludeNewer,
    /// The cutoff is an `exclude-older` bound.
    ExcludeOlder,
}

/// A point in time as written by the user, in the same formats pixi accepts
/// for `exclude-newer`. Use [`TimeCutoff::resolve`] to turn it into an
/// instant.
#[derive(Debug, Clone, Copy)]
pub enum TimeCutoff {
    /// A duration before "now", e.g. `7d` or `1h30m`.
    Duration(std::time::Duration),
    /// A calendar date in UTC. The whole day is inside the window.
    Date(Date),
    /// An absolute instant.
    Timestamp(Timestamp),
}

impl FromStr for TimeCutoff {
    type Err = ParseTimeCutoffError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let duration = match humantime::parse_duration(s) {
            Ok(duration) => return Ok(Self::Duration(duration)),
            Err(err) => err,
        };
        // The timestamp is tried before the date because jiff's date parser
        // accepts a full timestamp and silently drops the time part.
        let timestamp = match Timestamp::from_str(s) {
            Ok(timestamp) => return Ok(Self::Timestamp(timestamp)),
            Err(err) => err,
        };
        let date = match Date::from_str(s) {
            Ok(date) => return Ok(Self::Date(date)),
            Err(err) => err,
        };
        Err(ParseTimeCutoffError {
            input: s.to_owned(),
            duration,
            date,
            timestamp,
        })
    }
}

impl<'de> Deserialize<'de> for TimeCutoff {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl TimeCutoff {
    /// Resolve the cutoff to an instant. Durations are subtracted from `now`.
    /// A date resolves to the start of the following day for
    /// [`CutoffBound::ExcludeNewer`] (like pixi) and to the start of the day
    /// itself for [`CutoffBound::ExcludeOlder`], so the day is always inside
    /// the window.
    pub fn resolve(self, now: Timestamp, bound: CutoffBound) -> Result<Timestamp, jiff::Error> {
        match self {
            Self::Duration(duration) => now.checked_sub(duration),
            Self::Date(date) => {
                let date = match bound {
                    CutoffBound::ExcludeNewer => date.tomorrow()?,
                    CutoffBound::ExcludeOlder => date,
                };
                Ok(date.to_zoned(TimeZone::UTC)?.timestamp())
            }
            Self::Timestamp(timestamp) => Ok(timestamp),
        }
    }
}

/// Resolved time window that packages must fall into to be mirrored. Both
/// bounds are inclusive.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeFilter {
    pub exclude_newer: Option<Timestamp>,
    pub exclude_older: Option<Timestamp>,
    pub policy: TimestampPolicy,
}

impl TimeFilter {
    /// Resolve the user-supplied cutoffs against `now` and validate them.
    pub fn new(
        exclude_newer: Option<TimeCutoff>,
        exclude_older: Option<TimeCutoff>,
        policy: Option<TimestampPolicy>,
        now: Timestamp,
    ) -> miette::Result<Self> {
        let exclude_newer = exclude_newer
            .map(|cutoff| cutoff.resolve(now, CutoffBound::ExcludeNewer))
            .transpose()
            .into_diagnostic()?;
        let exclude_older = exclude_older
            .map(|cutoff| cutoff.resolve(now, CutoffBound::ExcludeOlder))
            .transpose()
            .into_diagnostic()?;
        if let (Some(older), Some(newer)) = (exclude_older, exclude_newer)
            && older > newer
        {
            return Err(miette::miette!(
                "exclude-older ({older}) is after exclude-newer ({newer}), no package can match"
            ));
        }
        let filter = Self {
            exclude_newer,
            exclude_older,
            policy: policy.unwrap_or_default(),
        };
        if policy.is_some() && !filter.is_active() {
            tracing::warn!(
                "timestamp-policy is set but neither exclude-newer nor exclude-older is, it has no effect"
            );
        }
        Ok(filter)
    }

    /// Whether any cutoff is configured.
    pub fn is_active(&self) -> bool {
        self.exclude_newer.is_some() || self.exclude_older.is_some()
    }

    /// The timestamp of `record` that is compared against the cutoffs under
    /// the configured policy.
    pub fn effective_timestamp(&self, record: &PackageRecord) -> Option<Timestamp> {
        let timestamp = match self.policy {
            TimestampPolicy::RequireIndexedTimestamp => record.indexed_timestamp,
            TimestampPolicy::AllowMissing | TimestampPolicy::RequireTimestamp => {
                record.indexed_timestamp.or(record.timestamp)
            }
        };
        timestamp.as_ref().map(TimestampMs::jiff_timestamp)
    }

    /// Whether `record` falls inside the time window. Records without a usable
    /// timestamp are only included under [`TimestampPolicy::AllowMissing`].
    pub fn includes(&self, record: &PackageRecord) -> bool {
        if !self.is_active() {
            return true;
        }
        match self.effective_timestamp(record) {
            None => self.policy == TimestampPolicy::AllowMissing,
            Some(timestamp) => {
                self.exclude_newer.is_none_or(|newer| timestamp <= newer)
                    && self.exclude_older.is_none_or(|older| timestamp >= older)
            }
        }
    }
}

/// The S3 settings for one side of the mirror.
///
/// These settings are all-or-nothing: either all of them are configured, or none
/// of them are and everything is resolved through the AWS SDK (environment
/// variables, `~/.aws/config`, SSO, instance metadata, ...).
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
    pub subdirs: Option<Vec<Subdir>>,

    pub max_retries: Option<u8>,
    pub max_parallel: Option<u8>,

    pub include: Option<Vec<MatchSpecWrapper>>,
    pub exclude: Option<Vec<MatchSpecWrapper>>,
    pub s3_config: Option<S3ConfigSourceDest>,
    pub precondition_checks: Option<bool>,

    pub exclude_newer: Option<TimeCutoff>,
    pub exclude_older: Option<TimeCutoff>,
    pub timestamp_policy: Option<TimestampPolicy>,
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
    pub subdirs: Option<Vec<Subdir>>,
    pub mode: MirrorMode,
    pub max_retries: u8,
    pub max_parallel: u8,
    pub no_progress: bool,
    pub s3_config_source: Option<S3Config>,
    pub s3_config_destination: Option<S3Config>,
    pub s3_credentials_source: Option<S3Credentials>,
    pub s3_credentials_destination: Option<S3Credentials>,
    pub channel_source: Channel,
    pub precondition_checks: bool,
    pub time_filter: TimeFilter,
}

impl CondaMirrorConfig {
    // this needs to be refactored anyway in https://github.com/conda-incubator/conda-mirror/issues/97
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: NamedChannelOrUrl,
        destination: NamedChannelOrUrl,
        subdirs: Option<Vec<Subdir>>,
        mode: MirrorMode,
        max_retries: u8,
        max_parallel: u8,
        no_progress: bool,
        s3_config_source: Option<S3Config>,
        s3_config_destination: Option<S3Config>,
        s3_credentials_source: Option<S3Credentials>,
        s3_credentials_destination: Option<S3Credentials>,
        precondition_checks: bool,
        time_filter: TimeFilter,
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
            precondition_checks,
            time_filter,
        })
    }

    fn platform_url(&self, platform: Subdir) -> Url {
        self.channel_source.platform_url(platform)
    }

    pub(crate) fn repodata_url(&self, platform: Subdir) -> Url {
        self.platform_url(platform)
            .join("repodata.json")
            .expect("repodata.json can be joined")
    }

    pub(crate) fn package_url(
        &self,
        filename: &str,
        platform: Subdir,
    ) -> Result<Url, url::ParseError> {
        self.platform_url(platform).join(filename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn utc(year: i16, month: i8, day: i8) -> Timestamp {
        jiff::civil::date(year, month, day)
            .to_zoned(TimeZone::UTC)
            .unwrap()
            .timestamp()
    }

    fn now() -> Timestamp {
        utc(2026, 3, 1)
    }

    fn resolve(s: &str, bound: CutoffBound) -> Timestamp {
        s.parse::<TimeCutoff>()
            .unwrap()
            .resolve(now(), bound)
            .unwrap()
    }

    fn resolve_newer(s: &str) -> Timestamp {
        resolve(s, CutoffBound::ExcludeNewer)
    }

    fn resolve_older(s: &str) -> Timestamp {
        resolve(s, CutoffBound::ExcludeOlder)
    }

    #[test]
    fn parses_rfc3339() {
        assert!(matches!(
            "2025-10-01T00:00:00Z".parse::<TimeCutoff>().unwrap(),
            TimeCutoff::Timestamp(_)
        ));
        assert_eq!(resolve_newer("2025-10-01T00:00:00Z"), utc(2025, 10, 1));
        assert_eq!(resolve_older("2025-10-01T00:00:00Z"), utc(2025, 10, 1));
        assert!(matches!(
            "2025-10-01T02:00:00+02:00".parse::<TimeCutoff>().unwrap(),
            TimeCutoff::Timestamp(_)
        ));
        assert_eq!(resolve_newer("2025-10-01T02:00:00+02:00"), utc(2025, 10, 1));
        assert_eq!(
            resolve_newer("2025-10-01T12:00:00Z"),
            utc(2025, 10, 1)
                .checked_add(std::time::Duration::from_secs(12 * 3600))
                .unwrap()
        );
    }

    #[test]
    fn parses_date_including_the_whole_day() {
        assert!(matches!(
            "2025-10-01".parse::<TimeCutoff>().unwrap(),
            TimeCutoff::Date(_)
        ));
        // Like pixi: `exclude-newer: 2025-10-01` keeps everything from that day.
        assert_eq!(resolve_newer("2025-10-01"), utc(2025, 10, 2));
        assert_eq!(resolve_older("2025-10-01"), utc(2025, 10, 1));
    }

    #[test]
    fn parses_humantime_durations() {
        assert!(matches!(
            "14d".parse::<TimeCutoff>().unwrap(),
            TimeCutoff::Duration(_)
        ));
        assert_eq!(resolve_newer("14d"), utc(2026, 2, 15));
        assert_eq!(resolve_older("14d"), utc(2026, 2, 15));
        assert_eq!(resolve_newer("2 weeks"), utc(2026, 2, 15));
        assert_eq!(resolve_newer("7days"), utc(2026, 2, 22));
        assert_eq!(
            resolve_newer("1h30m"),
            now()
                .checked_sub(std::time::Duration::from_secs(90 * 60))
                .unwrap()
        );
        assert_eq!(resolve_newer("0d"), now());
    }

    #[test]
    fn rejects_invalid_cutoffs() {
        for input in [
            "",
            "yesterday",
            "2025-13-01",
            "14",
            "-3d",
            "P14D",
            "14d ago",
        ] {
            let err = input.parse::<TimeCutoff>().unwrap_err().to_string();
            assert!(err.contains("neither a valid duration"), "{input:?}: {err}");
        }
    }

    #[test]
    fn time_filter_defaults() {
        let filter = TimeFilter::new(None, None, None, now()).unwrap();
        assert!(!filter.is_active());
        assert_eq!(filter.policy, TimestampPolicy::RequireTimestamp);
    }

    #[test]
    fn time_filter_rejects_inverted_window() {
        let newer = "2024-01-01".parse().unwrap();
        let older = "2025-01-01".parse().unwrap();
        assert!(TimeFilter::new(Some(newer), Some(older), None, now()).is_err());
        // The same date on both bounds is a valid one-day window.
        let filter = TimeFilter::new(Some(newer), Some(newer), None, now()).unwrap();
        assert_eq!(filter.exclude_older, Some(utc(2024, 1, 1)));
        assert_eq!(filter.exclude_newer, Some(utc(2024, 1, 2)));
    }

    #[test]
    fn yaml_accepts_unquoted_dates_and_durations() {
        let config: CondaMirrorYamlConfig = serde_yml::from_str(
            "exclude-newer: 14d\nexclude-older: 2025-10-01\ntimestamp-policy: allow-missing\n",
        )
        .unwrap();
        let filter = TimeFilter::new(
            config.exclude_newer,
            config.exclude_older,
            config.timestamp_policy,
            now(),
        )
        .unwrap();
        assert_eq!(filter.exclude_newer, Some(utc(2026, 2, 15)));
        assert_eq!(filter.exclude_older, Some(utc(2025, 10, 1)));
        assert_eq!(filter.policy, TimestampPolicy::AllowMissing);
    }

    #[test]
    fn yaml_accepts_every_policy() {
        for (input, expected) in [
            ("allow-missing", TimestampPolicy::AllowMissing),
            ("require-timestamp", TimestampPolicy::RequireTimestamp),
            (
                "require-indexed-timestamp",
                TimestampPolicy::RequireIndexedTimestamp,
            ),
        ] {
            let config: CondaMirrorYamlConfig =
                serde_yml::from_str(&format!("timestamp-policy: {input}\n")).unwrap();
            assert_eq!(config.timestamp_policy, Some(expected));
        }
    }

    #[test]
    fn yaml_rejects_unknown_policy_and_keys() {
        assert!(
            serde_yml::from_str::<CondaMirrorYamlConfig>("timestamp-policy: whatever\n").is_err()
        );
        assert!(
            serde_yml::from_str::<CondaMirrorYamlConfig>("exclude-newer: yesterday\n").is_err()
        );
        assert!(serde_yml::from_str::<CondaMirrorYamlConfig>("exclude_newer: 14d\n").is_err());
    }

    #[test]
    fn cli_parses_time_flags() {
        let cli = CliConfig::try_parse_from([
            "conda-mirror",
            "--exclude-newer",
            "14d",
            "--exclude-older",
            "2023-01-01",
            "--timestamp-policy",
            "require-indexed-timestamp",
        ])
        .unwrap();
        assert_eq!(
            cli.exclude_newer
                .unwrap()
                .resolve(now(), CutoffBound::ExcludeNewer)
                .unwrap(),
            utc(2026, 2, 15)
        );
        assert_eq!(
            cli.exclude_older
                .unwrap()
                .resolve(now(), CutoffBound::ExcludeOlder)
                .unwrap(),
            utc(2023, 1, 1)
        );
        assert_eq!(
            cli.timestamp_policy,
            Some(TimestampPolicy::RequireIndexedTimestamp)
        );

        assert!(
            CliConfig::try_parse_from(["conda-mirror", "--exclude-newer", "yesterday"]).is_err()
        );
        assert!(
            CliConfig::try_parse_from(["conda-mirror", "--timestamp-policy", "bogus"]).is_err()
        );
    }
}
