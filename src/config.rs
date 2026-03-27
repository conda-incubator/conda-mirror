use chrono::{DateTime, NaiveDate, Utc};
use miette::IntoDiagnostic;
use rattler_conda_types::{
    Channel, ChannelConfig, Matches, MatchSpec, NamedChannelOrUrl, PackageRecord, ParseStrictness,
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

    /// Enable precondition checks when uploading repodata files.
    #[arg(long)]
    pub precondition_checks: Option<bool>,
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

/// Parse a datetime string in one of the supported formats:
/// - ISO 8601 with timezone: `"2025-10-01T00:00:00Z"`
/// - Date only (`YYYY-MM-DD`): `"2025-10-01"` (treated as midnight UTC)
/// - Relative duration: `"14d"` (14 days ago from now)
pub fn parse_datetime(s: &str) -> Result<DateTime<Utc>, String> {
    // Try full RFC 3339 / ISO 8601 datetime with timezone
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try date-only format YYYY-MM-DD (treat as midnight UTC)
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if let Some(dt) = date.and_hms_opt(0, 0, 0) {
            return Ok(dt.and_utc());
        }
    }

    // Try relative duration suffix: e.g. "14d" = 14 days ago
    if let Some(n) = s.strip_suffix('d').and_then(|n| n.parse::<i64>().ok()) {
        return Ok(Utc::now() - chrono::Duration::days(n));
    }

    Err(format!(
        "invalid datetime {s:?}: expected RFC 3339 (e.g. \"2025-01-01T00:00:00Z\"), \
         a date (e.g. \"2025-01-01\"), or a relative duration (e.g. \"14d\")"
    ))
}

fn parse_matchspec_str<E: serde::de::Error>(s: &str) -> Result<MatchSpec, E> {
    MatchSpec::from_str(
        s,
        ParseStrictnessWithNameMatcher {
            parse_strictness: ParseStrictness::Strict,
            exact_names_only: false,
        },
    )
    .map_err(E::custom)
}

/// A package filter combining a [`MatchSpec`] with optional timestamp bounds.
///
/// Supports two YAML representations:
///
/// 1. A plain string MatchSpec:
///    ```yaml
///    include:
///      - "python >=3.9"
///      - "jupyter*[license=MIT]"
///    ```
///
/// 2. An object with an explicit `matchspec` key plus optional timestamp bounds:
///    ```yaml
///    include:
///      - matchspec: "python*"
///        exclude-newer: "2025-10-01T00:00:00Z"
///        exclude-older: "2023-01-01"
///    ```
#[derive(Debug, Clone)]
pub struct PackageFilter {
    pub matchspec: MatchSpec,
    /// Exclude packages whose build timestamp is **strictly after** this datetime.
    pub exclude_newer: Option<DateTime<Utc>>,
    /// Exclude packages whose build timestamp is **strictly before** this datetime.
    pub exclude_older: Option<DateTime<Utc>>,
}

impl PackageFilter {
    /// Returns `true` if the package satisfies both the MatchSpec and any
    /// configured timestamp bounds.
    ///
    /// Packages whose `timestamp` field is absent are always considered a match
    /// with respect to the timestamp bounds (conservative / include-unknown approach).
    pub fn matches(&self, pkg: &PackageRecord) -> bool {
        if !self.matchspec.matches(pkg) {
            return false;
        }
        if let Some(ref ts) = pkg.timestamp {
            if let Some(ref exclude_newer) = self.exclude_newer {
                if ts > exclude_newer {
                    return false;
                }
            }
            if let Some(ref exclude_older) = self.exclude_older {
                if ts < exclude_older {
                    return false;
                }
            }
        }
        true
    }
}

// Internal helper struct for the object form of a PackageFilter in YAML.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct PackageFilterObject {
    matchspec: String,
    exclude_newer: Option<String>,
    exclude_older: Option<String>,
}

// Two-way untagged: a bare string OR an object with matchspec + optional bounds.
#[derive(Deserialize)]
#[serde(untagged)]
enum PackageFilterRaw {
    String(String),
    Object(PackageFilterObject),
}

impl<'de> Deserialize<'de> for PackageFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = PackageFilterRaw::deserialize(deserializer)?;

        match raw {
            PackageFilterRaw::String(s) => {
                tracing::trace!("Deserializing PackageFilter from string: {s:?}");
                let matchspec = parse_matchspec_str(&s)?;
                Ok(PackageFilter {
                    matchspec,
                    exclude_newer: None,
                    exclude_older: None,
                })
            }
            PackageFilterRaw::Object(obj) => {
                tracing::trace!(
                    "Deserializing PackageFilter from object: matchspec={:?}",
                    obj.matchspec
                );
                let matchspec = parse_matchspec_str(&obj.matchspec)?;
                let exclude_newer = obj
                    .exclude_newer
                    .as_deref()
                    .map(parse_datetime)
                    .transpose()
                    .map_err(serde::de::Error::custom)?;
                let exclude_older = obj
                    .exclude_older
                    .as_deref()
                    .map(parse_datetime)
                    .transpose()
                    .map_err(serde::de::Error::custom)?;
                Ok(PackageFilter {
                    matchspec,
                    exclude_newer,
                    exclude_older,
                })
            }
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

    pub max_retries: Option<u8>,
    pub max_parallel: Option<u8>,

    pub include: Option<Vec<PackageFilter>>,
    pub exclude: Option<Vec<PackageFilter>>,
    pub s3_config: Option<S3ConfigSourceDest>,
    pub precondition_checks: Option<bool>,
}

/* -------------------------------------------- CONFIG ------------------------------------------- */

#[derive(Debug, Clone)]
pub enum MirrorMode {
    /// Mirror all packages.
    All,
    /// Mirror all packages except those matching the given patterns.
    AllButExclude(Vec<PackageFilter>),
    /// Mirror only packages matching the given patterns.
    OnlyInclude(Vec<PackageFilter>),
    /// Mirror all packages except those matching the given patterns.
    /// Override excludes with include patterns.
    IncludeExclude(Vec<PackageFilter>, Vec<PackageFilter>),
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
    pub precondition_checks: bool,
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
        precondition_checks: bool,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rattler_conda_types::{PackageName, Version};
    use std::str::FromStr;

    fn make_record(name: &str, timestamp_ms: Option<i64>) -> PackageRecord {
        use rattler_conda_types::utils::TimestampMs;
        let mut record = PackageRecord::new(
            PackageName::new_unchecked(name),
            Version::from_str("1.0.0").unwrap(),
            "0".to_string(),
        );
        record.timestamp = timestamp_ms.map(|ms| {
            let dt = DateTime::from_timestamp_millis(ms).expect("valid timestamp");
            TimestampMs::from(dt)
        });
        record
    }

    fn filter(spec: &str) -> PackageFilter {
        serde_yml::from_str(spec).expect("valid PackageFilter")
    }

    // ── parse_datetime ──────────────────────────────────────────────────────

    #[test]
    fn test_parse_datetime_rfc3339() {
        let dt = parse_datetime("2025-01-01T00:00:00Z").unwrap();
        assert_eq!(dt, Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_parse_datetime_date_only() {
        let dt = parse_datetime("2025-01-01").unwrap();
        assert_eq!(dt, Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_parse_datetime_relative_days() {
        let now = Utc::now();
        let dt = parse_datetime("7d").unwrap();
        let expected = now - chrono::Duration::days(7);
        // Allow a 5-second tolerance for test execution time.
        let diff_secs = (dt - expected).num_seconds().abs();
        assert!(
            diff_secs <= 5,
            "Expected timestamp within 5s of 7 days ago, got diff={diff_secs}s"
        );
    }

    #[test]
    fn test_parse_datetime_invalid() {
        assert!(parse_datetime("not-a-date").is_err());
        assert!(parse_datetime("14w").is_err()); // "w" weeks not supported
    }

    // ── PackageFilter deserialization ───────────────────────────────────────

    #[test]
    fn test_deserialize_string_form() {
        let f: PackageFilter = serde_yml::from_str("\"python >=3.9\"").unwrap();
        assert!(f.exclude_newer.is_none());
        assert!(f.exclude_older.is_none());
    }

    #[test]
    fn test_deserialize_object_form() {
        let yaml = r#"
matchspec: "python*"
exclude-newer: "2025-10-01T00:00:00Z"
exclude-older: "2023-01-01"
"#;
        let f: PackageFilter = serde_yml::from_str(yaml).unwrap();
        assert!(f.exclude_newer.is_some());
        assert!(f.exclude_older.is_some());
        let newer = f.exclude_newer.unwrap();
        assert_eq!(newer, Utc.with_ymd_and_hms(2025, 10, 1, 0, 0, 0).unwrap());
        let older = f.exclude_older.unwrap();
        assert_eq!(older, Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_deserialize_object_unknown_field_rejected() {
        let yaml = r#"
matchspec: "python*"
unknown-field: "value"
"#;
        let result: Result<PackageFilter, _> = serde_yml::from_str(yaml);
        assert!(result.is_err());
    }

    // ── PackageFilter::matches ──────────────────────────────────────────────

    #[test]
    fn test_matches_name_only() {
        let f = filter("\"python*\"");
        assert!(f.matches(&make_record("python", None)));
        assert!(f.matches(&make_record("python-3.9", None)));
        assert!(!f.matches(&make_record("numpy", None)));
    }

    #[test]
    fn test_matches_no_timestamp_always_passes_bounds() {
        // A package without a timestamp should pass regardless of bounds.
        let yaml = r#"
matchspec: "*"
exclude-newer: "2020-01-01T00:00:00Z"
exclude-older: "2030-01-01T00:00:00Z"
"#;
        let f: PackageFilter = serde_yml::from_str(yaml).unwrap();
        assert!(f.matches(&make_record("anything", None)));
    }

    #[test]
    fn test_matches_exclude_newer() {
        // exclude_newer = 2025-01-01: include packages built on or before 2025-01-01
        let yaml = "matchspec: \"*\"\nexclude-newer: \"2025-01-01T00:00:00Z\"\n";
        let f: PackageFilter = serde_yml::from_str(yaml).unwrap();

        let cutoff_ms = Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();

        // Exactly at the cutoff – should be included (not strictly after)
        assert!(f.matches(&make_record("pkg", Some(cutoff_ms))));
        // One millisecond after the cutoff – excluded
        assert!(!f.matches(&make_record("pkg", Some(cutoff_ms + 1))));
        // Before the cutoff – included
        assert!(f.matches(&make_record("pkg", Some(cutoff_ms - 1_000))));
    }

    #[test]
    fn test_matches_exclude_older() {
        // exclude_older = 2023-01-01: include packages built on or after 2023-01-01
        let yaml = "matchspec: \"*\"\nexclude-older: \"2023-01-01T00:00:00Z\"\n";
        let f: PackageFilter = serde_yml::from_str(yaml).unwrap();

        let cutoff_ms = Utc
            .with_ymd_and_hms(2023, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();

        // Exactly at the cutoff – included (not strictly before)
        assert!(f.matches(&make_record("pkg", Some(cutoff_ms))));
        // One millisecond before the cutoff – excluded
        assert!(!f.matches(&make_record("pkg", Some(cutoff_ms - 1))));
        // After the cutoff – included
        assert!(f.matches(&make_record("pkg", Some(cutoff_ms + 1_000))));
    }

    #[test]
    fn test_matches_both_bounds() {
        let yaml = "matchspec: \"python*\"\nexclude-newer: \"2025-01-01T00:00:00Z\"\nexclude-older: \"2023-01-01T00:00:00Z\"\n";
        let f: PackageFilter = serde_yml::from_str(yaml).unwrap();

        let ts_in_range = Utc
            .with_ymd_and_hms(2024, 6, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let ts_too_old = Utc
            .with_ymd_and_hms(2022, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let ts_too_new = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();

        assert!(f.matches(&make_record("python", Some(ts_in_range))));
        assert!(!f.matches(&make_record("python", Some(ts_too_old))));
        assert!(!f.matches(&make_record("python", Some(ts_too_new))));
        // Wrong name: never matches
        assert!(!f.matches(&make_record("numpy", Some(ts_in_range))));
    }

    // ── CondaMirrorYamlConfig deserialization ───────────────────────────────

    #[test]
    fn test_yaml_config_with_timestamp_filter() {
        let yaml = r#"
source: conda-forge
destination: ./mirror

include:
  - "jupyter*[license=MIT]"
  - matchspec: "python*"
    exclude-newer: "2025-10-01T00:00:00Z"
    exclude-older: "2023-01-01"
"#;
        let config: CondaMirrorYamlConfig = serde_yml::from_str(yaml).unwrap();
        let include = config.include.unwrap();
        assert_eq!(include.len(), 2);
        assert!(include[0].exclude_newer.is_none());
        assert!(include[0].exclude_older.is_none());
        assert!(include[1].exclude_newer.is_some());
        assert!(include[1].exclude_older.is_some());
    }
}
