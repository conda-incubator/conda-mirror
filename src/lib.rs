use futures::{Stream, StreamExt, lock::Mutex, stream::FuturesUnordered};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use miette::IntoDiagnostic;
use number_prefix::NumberPrefix;
use opendal::{Configurator, Operator, layers::RetryLayer};
use rattler_conda_types::{
    ChannelConfig, Matches, NamedChannelOrUrl, PackageRecord, Platform, RepoData,
    package::ArchiveType,
};
use rattler_digest::Sha256Hash;
use rattler_index::{PreconditionChecks, RepodataMetadataCollection, write_repodata};
use rattler_networking::{
    Authentication, AuthenticationMiddleware, AuthenticationStorage, S3Middleware,
    authentication_storage::{StorageBackend, backends::memory::MemoryStorage},
    retry_policies::ExponentialBackoff,
    s3_middleware::S3Config,
};
use reqwest::StatusCode;
use reqwest_middleware::{
    ClientBuilder, ClientWithMiddleware,
    reqwest::{self, Client},
};
use reqwest_retry::RetryTransientMiddleware;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    env::current_dir,
    fmt,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{fs::File, sync::Semaphore, task::JoinError};
use tokio_util::{
    bytes,
    codec::{BytesCodec, FramedRead},
};
use tracing::info;
use url::Url;

pub mod config;
use config::{CondaMirrorConfig, MirrorMode};

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
enum OpenDALConfigurator {
    File(opendal::services::FsConfig),
    S3(opendal::services::S3Config),
}

#[derive(Debug)]
struct SpeedTrackerBar {
    progress_bar: ProgressBar,
    total_bytes: Option<u64>,
    downloaded_bytes: u64,
    history: VecDeque<(Instant, u64)>,
    window: Duration,
    window_bits: u64,
}

impl SpeedTrackerBar {
    fn new(progress_bar: ProgressBar, window: Duration) -> Self {
        Self {
            progress_bar,
            total_bytes: Some(0u64),
            downloaded_bytes: 0u64,
            history: VecDeque::new(),
            window,
            window_bits: 0u64,
        }
    }

    fn record(&mut self, bytes: u64) {
        let bits = bytes * 8;
        let now = Instant::now();
        self.history.push_back((now, bits));
        self.window_bits += bits;

        // remove entries older than the window
        while let Some((time, bits)) = self.history.front() {
            if now.duration_since(*time) > self.window {
                self.window_bits -= bits;
                self.history.pop_front();
            } else {
                break;
            }
        }
    }

    fn avg_speed(&self) -> f64 {
        self.history
            .front()
            .map(|entry| {
                let elapsed = Instant::now().duration_since(entry.0).as_secs_f64();
                if elapsed > 0.0 {
                    self.window_bits as f64 / elapsed
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0)
    }
}

pub struct HumanBitsPerSecond(f64);

impl fmt::Display for HumanBitsPerSecond {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match NumberPrefix::decimal(self.0) {
            NumberPrefix::Standalone(number) => write!(f, "{number:.2} b/s"),
            NumberPrefix::Prefixed(prefix, number) => write!(f, "{number:.2} {prefix}b/s"),
        }
    }
}

#[derive(Debug, Error)]
pub enum MirrorPackageErrorKind {
    #[error("failed to open file {0}: {1}")]
    FileOpen(PathBuf, #[source] std::io::Error),
    #[error("failed to read file {0}: {1}")]
    FileRead(PathBuf, #[source] std::io::Error),
    #[error("failed to send request to {0}: {1}")]
    SendRequest(Url, #[source] reqwest_middleware::Error),
    #[error("failed to get response from {0}: {1}")]
    GetResponse(Url, #[source] reqwest::Error),
    #[error("invalid digest: {expected:x} (expected) != {actual:x} (actual)")]
    InvalidDigest {
        expected: Sha256Hash,
        actual: Sha256Hash,
    },
    #[error("invalid size: expected {expected} bytes, got {actual} bytes")]
    InvalidSize { expected: u64, actual: u64 },
    #[error("failed upload to {0}: {1}")]
    Upload(String, #[source] opendal::Error),
    #[error("failed to delete {0}: {1}")]
    Delete(String, #[source] opendal::Error),
    #[error("failed to parse URL: {0}")]
    ParseUrl(#[source] url::ParseError),
    #[error("error while fetching {url}: got status {status}")]
    UnsuccessfulFetch { status: StatusCode, url: Url },
}

#[derive(Debug, Error)]
#[error("error downloading {filename}: {source}")]
pub struct MirrorPackageError {
    pub filename: String,
    #[source]
    pub source: Box<MirrorPackageErrorKind>,
}

trait WithFileContext<T> {
    fn with_filename(self, filename: &str) -> Result<T, MirrorPackageError>;
}

impl<T, E> WithFileContext<T> for Result<T, E>
where
    MirrorPackageErrorKind: From<E>,
{
    fn with_filename(self, filename: &str) -> Result<T, MirrorPackageError> {
        self.map_err(|err| MirrorPackageError {
            filename: filename.to_string(),
            source: Box::new(err.into()),
        })
    }
}

#[derive(Debug, Error)]
pub enum MirrorSubdirErrorKind {
    #[error("failed to read repodata {0}: {1}")]
    FailedToReadRepodata(PathBuf, #[source] std::io::Error),
    #[error("failed to send request to {0}: {1}")]
    SendRequest(Url, #[source] reqwest_middleware::Error),
    #[error("error while fetching {url}: got status {status}")]
    UnsuccessfulFetch { status: StatusCode, url: Url },
    #[error("failed to get response text for {0}: {1}")]
    FailedToGetResponseText(Url, #[source] reqwest::Error),
    #[error("failed to parse repodata: {0}")]
    FailedToParseRepodata(#[source] serde_json::Error),
    #[error("failed to construct OpenDAL operator: {0}")]
    FailedToConstructOpenDalOperator(#[source] opendal::Error),
    #[error("failed to query available packages: {0}")]
    FailedToQueryAvailablePackages(#[source] opendal::Error),
    #[error("failed to delete {0} packages")]
    FailedToDeletePackages(usize),
    #[error("failed to add {0} packages")]
    FailedToAddPackages(usize),
    #[error("task panicked: {0}")]
    JoinError(#[from] JoinError),
    #[error("url parse error: {0}")]
    UrlParseError(#[source] url::ParseError),
    #[error("failed to write repodata: {0}")]
    // https://github.com/conda/rattler/issues/1726
    WriteRepodata(#[source] anyhow::Error),
}

#[derive(Debug, Error)]
#[error("error mirroring subdir {subdir}: {source}")]
pub struct MirrorSubdirError {
    pub subdir: Platform,
    #[source]
    pub source: MirrorSubdirErrorKind,
}

trait WithSubdirContext<T> {
    fn with_subdir(self, subdir: Platform) -> Result<T, MirrorSubdirError>;
}

impl<T, E> WithSubdirContext<T> for Result<T, E>
where
    MirrorSubdirErrorKind: From<E>,
{
    fn with_subdir(self, subdir: Platform) -> Result<T, MirrorSubdirError> {
        self.map_err(|err| MirrorSubdirError {
            subdir,
            source: err.into(),
        })
    }
}

pub async fn mirror(config: CondaMirrorConfig) -> miette::Result<()> {
    let client = get_client(&config)?;

    let channel_config = ChannelConfig::default_with_root_dir(current_dir().into_diagnostic()?);
    let dest_channel = config
        .destination
        .clone()
        .into_channel(&channel_config)
        .into_diagnostic()?;
    let dest_channel_url = dest_channel.base_url.url();
    let opendal_config = match dest_channel_url.scheme() {
        "file" => {
            let channel_path_str = dest_channel_url
                .to_file_path()
                .map_err(|_| miette::miette!("Could not convert URL to file path"))?
                .canonicalize()
                .map_err(|e| miette::miette!("Could not canonicalize path: {}", e))? // todo: if doesn't exist, create it
                .to_string_lossy()
                .to_string();
            let mut config = opendal::services::FsConfig::default();
            config.root = Some(channel_path_str);
            OpenDALConfigurator::File(config)
        }
        "s3" => {
            let s3_config = config
                .s3_config_destination
                .clone()
                .ok_or(miette::miette!("No S3 destination config set"))?;
            let mut opendal_s3_config = opendal::services::S3Config::default();
            opendal_s3_config.root = Some(dest_channel_url.path().to_string());
            opendal_s3_config.bucket = dest_channel_url
                .host_str()
                .ok_or(miette::miette!("No bucket in S3 URL"))?
                .to_string();
            opendal_s3_config.region = Some(s3_config.region);
            opendal_s3_config.endpoint = Some(s3_config.endpoint_url.to_string());
            opendal_s3_config.enable_virtual_host_style = !s3_config.force_path_style;
            // Use credentials from the CLI if they are provided.
            if let Some(s3_credentials) = config.s3_credentials_destination.clone() {
                opendal_s3_config.secret_access_key = Some(s3_credentials.secret_access_key);
                opendal_s3_config.access_key_id = Some(s3_credentials.access_key_id);
                opendal_s3_config.session_token = s3_credentials.session_token;
            } else {
                // If they're not provided, check rattler authentication storage for credentials.
                let auth_storage =
                    AuthenticationStorage::from_env_and_defaults().into_diagnostic()?;
                let auth = auth_storage
                    .get_by_url(dest_channel_url.to_string())
                    .into_diagnostic()?;
                if let (
                    _,
                    Some(Authentication::S3Credentials {
                        access_key_id,
                        secret_access_key,
                        session_token,
                    }),
                ) = auth
                {
                    opendal_s3_config.access_key_id = Some(access_key_id);
                    opendal_s3_config.secret_access_key = Some(secret_access_key);
                    opendal_s3_config.session_token = session_token;
                } else {
                    return Err(miette::miette!("Missing S3 credentials"));
                }
            }

            OpenDALConfigurator::S3(opendal_s3_config)
        }
        _ => {
            return Err(miette::miette!(
                "Unsupported scheme in destination: {}",
                dest_channel_url.scheme()
            ));
        }
    };
    tracing::info!("Using opendal config: {:?}", opendal_config);

    eprintln!(
        "🪞 Mirroring {} to {}...",
        config.source, config.destination
    );

    let subdirs = get_subdirs(&config, client.clone()).await?;
    tracing::info!("Mirroring the following subdirs: {:?}", subdirs);

    let max_parallel = config.max_parallel as usize;
    let multi_progress = Arc::new(MultiProgress::new());
    let semaphore = Arc::new(Semaphore::new(max_parallel));

    if config.no_progress {
        multi_progress.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }

    let mut tasks = FuturesUnordered::new();

    // Progress bar for speed/time tracking
    let speed_bar = multi_progress.add(ProgressBar::new_spinner());
    speed_bar.set_style(
        ProgressStyle::with_template("{spinner:.green} | {elapsed_precise} | {msg}").unwrap(),
    );
    speed_bar.enable_steady_tick(std::time::Duration::from_millis(150));

    let window_duration = Duration::from_secs(20);

    let speed_tracker_bar = Arc::new(Mutex::new(SpeedTrackerBar::new(speed_bar, window_duration)));

    for subdir in subdirs.clone() {
        let config = config.clone();
        let client = client.clone();
        let multi_progress = multi_progress.clone();
        let semaphore = semaphore.clone();
        let opendal_config = opendal_config.clone();
        let speed_tracker_bar = speed_tracker_bar.clone();
        let task = async move {
            match &opendal_config {
                // todo: call mirror_subdir with configurator instead
                OpenDALConfigurator::File(opendal_config) => {
                    mirror_subdir(
                        config.clone(),
                        opendal_config.clone(),
                        client.clone(),
                        subdir,
                        multi_progress.clone(),
                        semaphore.clone(),
                        speed_tracker_bar.clone(),
                    )
                    .await // TODO: remove async move and .await
                }
                OpenDALConfigurator::S3(opendal_config) => {
                    mirror_subdir(
                        config.clone(),
                        opendal_config.clone(),
                        client.clone(),
                        subdir,
                        multi_progress.clone(),
                        semaphore.clone(),
                        speed_tracker_bar.clone(),
                    )
                    .await
                }
            }
        };
        tasks.push(tokio::spawn(task));
    }

    let mut failed = Vec::new();
    while let Some(join_result) = tasks.next().await {
        match join_result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::error!("Failed to process subdir {}: {}", e.subdir, e.source);
                failed.push(e);
            }
            Err(join_err) => {
                tasks.clear();
                tracing::error!("Task panicked: {}", join_err);
                return Err(miette::miette!("Task panicked: {}", join_err));
            }
        }
    }

    if failed.is_empty() {
        eprintln!("✅ All subdirs mirrored successfully");
    } else {
        eprintln!(
            "❌ Mirroring completed. {} subdirs had failures.",
            failed.len()
        );
        for error in &failed {
            eprintln!(" - {}: {}", error.subdir, error.source);
        }
        return Err(miette::miette!(
            "Mirroring failed for {} subdirs",
            failed.len()
        ));
    }

    Ok(())
}

fn get_packages_to_mirror(
    repodata: RepoData,
    config: &CondaMirrorConfig,
) -> HashMap<String, PackageRecord> {
    let all_packages = repodata.packages.into_iter().chain(repodata.conda_packages);
    match config.mode.clone() {
        MirrorMode::All => all_packages.collect(),
        MirrorMode::OnlyInclude(include) => all_packages
            .filter(|pkg| include.iter().any(|i| i.matches(&pkg.1)))
            .collect(),
        MirrorMode::AllButExclude(exclude) => all_packages
            .filter(|pkg| !exclude.iter().any(|i| i.matches(&pkg.1)))
            .collect(),
        MirrorMode::IncludeExclude(include, exclude) => all_packages
            .filter(|pkg| {
                !exclude
                    .iter()
                    .any(|i| i.matches(&pkg.1) || include.iter().any(|i| i.matches(&pkg.1)))
            })
            .collect(),
    }
}

enum BytesStream {
    File(PathBuf, FramedRead<File, BytesCodec>),
    Reqwest(
        Url,
        Pin<
            Box<
                dyn Stream<
                        Item = Result<tokio_util::bytes::Bytes, reqwest_middleware::reqwest::Error>,
                    > + Send,
            >,
        >,
    ),
}

impl BytesStream {
    async fn next(&mut self) -> Option<Result<bytes::Bytes, MirrorPackageErrorKind>> {
        match self {
            BytesStream::File(path, stream) => stream.next().await.map(|bytes| {
                bytes
                    .map_err(|e| MirrorPackageErrorKind::FileRead(path.clone(), e))
                    .map(|bytes| bytes.into())
            }),
            BytesStream::Reqwest(url, stream) => stream.next().await.map(|bytes| {
                bytes.map_err(|e| MirrorPackageErrorKind::GetResponse(url.clone(), e))
            }),
        }
    }
}

#[allow(clippy::type_complexity)]
async fn dispatch_tasks_delete(
    packages_to_delete: Vec<String>,
    subdir: Platform,
    progress: Arc<MultiProgress>,
    semaphore: Arc<Semaphore>,
    op: Operator,
) -> Result<(), MirrorSubdirErrorKind> {
    let mut tasks = FuturesUnordered::new();
    if !packages_to_delete.is_empty() {
        let pb = Arc::new(progress.add(ProgressBar::new(packages_to_delete.len() as u64)));
        let sty = ProgressStyle::with_template("{bar:40.red/blue} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-");
        pb.set_style(sty);
        let packages_to_delete_len = packages_to_delete.len();

        let pb = pb.clone();
        for filename in packages_to_delete.clone() {
            let pb = pb.clone();
            let semaphore = semaphore.clone();
            let op = op.clone();
            let task = async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .expect("Semaphore was unexpectedly closed");
                pb.set_message(format!(
                    "Deleting packages in {} {}",
                    subdir.as_str(),
                    console::style(&filename).dim()
                ));

                let destination_path = format!("{}/{}", subdir.as_str(), filename);
                op.delete(destination_path.as_str())
                    .await
                    .map_err(|e| MirrorPackageErrorKind::Delete(destination_path, e))
                    .with_filename(&filename)?;

                pb.inc(1);
                let res: Result<(), MirrorPackageError> = Ok(());
                res
            };
            tasks.push(tokio::spawn(task));
        }

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();
        while let Some(join_result) = tasks.next().await {
            match join_result {
                Ok(Ok(result)) => succeeded.push(result),
                Ok(Err(e)) => {
                    tracing::error!(
                        "Failed to delete package {subdir}/{}: {}",
                        e.filename,
                        e.source,
                    );
                    failed.push(e);
                    pb.inc(1);
                }
                Err(join_err) => {
                    tracing::error!("Task panicked: {}", join_err);
                    tasks.clear();
                    pb.abandon_with_message(format!(
                        "{} {}",
                        console::style("Failed to delete packages in").red(),
                        console::style(subdir.as_str()).dim()
                    ));
                    return Err(join_err.into());
                }
            }
        }
        tracing::info!(
            "Deleted {}/{} packages in subdir {}",
            packages_to_delete_len - failed.len(),
            packages_to_delete_len,
            subdir.as_str()
        );
        if failed.is_empty() {
            pb.finish_with_message(format!(
                "{} {}",
                console::style("Finished deleting packages in").green(),
                console::style(subdir.as_str()).bold()
            ));
        } else {
            pb.abandon_with_message(format!(
                "{} {} with {} failures",
                console::style("Finished deleting packages in").red(),
                console::style(subdir.as_str()).bold(),
                failed.len()
            ));
            return Err(MirrorSubdirErrorKind::FailedToDeletePackages(failed.len()));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
async fn dispatch_tasks_add(
    packages_to_add: HashMap<String, PackageRecord>,
    subdir: Platform,
    config: &CondaMirrorConfig,
    client: ClientWithMiddleware,
    progress: Arc<MultiProgress>,
    semaphore: Arc<Semaphore>,
    op: Operator,
    speed_tracker_bar: Arc<Mutex<SpeedTrackerBar>>,
) -> Result<(), MirrorSubdirErrorKind> {
    if packages_to_add.is_empty() {
        return Ok(());
    }
    let mut tasks = FuturesUnordered::new();

    let pb = Arc::new(progress.add(ProgressBar::new(packages_to_add.len() as u64)));
    let sty = ProgressStyle::with_template("[{bar:40.cyan/blue}] {pos:>7}/{len:7} {msg}")
        .unwrap()
        .progress_chars("##-");
    pb.set_style(sty);

    let packages_to_add_len = packages_to_add.len();

    let pb = pb.clone();
    for (filename, package_record) in packages_to_add {
        let pb = pb.clone();
        let speed_tracker_bar = speed_tracker_bar.clone();
        let semaphore = semaphore.clone();
        let client = client.clone();
        let op = op.clone();
        let package_url = config
            .package_url(filename.as_str(), subdir)
            .map_err(MirrorSubdirErrorKind::UrlParseError)?;
        let task = async move {
            let _permit = semaphore
                .acquire()
                .await
                .expect("Semaphore was unexpectedly closed");

            pb.set_message(format!(
                "Mirroring {} {}",
                subdir.as_str(),
                console::style(&filename).dim()
            ));

            // use rattler client for downloading the package
            let mut stream: BytesStream = if package_url.scheme() == "file" {
                let path = package_url.to_file_path().unwrap();
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|e| MirrorPackageErrorKind::FileOpen(path.clone(), e))
                    .with_filename(&filename)?;
                let read =
                    tokio_util::codec::FramedRead::new(file, tokio_util::codec::BytesCodec::new());
                BytesStream::File(path, read)
            } else {
                let response = client
                    .get(package_url.clone())
                    .send()
                    .await
                    .map_err(|e| MirrorPackageErrorKind::SendRequest(package_url.clone(), e))
                    .with_filename(&filename)?;
                if !response.status().is_success() {
                    return Err(MirrorPackageErrorKind::UnsuccessfulFetch {
                        status: response.status(),
                        url: package_url,
                    })
                    .with_filename(&filename);
                }
                BytesStream::Reqwest(package_url, Box::pin(response.bytes_stream()))
            };

            let mut hasher = Sha256::default();
            let mut actual_bytes: u64 = 0;
            let destination_path = format!("{}/{}", subdir.as_str(), filename);
            let mut writer = op
                .writer(destination_path.as_str())
                .await
                .map_err(|e| MirrorPackageErrorKind::Upload(destination_path.clone(), e))
                .with_filename(&filename)?;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.with_filename(&filename)?;
                hasher.update(&chunk);
                actual_bytes += chunk.len() as u64;
                writer
                    .write(chunk)
                    .await
                    .map_err(|e| MirrorPackageErrorKind::Upload(destination_path.clone(), e))
                    .with_filename(&filename)?;
            }

            tracing::debug!("Downloaded package {filename} with {actual_bytes} bytes");

            let expected_digest = package_record.sha256;
            let digest = hasher.finalize();
            if let Some(expected_digest) = expected_digest {
                if expected_digest != digest {
                    writer
                        .abort()
                        .await
                        .map_err(|e| MirrorPackageErrorKind::Upload(destination_path.clone(), e))
                        .with_filename(&filename)?;
                    return Err(MirrorPackageError {
                        filename,
                        source: Box::new(MirrorPackageErrorKind::InvalidDigest {
                            expected: expected_digest,
                            actual: digest,
                        }),
                    });
                }
                tracing::debug!("Verified SHA256 of {filename}: {expected_digest:x}");
            } else {
                tracing::debug!("No SHA256 digest found for {filename}, skipping verification.");
            }

            // Verify size from repodata when available.
            if let Some(expected_size) = package_record.size
                && expected_size != actual_bytes
            {
                writer
                    .abort()
                    .await
                    .map_err(|e| MirrorPackageErrorKind::Upload(destination_path.clone(), e))
                    .with_filename(&filename)?;
                return Err(MirrorPackageErrorKind::InvalidSize {
                    expected: expected_size,
                    actual: actual_bytes,
                })
                .with_filename(&filename);
            }

            // use opendal to upload the package
            // TODO: Verify digest on S3 remote.
            writer
                .close()
                .await
                .map_err(|e| MirrorPackageErrorKind::Upload(destination_path, e))
                .with_filename(&filename)?;
            pb.inc(1);

            let mut speed_bar_guard = speed_tracker_bar.lock().await;

            // Update the total downloaded bytes
            speed_bar_guard.downloaded_bytes += actual_bytes;

            // Record current download in the speed tracker
            speed_bar_guard.record(actual_bytes);

            let avg_speed = speed_bar_guard.avg_speed();

            let msg = if let Some(total_bytes) = speed_bar_guard.total_bytes {
                // If the total size of the download is known
                format!(
                    "↕ {:.2} / {:.2} | {}",
                    HumanBytes(speed_bar_guard.downloaded_bytes),
                    HumanBytes(total_bytes),
                    HumanBitsPerSecond(avg_speed),
                )
            } else {
                // If total size is unknown, display only downloaded bytes and speed
                format!(
                    "↕ {:.2} | {}",
                    HumanBytes(speed_bar_guard.downloaded_bytes),
                    HumanBitsPerSecond(avg_speed),
                )
            };

            speed_bar_guard.progress_bar.set_message(msg);
            let res: Result<(), MirrorPackageError> = Ok(());
            res
        };
        tasks.push(tokio::spawn(task));
    }

    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    while let Some(join_result) = tasks.next().await {
        match join_result {
            Ok(Ok(result)) => succeeded.push(result),
            Ok(Err(e)) => {
                tracing::error!(
                    "Failed to add package {subdir}/{}: {}",
                    e.filename,
                    e.source,
                );
                pb.inc(1);
                failed.push(e);
            }
            Err(join_err) => {
                tasks.clear();
                tracing::error!("Task panicked: {}", join_err);
                pb.abandon_with_message(format!(
                    "{} {}",
                    console::style("Failed to add packages in").red(),
                    console::style(subdir.as_str()).dim()
                ));
                return Err(join_err.into());
            }
        }
    }
    tracing::info!(
        "Added {}/{} packages in subdir {}",
        packages_to_add_len - failed.len(),
        packages_to_add_len,
        subdir.as_str()
    );
    if failed.is_empty() {
        pb.finish_with_message(format!(
            "{} {}",
            console::style("Finished adding packages in").green(),
            console::style(subdir.as_str()).bold()
        ));
    } else {
        pb.abandon_with_message(format!(
            "{} {} with {} failures",
            console::style("Finished adding packages in").red(),
            console::style(subdir.as_str()).bold(),
            failed.len()
        ));
        return Err(MirrorSubdirErrorKind::FailedToAddPackages(failed.len()));
    }
    Ok(())
}

async fn mirror_subdir<T: Configurator>(
    config: CondaMirrorConfig,
    opendal_config: T,
    client: ClientWithMiddleware,
    subdir: Platform,
    progress: Arc<MultiProgress>,
    semaphore: Arc<Semaphore>,
    speed_tracker_bar: Arc<Mutex<SpeedTrackerBar>>,
) -> Result<(), MirrorSubdirError> {
    let repodata_url = config.repodata_url(subdir);
    // TODO: implement spinner for repodata fetching, use rattler-repodata-gateway for sharded repodata?
    let repodata = if repodata_url.scheme() == "file" {
        let repodata_path = repodata_url.to_file_path().unwrap();
        info!("Reading repodata from {}", repodata_path.to_string_lossy());
        RepoData::from_path(&repodata_path)
            .map_err(|e| MirrorSubdirErrorKind::FailedToReadRepodata(repodata_path, e))
            .with_subdir(subdir)?
    } else {
        info!("Fetching repodata from {repodata_url}");
        let response = client
            .get(repodata_url.clone())
            .send()
            .await
            .map_err(|e| MirrorSubdirErrorKind::SendRequest(repodata_url.clone(), e))
            .with_subdir(subdir)?;
        if !response.status().is_success() {
            return Err(MirrorSubdirErrorKind::UnsuccessfulFetch {
                status: response.status(),
                url: repodata_url,
            })
            .with_subdir(subdir);
        }
        let text = response
            .text()
            .await
            .map_err(|e| MirrorSubdirErrorKind::FailedToGetResponseText(repodata_url, e))
            .with_subdir(subdir)?;
        serde_json::from_str(&text)
            .map_err(MirrorSubdirErrorKind::FailedToParseRepodata)
            .with_subdir(subdir)?
    };
    tracing::info!("Fetched repo data for subdir: {}", subdir);

    let builder = opendal_config.into_builder();
    let op = Operator::new(builder)
        .map_err(MirrorSubdirErrorKind::FailedToConstructOpenDalOperator)
        .with_subdir(subdir)?
        .layer(RetryLayer::new().with_max_times(config.max_retries.into()))
        .finish();
    let available_packages = op
        .list_with(&format!("{}/", subdir.as_str()))
        .await
        .map_err(MirrorSubdirErrorKind::FailedToQueryAvailablePackages)
        .with_subdir(subdir)?
        .iter()
        .filter_map(|entry| {
            if entry.metadata().mode().is_file() {
                let filename = entry.name().to_string();
                ArchiveType::try_from(&filename).map(|_| filename)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    let repodata_info = repodata.info.clone();
    let repodata_version = repodata.version;
    let repodata_removed = repodata.removed.clone();
    let packages_to_mirror = get_packages_to_mirror(repodata, &config);
    tracing::info!(
        "Mirroring {} packages in {}",
        packages_to_mirror.len(),
        subdir,
    );
    let packages_to_delete = available_packages
        .difference(&packages_to_mirror.keys().cloned().collect::<HashSet<_>>())
        .cloned()
        .collect::<Vec<_>>();
    let mut packages_to_add = HashMap::new();
    let mut subdir_size_to_add = Some(0);
    for (filename, package) in packages_to_mirror.clone() {
        if !available_packages.contains(&filename) {
            if let Some(size) = package.size {
                if let Some(current) = subdir_size_to_add {
                    subdir_size_to_add = Some(current + size);
                }
            } else {
                subdir_size_to_add = None;
            }
            packages_to_add.insert(filename, package);
        }
    }

    if let Some(add_size) = subdir_size_to_add {
        let mut speed_bar_guard = speed_tracker_bar.lock().await;
        if let Some(current_total) = speed_bar_guard.total_bytes.as_mut() {
            *current_total += add_size;
        }
    }

    tracing::info!(
        "Deleting {} existing packages in {}",
        packages_to_delete.len(),
        subdir
    );
    dispatch_tasks_delete(
        packages_to_delete,
        subdir,
        progress.clone(),
        semaphore.clone(),
        op.clone(),
    )
    .await
    .with_subdir(subdir)?;

    tracing::info!("Adding {} packages in {}", packages_to_add.len(), subdir);
    dispatch_tasks_add(
        packages_to_add,
        subdir,
        &config,
        client,
        progress.clone(),
        semaphore.clone(),
        op.clone(),
        speed_tracker_bar.clone(),
    )
    .await
    .with_subdir(subdir)?;

    /* ---------------------------- WRITE REPODATA ---------------------------- */
    let packages = packages_to_mirror
        .iter()
        .filter(
            |(filename, _)| match ArchiveType::try_from(filename.as_str()) {
                Some(ArchiveType::TarBz2) => true,
                Some(ArchiveType::Conda) => false,
                None => {
                    unreachable!("Packages in repodata are always either Conda or TarBz2")
                }
            },
        )
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let conda_packages = packages_to_mirror
        .iter()
        .filter(
            |(filename, _)| match ArchiveType::try_from(filename.as_str()) {
                Some(ArchiveType::TarBz2) => false,
                Some(ArchiveType::Conda) => true,
                None => {
                    unreachable!("Packages in repodata are always either Conda or TarBz2")
                }
            },
        )
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let new_repodata = RepoData {
        info: repodata_info,
        packages,
        conda_packages,
        removed: repodata_removed,
        version: repodata_version,
    };
    let precondition_checks = if config.precondition_checks {
        PreconditionChecks::Enabled
    } else {
        PreconditionChecks::Disabled
    };
    let metadata =
        RepodataMetadataCollection::new(&op, subdir, true, true, true, precondition_checks)
            .await
            .map_err(MirrorSubdirErrorKind::FailedToConstructOpenDalOperator)
            .with_subdir(subdir)?;
    write_repodata(new_repodata, None, subdir, op, &metadata)
        .await
        .map_err(MirrorSubdirErrorKind::WriteRepodata)
        .with_subdir(subdir)?;
    // todo: check if non-conda and non-repodata files exist, print warning if any
    Ok(())
}

async fn get_subdirs(
    config: &CondaMirrorConfig,
    client: ClientWithMiddleware,
) -> miette::Result<Vec<Platform>> {
    if let Some(subdirs) = config.subdirs.clone() {
        return Ok(subdirs);
    }

    let mut subdirs = Vec::new();

    for subdir in Platform::all() {
        tracing::debug!("Checking subdir: {}", subdir);
        let repodata_url = config.repodata_url(subdir);

        // todo: parallelize
        if repodata_url.scheme() == "file" {
            let path = PathBuf::from(repodata_url.path());
            tracing::debug!("Checking file path: {}", path.display());
            if path.exists() {
                subdirs.push(subdir);
            }
        } else {
            let response = client
                .head(repodata_url.clone())
                .send()
                .await
                .into_diagnostic()?;
            tracing::debug!("Got response for url {}: {:?}", repodata_url, response);

            if response.status().is_success() {
                subdirs.push(subdir);
            }
        }
    }
    Ok(subdirs)
}

fn get_client(config: &CondaMirrorConfig) -> miette::Result<ClientWithMiddleware> {
    let client = Client::builder()
        .pool_max_idle_per_host(20)
        .user_agent(format!("conda-mirror/{}", env!("CARGO_PKG_VERSION")))
        .read_timeout(Duration::from_secs(30))
        .build()
        .expect("failed to create reqwest Client");
    let mut client_builder = ClientBuilder::new(client.clone());

    let auth_store = AuthenticationStorage::from_env_and_defaults().into_diagnostic()?;
    if let NamedChannelOrUrl::Url(source_url) = config.source.clone()
        && source_url.scheme() == "s3"
    {
        let s3_host = source_url
            .host()
            .ok_or(miette::miette!("Invalid S3 url: {}", source_url))?
            .to_string();
        let s3_config = config
            .clone()
            .s3_config_source
            .ok_or(miette::miette!("No S3 source config set"))?;

        let s3_middleware = S3Middleware::new(
            HashMap::from([(
                s3_host,
                S3Config::Custom {
                    endpoint_url: s3_config.endpoint_url,
                    region: s3_config.region,
                    force_path_style: s3_config.force_path_style,
                },
            )]),
            // TODO: once rattler has a custom InMemoryBackend, add this to auth_store with custom source credentials
            auth_store,
        );
        client_builder = client_builder.with(s3_middleware);
    }

    let auth_store = if let Some(s3_credentials) = config.s3_credentials_source.clone() {
        let mut auth_store = AuthenticationStorage::from_env_and_defaults().into_diagnostic()?;
        let memory_storage = MemoryStorage::default();
        let s3_host = match config.source.clone() {
            NamedChannelOrUrl::Path(_) | NamedChannelOrUrl::Name(_) => {
                return Err(miette::miette!(
                    "Source is not an S3 URL: {}",
                    config.source
                ));
            }
            NamedChannelOrUrl::Url(url) => {
                let scheme = url.scheme();
                if scheme != "s3" {
                    return Err(miette::miette!("Invalid S3 URL: {}", url));
                }
                let host = url
                    .host()
                    .ok_or(miette::miette!("Invalid S3 URL: {}", url))?;
                host.to_string()
            }
        };
        memory_storage
            .store(
                s3_host.as_str(),
                &Authentication::S3Credentials {
                    access_key_id: s3_credentials.access_key_id,
                    secret_access_key: s3_credentials.secret_access_key,
                    session_token: s3_credentials.session_token,
                },
            )
            .into_diagnostic()?;
        auth_store.backends.insert(0, Arc::new(memory_storage));
        auth_store
    } else {
        AuthenticationStorage::from_env_and_defaults().into_diagnostic()?
    };

    client_builder = client_builder.with_arc(Arc::new(
        AuthenticationMiddleware::from_auth_storage(auth_store),
    ));

    client_builder = client_builder.with(RetryTransientMiddleware::new_with_policy(
        ExponentialBackoff::builder().build_with_max_retries(config.max_retries.into()),
    ));

    let authenticated_client = client_builder.build();
    Ok(authenticated_client)
}
