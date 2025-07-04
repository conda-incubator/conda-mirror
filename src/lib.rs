use futures::{StreamExt, stream::FuturesUnordered};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use miette::IntoDiagnostic;
use opendal::{Configurator, Operator, layers::RetryLayer};
use rattler_conda_types::{
    ChannelConfig, NamedChannelOrUrl, PackageRecord, Platform, RepoData, package::ArchiveType,
};
use rattler_digest::{Sha256Hash, compute_bytes_digest};
use rattler_index::write_repodata;
use rattler_networking::{
    Authentication, AuthenticationMiddleware, AuthenticationStorage, S3Middleware,
    authentication_storage::{StorageBackend, backends::memory::MemoryStorage},
    retry_policies::ExponentialBackoff,
    s3_middleware::S3Config,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, reqwest::Client};
use reqwest_retry::RetryTransientMiddleware;
use std::{
    collections::{HashMap, HashSet},
    env::current_dir,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{io::AsyncReadExt, sync::Semaphore};

pub mod config;
use config::{CondaMirrorConfig, MirrorMode};

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
enum OpenDALConfigurator {
    File(opendal::services::FsConfig),
    S3(opendal::services::S3Config),
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

    let max_parallel = 32;
    let multi_progress = Arc::new(MultiProgress::new());
    let semaphore = Arc::new(Semaphore::new(max_parallel));

    let mut tasks = FuturesUnordered::new();
    for subdir in subdirs {
        let config = config.clone();
        let client = client.clone();
        let multi_progress = multi_progress.clone();
        let semaphore = semaphore.clone();
        let opendal_config = opendal_config.clone();
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
                    )
                    .await
                }
            }
        };
        tasks.push(tokio::spawn(task));
    }

    while let Some(join_result) = tasks.next().await {
        match join_result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::error!("Failed to process subdir: {}", e);
                tasks.clear();
                return Err(e);
            }
            Err(join_err) => {
                tracing::error!("Task panicked: {}", join_err);
                tasks.clear();
                return Err(miette::miette!("Task panicked: {}", join_err));
            }
        }
    }

    eprintln!("✅ Mirroring completed");
    Ok(())
}

fn get_packages_to_mirror(
    repodata: &RepoData,
    config: &CondaMirrorConfig,
) -> HashMap<String, PackageRecord> {
    let mut all_packages = HashMap::new();
    all_packages.extend(repodata.packages.clone());
    all_packages.extend(repodata.conda_packages.clone());
    match config.mode.clone() {
        MirrorMode::All => all_packages.clone(),
        MirrorMode::OnlyInclude(include) => all_packages
            .clone()
            .into_iter()
            .filter(|pkg| include.iter().any(|i| i.matches(pkg.1.clone())))
            .collect(),
        MirrorMode::AllButExclude(exclude) => all_packages
            .clone()
            .into_iter()
            .filter(|pkg| !exclude.iter().any(|i| i.matches(pkg.1.clone())))
            .collect(),
        MirrorMode::IncludeExclude(include, exclude) => all_packages
            .clone()
            .into_iter()
            .filter(|pkg| {
                !exclude.iter().any(|i| {
                    i.matches(pkg.1.clone()) || include.iter().any(|i| i.matches(pkg.1.clone()))
                })
            })
            .collect(),
    }
}

#[allow(clippy::type_complexity)]
async fn dispatch_tasks_delete(
    packages_to_delete: Vec<String>,
    subdir: Platform,
    progress: Arc<MultiProgress>,
    semaphore: Arc<Semaphore>,
    op: Operator,
) -> miette::Result<()> {
    let mut tasks = FuturesUnordered::new();
    if !packages_to_delete.is_empty() {
        let pb = Arc::new(progress.add(ProgressBar::new(packages_to_delete.len() as u64)));
        let sty = ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.red/blue} {pos:>7}/{len:7} {msg}",
        )
        .unwrap()
        .progress_chars("##-");
        pb.set_style(sty);
        let packages_to_delete_len = packages_to_delete.len();

        let pb = pb.clone();
        for filename in packages_to_delete {
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
                    .into_diagnostic()?;

                pb.inc(1);
                let res: miette::Result<()> = Ok(());
                res
            };
            tasks.push(tokio::spawn(task));
        }

        let mut results = Vec::new();
        while let Some(join_result) = tasks.next().await {
            match join_result {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => {
                    tasks.clear();
                    tracing::error!("Failed to delete package: {}", e);
                    pb.abandon_with_message(format!(
                        "{} {}",
                        console::style("Failed to delete packages in").red(),
                        console::style(subdir.as_str()).dim()
                    ));
                    return Err(e);
                }
                Err(join_err) => {
                    tasks.clear();
                    tracing::error!("Task panicked: {}", join_err);
                    pb.abandon_with_message(format!(
                        "{} {}",
                        console::style("Failed to delete packages in").red(),
                        console::style(subdir.as_str()).dim()
                    ));
                    return Err(miette::miette!("Task panicked: {}", join_err));
                }
            }
        }
        tracing::debug!(
            "Successfully deleted {} packages in subdir {}",
            packages_to_delete_len,
            subdir.as_str()
        );
        pb.finish_with_message(format!(
            "{} {}",
            console::style("Finished deleting packages in").green(),
            subdir.as_str()
        ));
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
async fn dispatch_tasks_add(
    packages_to_add: HashMap<String, PackageRecord>,
    subdir: Platform,
    config: CondaMirrorConfig,
    client: ClientWithMiddleware,
    progress: Arc<MultiProgress>,
    semaphore: Arc<Semaphore>,
    op: Operator,
) -> miette::Result<()> {
    if !packages_to_add.is_empty() {
        let mut tasks = FuturesUnordered::new();

        let pb = Arc::new(progress.add(ProgressBar::new(packages_to_add.len() as u64)));
        let sty = ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
        )
        .unwrap()
        .progress_chars("##-");
        pb.set_style(sty);
        let packages_to_add_len = packages_to_add.len();

        let pb = pb.clone();
        for (filename, package_record) in packages_to_add {
            let pb = pb.clone();
            let semaphore = semaphore.clone();
            let config = config.clone();
            let client = client.clone();
            let op = op.clone();
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
                let package_url = config.package_url(filename.as_str(), subdir)?;
                let mut buf = Vec::new();
                if package_url.scheme() == "file" {
                    let path = package_url.to_file_path().unwrap();
                    let mut file = tokio::fs::File::open(path).await.into_diagnostic()?;
                    file.read_to_end(&mut buf).await.into_diagnostic()?;
                } else {
                    let response = client.get(package_url).send().await.into_diagnostic()?;
                    let bytes = response.bytes().await.into_diagnostic()?;
                    buf.extend_from_slice(&bytes);
                };
                tracing::debug!("Downloaded package {} with {} bytes", filename, buf.len());

                let expected_digest = package_record.sha256;
                if let Some(expected_digest) = expected_digest {
                    let digest: Sha256Hash = compute_bytes_digest::<sha2::Sha256>(&buf);
                    if expected_digest != digest {
                        return Err(miette::miette!(
                            "Digest of {} does not match: {:x} != {:x}",
                            filename,
                            expected_digest,
                            digest
                        ));
                    }
                }
                tracing::debug!("Verified SHA256 of {}", filename);

                // use opendal to upload the package
                let destination_path = format!("{}/{}", subdir.as_str(), filename);
                op.write(destination_path.as_str(), buf)
                    .await
                    .into_diagnostic()?;

                pb.inc(1);
                let res: miette::Result<()> = Ok(());
                res
            };
            tasks.push(tokio::spawn(task));
        }

        let mut results = Vec::new();
        while let Some(join_result) = tasks.next().await {
            match join_result {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => {
                    tasks.clear();
                    tracing::error!("Failed to add package: {}", e);
                    pb.abandon_with_message(format!(
                        "{} {}",
                        console::style("Failed to add packages in").red(),
                        console::style(subdir.as_str()).dim()
                    ));
                    return Err(e);
                }
                Err(join_err) => {
                    tasks.clear();
                    tracing::error!("Task panicked: {}", join_err);
                    pb.abandon_with_message(format!(
                        "{} {}",
                        console::style("Failed to add packages in").red(),
                        console::style(subdir.as_str()).dim()
                    ));
                    return Err(miette::miette!("Task add: {}", join_err));
                }
            }
        }
        tracing::debug!(
            "Successfully added {} packages in subdir {}",
            packages_to_add_len,
            subdir.as_str()
        );
        pb.finish_with_message(format!(
            "{} {}",
            console::style("Finished adding packages in").green(),
            subdir.as_str()
        ));
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
) -> miette::Result<()> {
    let repodata_url = config.repodata_url(subdir)?;
    let repodata = if repodata_url.scheme() == "file" {
        RepoData::from_path(
            repodata_url
                .to_file_path()
                .map_err(|_| miette::miette!("Invalid file path: {}", repodata_url))?,
        )
        .into_diagnostic()?
    } else {
        let response = client.get(repodata_url).send().await.into_diagnostic()?;
        if !response.status().is_success() {
            return Err(miette::miette!(
                "Failed to fetch repodata: {}",
                response.status()
            ));
        }
        let text = response.text().await.into_diagnostic()?;
        serde_json::from_str(&text).into_diagnostic()?
    };
    tracing::info!("Fetched repo data for subdir: {}", subdir);

    let builder = opendal_config.into_builder();
    let op = Operator::new(builder)
        .into_diagnostic()?
        .layer(RetryLayer::new())
        .finish();
    let available_packages = op
        .list_with(&format!("{}/", subdir.as_str()))
        .await
        .into_diagnostic()?
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

    let packages_to_mirror = get_packages_to_mirror(&repodata, &config);
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
    for (filename, package) in packages_to_mirror.clone() {
        if !available_packages.contains(&filename) {
            packages_to_add.insert(filename, package);
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
    .await?;

    tracing::info!("Adding {} packages in {}", packages_to_add.len(), subdir);
    dispatch_tasks_add(
        packages_to_add,
        subdir,
        config,
        client,
        progress.clone(),
        semaphore.clone(),
        op.clone(),
    )
    .await?;

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
        info: repodata.info,
        packages,
        conda_packages,
        removed: repodata.removed,
        version: repodata.version,
    };

    write_repodata(new_repodata, None, true, true, subdir, op)
        .await
        .map_err(|e| miette::miette!("Could not write repodata: {}", e))?;
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
        let repodata_url = config.repodata_url(subdir)?;

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
    if let NamedChannelOrUrl::Url(source_url) = config.source.clone() {
        if source_url.scheme() == "s3" {
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
        ExponentialBackoff::builder().build_with_max_retries(3),
    ));

    let authenticated_client = client_builder.build();
    Ok(authenticated_client)
}
