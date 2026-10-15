use std::{
    fs::{self, File},
    path::Path,
    time::{Duration, SystemTime},
};

use conda_mirror::{
    config::{CondaMirrorConfig, MirrorMode},
    mirror,
};
use rattler_conda_types::{NamedChannelOrUrl, Subdir};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use url::Url;

const PACKAGE: &[u8] = b"package contents for timestamp tests";
const LAST_MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

fn repodata(filenames: &[&str]) -> Vec<u8> {
    let packages = filenames
        .iter()
        .map(|filename| {
            (
                (*filename).to_owned(),
                json!({
                    "name": filename.strip_suffix("-1.0-0.tar.bz2").unwrap(),
                    "version": "1.0",
                    "build": "0",
                    "build_number": 0,
                    "subdir": "noarch",
                    "depends": [],
                    "size": PACKAGE.len(),
                    "sha256": hex::encode(Sha256::digest(PACKAGE)),
                    // A build timestamp must not be used as the file's mtime.
                    "timestamp": 1_000_000,
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::to_vec(&json!({
        "info": { "subdir": "noarch" },
        "packages": packages,
        "packages.conda": {},
        "repodata_version": 1,
    }))
    .unwrap()
}

fn config(source: Url, destination: &Path) -> CondaMirrorConfig {
    CondaMirrorConfig::new(
        NamedChannelOrUrl::Url(source),
        NamedChannelOrUrl::Url(Url::from_directory_path(destination).unwrap()),
        Some(vec![Subdir::NoArch]),
        MirrorMode::All,
        0,
        1,
        true,
        None,
        None,
        None,
        None,
        false,
    )
    .unwrap()
}

fn mtime(path: &Path) -> SystemTime {
    fs::metadata(path).unwrap().modified().unwrap()
}

#[tokio::test]
async fn preserves_local_package_mtime() {
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let subdir = source.path().join("noarch");
    fs::create_dir(&subdir).unwrap();
    let filename = "example-1.0-0.tar.bz2";
    let package = subdir.join(filename);
    fs::write(&package, PACKAGE).unwrap();
    // Include subsecond precision to exercise filesystem timestamp fidelity.
    let modified = httpdate::parse_http_date(LAST_MODIFIED).unwrap() + Duration::from_millis(123);
    File::options()
        .write(true)
        .open(&package)
        .unwrap()
        .set_modified(modified)
        .unwrap();
    fs::write(subdir.join("repodata.json"), repodata(&[filename])).unwrap();
    let config = config(
        Url::from_directory_path(source.path()).unwrap(),
        destination.path(),
    );

    mirror(config.clone()).await.unwrap();

    let mirrored = destination.path().join("noarch").join(filename);
    assert_eq!(fs::read(&mirrored).unwrap(), PACKAGE);
    assert_eq!(mtime(&mirrored), mtime(&package));
    assert!(mtime(&destination.path().join("noarch/repodata.json")) > modified);

    mirror(config).await.unwrap();
    assert_eq!(mtime(&mirrored), mtime(&package));
}

#[tokio::test]
async fn preserves_http_last_modified_and_tolerates_unavailable_timestamps() {
    let destination = tempfile::tempdir().unwrap();
    let filenames = [
        "valid-1.0-0.tar.bz2",
        "missing-1.0-0.tar.bz2",
        "invalid-1.0-0.tar.bz2",
        "nonascii-1.0-0.tar.bz2",
    ];
    let repodata = repodata(&filenames);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(async move {
        for _ in 0..=filenames.len() {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let mut buffer = [0; 1024];
                let len = stream.read(&mut buffer).await.unwrap();
                assert_ne!(len, 0);
                request.extend_from_slice(&buffer[..len]);
            }
            let request = String::from_utf8(request).unwrap();
            let path = request.split_whitespace().nth(1).unwrap();
            let (body, last_modified): (&[u8], &[u8]) = match path {
                "/noarch/repodata.json" => (&repodata, b""),
                "/noarch/valid-1.0-0.tar.bz2" => {
                    (PACKAGE, b"Last-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n")
                }
                "/noarch/missing-1.0-0.tar.bz2" => (PACKAGE, b""),
                "/noarch/invalid-1.0-0.tar.bz2" => (PACKAGE, b"Last-Modified: invalid\r\n"),
                "/noarch/nonascii-1.0-0.tar.bz2" => (PACKAGE, b"Last-Modified: \xff\r\n"),
                _ => panic!("unexpected request: {path}"),
            };
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(last_modified).await.unwrap();
            stream.write_all(b"\r\n").await.unwrap();
            stream.write_all(body).await.unwrap();
        }
    });
    let started = SystemTime::now();

    tokio::time::timeout(
        Duration::from_secs(30),
        mirror(config(source, destination.path())),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();

    let modified = httpdate::parse_http_date(LAST_MODIFIED).unwrap();
    for (index, filename) in filenames.iter().enumerate() {
        let package = destination.path().join("noarch").join(filename);
        assert_eq!(fs::read(&package).unwrap(), PACKAGE);
        if index == 0 {
            assert_eq!(mtime(&package), modified);
        } else {
            // Allow for filesystem timestamp rounding.
            assert!(mtime(&package) >= started - Duration::from_secs(1));
        }
    }
    assert!(mtime(&destination.path().join("noarch/repodata.json")) > modified);
}
