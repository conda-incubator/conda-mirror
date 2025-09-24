<div align="center">

[![License][license-badge]](LICENSE)
[![CI Status][ci-badge]][ci]
[![Conda Platform][conda-badge]][conda-url]
[![Conda Downloads][conda-downloads-badge]][conda-url]
[![Project Chat][chat-badge]][chat-url]
[![Pixi Badge][pixi-badge]][pixi-url]

[license-badge]: https://img.shields.io/github/license/conda-incubator/conda-mirror?style=flat-square
[ci-badge]: https://img.shields.io/github/actions/workflow/status/conda-incubator/conda-mirror/ci.yml?style=flat-square&branch=main
[ci]: https://github.com/conda-incubator/conda-mirror/actions/
[conda-badge]: https://img.shields.io/conda/vn/conda-forge/conda-mirror?style=flat-square
[conda-downloads-badge]: https://img.shields.io/conda/dn/conda-forge/conda-mirror?style=flat-square
[conda-url]: https://prefix.dev/channels/conda-forge/packages/conda-mirror
[chat-badge]: https://img.shields.io/discord/1082332781146800168.svg?label=&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2&style=flat-square
[chat-url]: https://discord.gg/kKV8ZxyzY4
[pixi-badge]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/prefix-dev/pixi/main/assets/badge/v0.json&style=flat-square
[pixi-url]: https://pixi.sh

</div>

# conda-mirror

Mirror conda channels

## 🗂 Table of Contents

- [Introduction](#-introduction)
- [Installation](#-installation)
- [Usage](#-usage)

## 📖 Introduction

This tool allows you to mirror conda channels to different backends using parallelism.
You can also specify custom whitelists or blacklists if you only want to include certain kinds of packages.

## 💿 Installation

You can install `conda-mirror` using `pixi`:

```bash
pixi global install conda-mirror
```

Or using `cargo`:

```bash
cargo install --locked --git https://github.com/conda-incubator/conda-mirror.git
```

Or by downloading our pre-built binaries from the [releases page](https://github.com/conda-incubator/conda-mirror/releases).

Instead of installing `conda-mirror` globally, you can also use [`pixi exec`](https://pixi.sh/latest/reference/cli/pixi/exec/) to run `conda-mirror` in a temporary environment:

```bash
pixi exec conda-mirror --source robostack --destination ./robostack
```

## 🎯 Usage

### CLI

You can mirror conda channels using

```bash
conda-mirror --source conda-forge --destination ./conda-forge
```

#### Subdirs

If you only want to mirror certain subdirs, you can do so using the `--subdir` flag:

```bash
conda-mirror --source robostack --destination ./robostack --subdir linux-64 --subdir linux-aarch64
```

#### Supported backends

You can mirror from multiple source backends, namely:

- filesystem: `--source ./conda-forge-local`
- http(s): `--source conda-forge` or `--source https://prefix.dev/conda-forge`
- oci: `--source oci://ghcr.io/channel-mirrors/conda-forge`
- s3: `--source s3://my-source-bucket/channel`

For mirroring authenticated channel, `conda-mirror` uses pixi's authentication.
See the [official documentation](https://pixi.sh/latest/deployment/authentication/#authentication) for more information.

You can mirror to multiple destination backends as well, namely:

- filesystem: `--destination ./conda-forge-local`
- s3: `--destination s3://my-destination-bucket/channel`

#### Configuration file

For more control like including only specific packages, you can use a configuration file and pass them to `conda-mirror` using `--config my-config.yml`.

Mirror all packages except a specific blacklist:

```yml
source: conda-forge
destination: ./my-channel

exclude:
  # you can use MatchSpecs here
  - jupyter >=0.5.0
  # you can also specify licenses in the MatchSpecs
  - "*[license=AGPL-3.0-or-later]"
```

Only mirror whitelisted packages:

```yml
source: conda-forge
destination: ./my-channel

include:
  - name-glob: jupyter*
```

Exclude all packages defined in `exclude`, override this behavior by specifying overrides in `include`:

```yml
source: conda-forge
destination: ./my-channel

include:
  - jupyter-ai
exclude:
  - name-glob: jupyter*
```

Only mirror certain subdirs:

```yml
source: conda-forge
destination: ./my-channel
subdirs:
  - linux-64
  - osx-arm64
  - noarch
  - win-64
```
### Append to Existing Mirror

When trying to add new packages to existing mirror, the `--append` flag can be used to prevent deletion of existing packages (default behavior). The repodata will be rewritten with this flag enabled.

**Notes** 
- The CLI option will override the YAML option.
- In append mode the tool skips deletions and repodata is written for the union of previously present files plus the files added in the current run.
- Files that exist locally but are not in the source `repodata.json` won't be indexed.
- To make a strict snapshot again, re-run without `--append`.

Append new packages to the existing mirror:
```yml
source: conda-forge
destination: ./my-channel
append: true
include:
  - matplotlib
```

#### S3 configuration

When using S3, you need to configure the S3 endpoint by setting the region, endpoint url, and whether to use path-style addressing.
You can either set these by using the appropriate CLI flags or by using a configuration file.

```yml
source: s3://my-source-channel
destination: s3://my-destination-channel

s3-config:
  source:
    endpoint-url: https://fsn1.your-objectstorage.com
    force-path-style: false
    region: US
  destination:
    endpoint-url: https://s3.eu-central-1.amazonaws.com
    force-path-style: false
    region: eu-central-1
```

See [pixi's documentation](https://pixi.sh/latest/deployment/s3/#s3-compatible-storage) for configuring S3-compatible storage like Cloudflare R2 or Hetzner Object Storage.
