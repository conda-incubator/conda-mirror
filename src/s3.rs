use miette::{Context, IntoDiagnostic};
use rattler_networking::AuthenticationStorage;
use rattler_s3::{ResolvedS3Credentials, S3AddressingStyle, S3CredentialSource};
use url::Url;

use crate::config::{S3Config, S3Credentials};

/// The settings needed to talk to an S3 bucket, leaving the credentials aside.
struct S3Settings {
    endpoint_url: Url,
    region: String,
    addressing_style: S3AddressingStyle,
}

/// Resolve the settings of the S3 bucket of `bucket_url`.
///
/// Either the settings are configured explicitly (through the CLI or the
/// configuration file), or they are resolved through the AWS SDK, in which case
/// the credential source it assembled is returned as well. That source is the
/// part that can refresh expiring credentials; a set of credentials produced by
/// it cannot.
async fn resolve_s3_settings(
    bucket_url: &Url,
    s3_config: Option<&S3Config>,
) -> miette::Result<(S3Settings, Option<S3CredentialSource>)> {
    let Some(s3_config) = s3_config else {
        let from_sdk = S3CredentialSource::from_sdk()
            .await
            .into_diagnostic()
            .wrap_err_with(|| {
                format!(
                    "failed to resolve the S3 configuration of {bucket_url} through the AWS SDK. \
                     Log in with `aws sso login` if you are using AWS SSO, or configure the \
                     endpoint URL, region and addressing style explicitly"
                )
            })?;

        let settings = S3Settings {
            endpoint_url: from_sdk.endpoint_url.clone(),
            region: from_sdk.region.clone(),
            addressing_style: from_sdk.addressing_style,
        };
        return Ok((settings, Some(from_sdk)));
    };

    Ok((
        S3Settings {
            endpoint_url: s3_config.endpoint_url.clone(),
            region: s3_config.region.clone(),
            addressing_style: if s3_config.force_path_style {
                S3AddressingStyle::Path
            } else {
                S3AddressingStyle::VirtualHost
            },
        },
        None,
    ))
}

/// Resolve the credentials of the S3 bucket of `bucket_url` from the CLI, the
/// configuration file, or the authentication storage of `rattler auth` /
/// `pixi auth`.
fn resolve_own_credentials(
    bucket_url: &Url,
    settings: &S3Settings,
    credentials: Option<&S3Credentials>,
    auth_storage: &AuthenticationStorage,
) -> Option<ResolvedS3Credentials> {
    rattler_s3::S3Credentials {
        endpoint_url: settings.endpoint_url.clone(),
        region: settings.region.clone(),
        addressing_style: settings.addressing_style,
        access_key_id: credentials.map(|credentials| credentials.access_key_id.clone()),
        secret_access_key: credentials.map(|credentials| credentials.secret_access_key.clone()),
        session_token: credentials.and_then(|credentials| credentials.session_token.clone()),
    }
    .resolve(bucket_url, auth_storage)
}

fn missing_credentials(bucket_url: &Url) -> miette::Report {
    miette::miette!(
        help = "pass them explicitly, log in with `pixi auth login`, or drop the S3 settings to \
                resolve everything through the AWS SDK",
        "missing S3 credentials for {bucket_url}"
    )
}

/// Resolve how the requests to the S3 bucket of `bucket_url` should be signed.
///
/// Credentials that were configured explicitly (through the CLI or the
/// configuration file) or that are in the authentication storage of
/// `rattler auth` / `pixi auth` are used as-is. If there are none, the credential
/// provider of the AWS SDK is kept around so that it can be asked again once the
/// credentials it handed out expire, which is what makes multi-day runs work with
/// the temporary credentials of AWS SSO, assumed roles and instance metadata.
pub(crate) async fn resolve_s3_credential_source(
    bucket_url: &Url,
    s3_config: Option<&S3Config>,
    credentials: Option<&S3Credentials>,
    auth_storage: &AuthenticationStorage,
) -> miette::Result<S3CredentialSource> {
    let (settings, from_sdk) = resolve_s3_settings(bucket_url, s3_config).await?;

    // Credentials passed explicitly take precedence over the ones from the
    // authentication storage, which take precedence over the ones from the AWS SDK.
    if let Some(credentials) =
        resolve_own_credentials(bucket_url, &settings, credentials, auth_storage)
    {
        return Ok(credentials.into());
    }

    // We have no credentials of our own, so leave them to the AWS SDK.
    from_sdk.ok_or_else(|| missing_credentials(bucket_url))
}

/// Resolve the settings and credentials needed to talk to the S3 bucket of
/// `bucket_url`.
///
/// Note that credentials are resolved once, so temporary credentials that expire
/// during a long mirror run are not refreshed. Prefer
/// [`resolve_s3_credential_source`] where the consumer can refresh them itself.
pub(crate) async fn resolve_s3_credentials(
    bucket_url: &Url,
    s3_config: Option<&S3Config>,
    credentials: Option<&S3Credentials>,
    auth_storage: &AuthenticationStorage,
) -> miette::Result<ResolvedS3Credentials> {
    resolve_s3_credential_source(bucket_url, s3_config, credentials, auth_storage)
        .await?
        .credentials()
        .await
        .into_diagnostic()
        .wrap_err_with(|| {
            format!("failed to resolve the S3 credentials of {bucket_url} through the AWS SDK")
        })
}
