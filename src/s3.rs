use miette::{Context, IntoDiagnostic};
use rattler_networking::AuthenticationStorage;
use rattler_s3::{ResolvedS3Credentials, S3AddressingStyle};
use url::Url;

use crate::config::{S3Config, S3Credentials};

/// Resolve the settings and credentials needed to talk to the S3 bucket of
/// `bucket_url`.
///
/// Either the S3 settings are configured explicitly (through the CLI or the
/// configuration file), in which case the credentials come from the CLI or from
/// the authentication storage of `rattler auth` / `pixi auth`, or nothing is
/// configured and everything is resolved through the AWS SDK, which covers
/// environment variables, `~/.aws/config` profiles (including SSO), and instance
/// metadata.
///
/// Note that credentials are resolved once, so temporary credentials that expire
/// during a long mirror run are not refreshed.
pub(crate) async fn resolve_s3_credentials(
    bucket_url: &Url,
    s3_config: Option<&S3Config>,
    credentials: Option<&S3Credentials>,
    auth_storage: &AuthenticationStorage,
) -> miette::Result<ResolvedS3Credentials> {
    // Which endpoint to talk to, and the credentials to fall back on if we have
    // none of our own.
    let (endpoint_url, region, addressing_style, from_sdk) = match s3_config {
        Some(s3_config) => (
            s3_config.endpoint_url.clone(),
            s3_config.region.clone(),
            if s3_config.force_path_style {
                S3AddressingStyle::Path
            } else {
                S3AddressingStyle::VirtualHost
            },
            None,
        ),
        None => {
            let from_sdk = ResolvedS3Credentials::from_sdk()
                .await
                .into_diagnostic()
                .wrap_err(format!(
                    "failed to resolve the S3 configuration of {bucket_url} through the AWS SDK. \
                     Log in with `aws sso login` if you are using AWS SSO, or configure the \
                     endpoint URL, region and addressing style explicitly"
                ))?;
            (
                from_sdk.endpoint_url.clone(),
                from_sdk.region.clone(),
                from_sdk.addressing_style,
                Some(from_sdk),
            )
        }
    };

    // Credentials passed explicitly take precedence over the ones from the
    // authentication storage, which take precedence over the ones from the AWS SDK.
    rattler_s3::S3Credentials {
        endpoint_url,
        region,
        addressing_style,
        access_key_id: credentials.map(|credentials| credentials.access_key_id.clone()),
        secret_access_key: credentials.map(|credentials| credentials.secret_access_key.clone()),
        session_token: credentials.and_then(|credentials| credentials.session_token.clone()),
    }
    .resolve(bucket_url, auth_storage)
    .or(from_sdk)
    .ok_or_else(|| {
        miette::miette!(
            help = "pass them explicitly, log in with `pixi auth login`, or drop the S3 settings \
                    to resolve everything through the AWS SDK",
            "missing S3 credentials for {bucket_url}"
        )
    })
}
