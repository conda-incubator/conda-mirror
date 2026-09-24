use miette::{Context, IntoDiagnostic};
use rattler_networking::AuthenticationStorage;
use rattler_s3::{ResolvedS3Credentials, S3AddressingStyle};
use url::Url;

use crate::config::{S3Config, S3Credentials};

/// Resolve the settings and credentials needed to talk to the S3 bucket of
/// `bucket_url`.
///
/// Anything that is configured explicitly (through the CLI or the configuration
/// file) takes precedence. Credentials that are not passed explicitly are looked
/// up in the authentication storage of `rattler auth` / `pixi auth`. Whatever is
/// left is resolved through the AWS SDK, which covers environment variables,
/// `~/.aws/config` profiles (including SSO), and instance metadata.
///
/// Note that credentials are resolved once, so temporary credentials that expire
/// during a long mirror run are not refreshed.
pub(crate) async fn resolve_s3_credentials(
    bucket_url: &Url,
    s3_config: Option<&S3Config>,
    credentials: Option<&S3Credentials>,
    auth_storage: &AuthenticationStorage,
) -> miette::Result<ResolvedS3Credentials> {
    let endpoint_url = s3_config.and_then(|config| config.endpoint_url.clone());
    let region = s3_config.and_then(|config| config.region.clone());
    let addressing_style =
        s3_config
            .and_then(|config| config.force_path_style)
            .map(|force_path_style| {
                if force_path_style {
                    S3AddressingStyle::Path
                } else {
                    S3AddressingStyle::VirtualHost
                }
            });

    // If the endpoint and region are known we can try to resolve the credentials
    // without involving the AWS SDK at all.
    if let (Some(endpoint_url), Some(region)) = (endpoint_url.clone(), region.clone())
        && let Some(resolved) = partial_credentials(
            endpoint_url,
            region,
            addressing_style.unwrap_or_default(),
            credentials,
        )
        .resolve(bucket_url, auth_storage)
    {
        return Ok(resolved);
    }

    // Otherwise, ask the AWS SDK and layer the explicit configuration on top of
    // what it found.
    let from_sdk = match ResolvedS3Credentials::from_sdk().await {
        Ok(from_sdk) => from_sdk,
        Err(err) => {
            // The AWS SDK has nothing to offer. That is fine as long as the endpoint
            // and the credentials are configured explicitly, in which case only the
            // region needs to be known.
            if let Some(region) = region
                && let Some(resolved) = partial_credentials(
                    endpoint_url.unwrap_or(aws_global_endpoint()),
                    region,
                    addressing_style.unwrap_or_default(),
                    credentials,
                )
                .resolve(bucket_url, auth_storage)
            {
                tracing::debug!(
                    "could not resolve the S3 configuration of {bucket_url} through the AWS SDK ({err}), using the configured settings"
                );
                return Ok(resolved);
            }
            return Err(err).into_diagnostic().wrap_err(format!(
                "failed to resolve the S3 configuration of {bucket_url} through the AWS SDK. \
                 Log in with `aws sso login` if you are using AWS SSO, or configure the endpoint \
                 URL, region and credentials explicitly"
            ));
        }
    };
    let endpoint_url = endpoint_url.unwrap_or(from_sdk.endpoint_url);
    let region = region.unwrap_or(from_sdk.region);
    let addressing_style = addressing_style.unwrap_or(from_sdk.addressing_style);

    Ok(partial_credentials(
        endpoint_url.clone(),
        region.clone(),
        addressing_style,
        credentials,
    )
    .resolve(bucket_url, auth_storage)
    .unwrap_or(ResolvedS3Credentials {
        endpoint_url,
        region,
        addressing_style,
        access_key_id: from_sdk.access_key_id,
        secret_access_key: from_sdk.secret_access_key,
        session_token: from_sdk.session_token,
    }))
}

/// The global AWS S3 endpoint, used as a last resort when the AWS SDK cannot tell
/// us which endpoint to talk to. Opendal turns this into the regional endpoint.
fn aws_global_endpoint() -> Url {
    Url::parse("https://s3.amazonaws.com").expect("the global endpoint is a valid URL")
}

/// Combine the S3 settings with the explicitly passed credentials, ready to be
/// resolved against the authentication storage.
fn partial_credentials(
    endpoint_url: Url,
    region: String,
    addressing_style: S3AddressingStyle,
    credentials: Option<&S3Credentials>,
) -> rattler_s3::S3Credentials {
    rattler_s3::S3Credentials {
        endpoint_url,
        region,
        addressing_style,
        access_key_id: credentials.map(|credentials| credentials.access_key_id.clone()),
        secret_access_key: credentials.map(|credentials| credentials.secret_access_key.clone()),
        session_token: credentials.and_then(|credentials| credentials.session_token.clone()),
    }
}
