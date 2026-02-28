"""
Utility functions for configuration and secret validation.
"""
from typing import Any

from quackpipe.exceptions import ValidationError
from quackpipe.secrets import fetch_secret_bundle


def get_merged_params(config: dict[str, Any], secret_name: str | None = None, resolve_secrets: bool = False) -> dict[str, Any]:
    """
    Merges configuration with secrets if resolve_secrets is True.
    """
    params = config.copy()
    if resolve_secrets and secret_name:
        secrets = fetch_secret_bundle(secret_name)
        params.update(secrets)
    return params


def validate_required_fields(
    params: dict[str, Any],
    required_fields: list[str],
    source_type: str,
    secret_name: str | None = None,
    resolve_secrets: bool = False
):
    """
    Validates that the required fields are present in the parameters.

    - If resolve_secrets is True: All required_fields MUST be present in 'params'.
    - If resolve_secrets is False: If 'secret_name' is provided, we skip the check
      (optimistic/static validation). If no 'secret_name' is provided, the fields
      must be in 'params'.
    """
    # If we are NOT resolving secrets and we HAVE a secret_name, we assume the
    # required fields will be provided by the secret later (Static/YAML validation mode).
    if not resolve_secrets and secret_name:
        return

    missing_fields = [field for field in required_fields if field not in params]

    if missing_fields:
        fields_str = ", ".join([f"'{f}'" for f in missing_fields])
        msg = f"{source_type.capitalize()} source requires {fields_str}"

        if secret_name and resolve_secrets:
            msg += f" (Checked both config and environment variables for secret name: '{secret_name}')"
        elif not secret_name:
            msg += " in its configuration."
        else:
            msg += "."

        raise ValidationError(msg)
