import os
import pytest
import json
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# List of ALL required environment variables
REQUIRED_ENV_VARS = [
    "BASE_URL",
    "USERNAME",
    "PASSWORD",
    "HOST",
    "HOST_USERNAME",
    "HOST_PASSWORD",
    "INSTANCE_TYPE",
    "SCVMM_LAYOUT_CODE",
    "HOST_NAME",
    "NETWORK_NAME",
    "SCVMM_TEMPLATE_NAME",
    "SERVER_TYPE_NAME",
    "CLUSTER_TYPE",
    "CLUSTER_LAYOUT_NAME",
]

@pytest.fixture(scope="session", autouse=True)
def validate_required_env():
    """Fail the entire test run if any required env var is missing/empty/invalid."""
    missing = []

    for var in REQUIRED_ENV_VARS:
        value = os.getenv(var)
        if not value or value.strip() == "":
            missing.append(var)

    # Extra: validate VOLUMES is valid JSON
    volumes = os.getenv("VOLUMES")
    if volumes:
        try:
            json.loads(volumes)
        except json.JSONDecodeError:
            pytest.fail(" VOLUMES must be valid JSON")

    if missing:
        pytest.fail(f" Missing required environment variables: {', '.join(missing)}")
