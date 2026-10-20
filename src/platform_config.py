"""Shared names and paths for the Databricks notebooks and the tested modules."""

CATALOG = "workspace"
SCHEMA = "healthcare"
VOLUME = "landing"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
CLAIMS_PATH = f"{VOLUME_PATH}/claims"
REFERENCE_PATH = f"{VOLUME_PATH}/reference"
MANIFEST_PATH = f"{VOLUME_PATH}/manifest.json"
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/bronze_claims"

REFERENCE_FILES = {
    "patients": "patients.csv",
    "providers": "providers.csv",
    "eligibility": "eligibility.csv",
}


def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


BRONZE_CLAIMS = table("bronze_claims")
BRONZE_PATIENTS = table("bronze_patients")
BRONZE_PROVIDERS = table("bronze_providers")
BRONZE_ELIGIBILITY = table("bronze_eligibility")
SILVER_CLAIMS = table("silver_claims")
QUARANTINE_CLAIMS = table("quarantine_claims")
SILVER_FILE_LOG = table("silver_file_log")

# Raw claim columns, in file order. Bronze keeps all of them as strings.
CLAIM_COLUMNS = [
    "claim_id", "patient_id", "provider_id", "service_date", "submitted_date",
    "dx_code", "proc_code", "billed_amount", "status", "updated_at",
]
