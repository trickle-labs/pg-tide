# Repository scripts

These scripts support the commands and workflows that call them. Keep a
script only when its caller, contract, or procedure is listed here.

## Checks and generators

- `check_documentation.py`: validates executable documentation markers.
- `check_flake_registry.py`: validates the flake policy.
- `check_lifecycle_contract.py`: validates lifecycle policy and migrations.
- `check_observability.py`: validates dashboards, alerts, and runbooks.
- `check_operational_budgets.py`: validates operational benchmark data.
- `check_pre_v1_baseline.py`: validates the pre-v1 baseline.
- `check_production_test_controls.sh`: validates production test controls.
- `check_release_artifacts.py`: validates release artifact policy.
- `check_required_tests.py`: validates required-test manifests and results.
- `check_repository_hygiene.py`: validates script ownership and repository hygiene.
- `check_security_contract.py`: validates security evidence contracts.
- `check_supply_chain.py`: validates dependency and advisory policy.
- `check_upgrade_completeness.sh`: validates extension migration packaging.
- `check_v1_artifacts.py`: validates frozen v1 artifacts.
- `check_v1_contracts.py`: validates frozen v1 contracts.
- `check_v1_surface.py`: validates the supported v1 surface.
- `generate_connector_surface.py`: generates connector documentation.
- `generate_operator_errors.py`: generates operator-error catalog outputs.
- `generate_pipeline_schema.sh`: generates the pipeline schema.
- `package_extension.sh`: packages the PostgreSQL extension.

## Test and evidence helpers

- `catalog_snapshot.py`: captures catalog evidence for integration tests.
- `run_quickstart_sql.py`: runs marked Quick Start SQL.
- `run_required_test.py`: records required-test execution evidence.
- `test_extension_cleanroom.py`: runs the packaged extension clean-room test.
