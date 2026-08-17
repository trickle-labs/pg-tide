#!/usr/bin/env python3
"""Static checks for dashboard, alert, and runbook contracts."""
import json, re, sys
from pathlib import Path
try:
    import yaml
except ImportError:
    yaml = None
root = Path(__file__).resolve().parents[1]
metrics = set(re.findall(r'"(pg_tide_[a-zA-Z0-9_]+)"', (root/'pg-tide-relay/src/metrics.rs').read_text()))
errors=[]
for path in sorted((root/'pg-tide/dashboards').glob('*.json')):
    try: data=json.loads(path.read_text())
    except Exception as e: errors.append(f'{path}: invalid JSON: {e}'); continue
    text=json.dumps(data)
    for name in set(re.findall(r'pg_tide_[a-zA-Z0-9_]+', text)):
        if name.removesuffix('_bucket').removesuffix('_count').removesuffix('_sum') not in metrics: errors.append(f'{path}: unknown metric {name}')
    for p in data.get('panels',[]):
        if path.name == 'relay-health.json' and p.get('type') != 'row' and not p.get('description'): errors.append(f'{path}: panel {p.get("title")} lacks operator metadata')
alerts=root/'pg-tide/dashboards/alerts.yaml'
if yaml:
    try:
        doc=yaml.safe_load(alerts.read_text())
        for group in doc.get('groups',[]):
            for rule in group.get('rules',[]):
                value=rule.get('annotations',{}).get('description','')
                if 'runbook' not in value.lower(): errors.append(f'{alerts}: {rule.get("alert")} lacks runbook annotation')
                for name in set(re.findall(r'pg_tide_[a-zA-Z0-9_]+', str(rule.get('expr','')))):
                    if name.removesuffix('_bucket').removesuffix('_count').removesuffix('_sum') not in metrics: errors.append(f'{alerts}: unknown metric {name}')
    except Exception as e: errors.append(f'{alerts}: invalid YAML: {e}')
else:
    # Ruby ships with macOS and GitHub runners; keep this validator dependency-free.
    import subprocess
    try: subprocess.run(['ruby','-e','require "yaml"; YAML.load_stream(File.read(ARGV[0]))',str(alerts)],check=True,capture_output=True)
    except Exception as e: errors.append(f'{alerts}: invalid YAML: {e}')
    for name in set(re.findall(r'pg_tide_[a-zA-Z0-9_]+', '\n'.join(line for line in alerts.read_text().splitlines() if 'expr:' in line))):
        if name.removesuffix('_bucket').removesuffix('_count').removesuffix('_sum') not in metrics: errors.append(f'{alerts}: unknown metric {name}')
    if 'runbook_url:' not in alerts.read_text(): errors.append(f'{alerts}: no runbook annotations')
manifest=root/'docs/runbook-evidence.toml'
if manifest.exists():
    import tomllib
    try:
        data=tomllib.loads(manifest.read_text())
        required={'id','path','alerts','tests','workflow','owner','last_reviewed'}
        for item in data.get('runbook',[]):
            missing=required-set(item)
            if missing: errors.append(f'{manifest}: {item.get("id", "unknown")} missing {sorted(missing)}')
            if not (root/item['path']).exists(): errors.append(f'{manifest}: missing {item["path"]}')
    except Exception as e: errors.append(f'{manifest}: invalid TOML: {e}')
if errors:
    print('\n'.join(errors)); sys.exit(1)
print('observability contracts valid')
