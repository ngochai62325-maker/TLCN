import sys
import yaml

sys.stdout.reconfigure(encoding='utf-8')

with open("src/ingestion/config/source_registry.yaml", "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

sources = data.get("sources", {})
print(f"Total source keys in YAML: {len(sources)}")
for k, v in sources.items():
    aliases = v.get("aliases", [])
    print(f"Key: {k}")
    print(f"  Name: {v.get('source_name')}")
    print(f"  Provider: {v.get('provider')}")
    print(f"  Dataset: {v.get('dataset')}")
    print(f"  Type: {v.get('source_type')}")
    print(f"  Local Path: {v.get('local_path')}")
    print(f"  Local Fallback: {v.get('local_fallback')}")
    print(f"  Endpoint: {v.get('endpoint')}")
    print(f"  Aliases: {aliases}")
    print("-" * 50)
