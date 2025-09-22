import os
import yaml

# Configuration
root_dir = r"C:\dumps\DB-paradox"
schema_name = "SCHEMA_NAME"

# Oracle connection details
oracle_cfg = {
    "oracle": {
        "conn": "IP_ADDR:1521/apppdb19c",
        "username": "DEV_USER",
        "password": "DEV_PASS"
    }
}

# Collect all .DB files
tables_cfg = []
for fname in sorted(os.listdir(root_dir)):
    if fname.lower().endswith(".db"):
        table_name = os.path.splitext(fname)[0]  # remove .DB extension
        tables_cfg.append({
            "path": os.path.join(root_dir, fname),
            "target_table": table_name,
            "schema": schema_name,
            "drop_before_load": True
        })

# Wrap everything
config = oracle_cfg
config["source"] = {
    "type": "paradox",
    "root_dir": root_dir,
    "tables": tables_cfg
}

# Dump to YAML
out_file = "paradox_configgen.yml"
with open(out_file, "w", encoding="utf-8") as f:
    yaml.dump(config, f, sort_keys=False, allow_unicode=True)

print(f"Config written to {out_file}")
