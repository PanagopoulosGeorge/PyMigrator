from __future__ import annotations
import os
from typing import Any, Dict, List, Optional
import yaml

class ConfigError(Exception):
	pass


def load_config(path: str) -> Dict[str, Any]:
	if not os.path.exists(path):
		raise ConfigError(f"Config file not found: {path}")
	with open(path, "r", encoding="utf-8") as f:
		data = yaml.safe_load(f) or {}

	# Basic validation
	if "oracle" not in data:
		raise ConfigError("Missing 'oracle' section in config")
	oracle = data["oracle"]
	for key in ["conn", "username", "password"]:
		if key not in oracle or not oracle[key]:
			raise ConfigError(f"oracle.{key} is required")

	sources = data.get("source", {})
	if not sources:
		raise ConfigError("Missing 'source' section in config")

	# Normalize tables list
	tables: List[Dict[str, Any]] = sources.get("tables", []) or []
	for t in tables:
		if "path" not in t:
			raise ConfigError("Each table entry must include 'path'")
		if "target_table" not in t:
			raise ConfigError("Each table entry must include 'target_table'")

	return data
