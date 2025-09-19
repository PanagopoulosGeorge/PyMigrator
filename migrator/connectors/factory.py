from __future__ import annotations
from typing import Any, Dict

from .dbf import DBFConnector
from .paradox import ParadoxConnector
from .base import BaseConnector


def create_connector(sources_cfg: Dict[str, Any]) -> BaseConnector:
	"""
	Create a source connector based on the `sources` section of the config.

	Backwards compatible behavior:
	- If `type` is omitted and `root_dir` is present, use DBF connector.
	- If `type` is "dbf", expect `root_dir` (optional if table paths are absolute).
	"""
	connector_type = (sources_cfg.get("type") or "").strip().lower()
	if not connector_type:
		# Heuristic for legacy config
		if "dbf_dir" in sources_cfg:
			connector_type = "dbf"
		else:
			raise ValueError("Unable to infer connector type. Please set sources.type, e.g., 'dbf'.")

	if connector_type == "dbf":
		return DBFConnector(root_dir=sources_cfg.get("root_dir"))
	elif connector_type == "paradox":
		return ParadoxConnector(root_dir=sources_cfg.get("root_dir"))

	raise ValueError(f"Unsupported connector type: {connector_type}")


