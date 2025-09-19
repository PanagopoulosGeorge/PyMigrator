from __future__ import annotations
from typing import Dict, Iterator, List, Any, Optional, Protocol
import pandas as pd


# migrator/connectors/base.py
class BaseConnector(Protocol):
	def get_table_metadata(self, path: str) -> Dict[str, Any]:
		...
	def stream_rows(self, path: str, chunksize: int = 5000) -> Iterator[pd.DataFrame]:
		...  # add path param to match usage

