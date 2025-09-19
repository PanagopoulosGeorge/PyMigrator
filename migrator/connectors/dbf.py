from __future__ import annotations
from typing import Dict, Iterator, List, Any, Optional
import os
from dbfread import DBF
import pandas as pd
from .base import BaseConnector
from .parsers import ParseDBFb

GREEK_ENCODING = 'cp737'

class DBFConnector(BaseConnector):
	def __init__(self, root_dir: Optional[str] = None):
		self.root_dir = root_dir
		

	def get_table_metadata(self, path) -> Dict[str, Any]:
		parser = ParseDBFb(path, GREEK_ENCODING)
		columns: List[Dict[str, Any]] = []
		for field in parser.metadata:
			columns.append({
				"name": field[0],
				"type": field[1],
				"length": field[2],
				"decimal_count": field[3],
			})
		return {
			"table_name": os.path.splitext(parser.path)[0],
			"columns": columns,
			"row_count": len(parser.data),
		}

	def stream_rows(self, path, chunksize: int = 5000) -> Iterator[pd.DataFrame]:
		parser = ParseDBFb(path, GREEK_ENCODING)
		batch = []
		for rec in parser.data:
			col_mapper = lambda x: x[0]
			dict_rec = dict(zip(map(col_mapper, parser.metadata), rec))
			batch.append(dict_rec)
			if len(batch) >= chunksize:
				yield pd.DataFrame.from_records(batch)
				batch = []
		if batch:
			yield pd.DataFrame.from_records(batch)
