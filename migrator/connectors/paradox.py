from __future__ import annotations
from ast import parse
from typing import Dict, Iterator, Any, Optional
import os
import pandas as pd
from pypxlib import Table
from .base import BaseConnector


class ParadoxConnector(BaseConnector):
    def __init__(self, root_dir: Optional[str] = None):
        self.root_dir = root_dir

    def get_table_metadata(self, path) -> Dict[str, Any]:
        table = Table(path)
        columns = []

        # table.fields is an OrderedDict {name: FieldClass}
        for name, field in table.fields.items():
            columns.append({
                "name": name,
                "type": type(field).__name__,   # e.g. AlphaField, DateField
                # Paradox doesn't expose length/decimals the same way as DBF.
            })

        return {
            "table_name": os.path.splitext(os.path.basename(path))[0],
            "columns": columns,
            "row_count": len(table),  # Table supports len()
        }

    def stream_rows(self, path, chunksize: int = 5000) -> Iterator[pd.DataFrame]:
        table = Table(path, encoding='cp737', px_encoding='cp737')
        batch = []
        col_names = list(table.fields.keys())

        for row in table:  # row is a Row object
            #row_dict = {col: row[col] for col in col_names}
            row_dict = {}
            parser_error = ""
            for col in col_names:
                try:
                    row_dict[col] = row[col]
                except ValueError as e:
                    parser_error += f"column: {col} parsing error: {e}|"
            row_dict['parser_error'] = parser_error
            batch.append(row_dict)

            if len(batch) >= chunksize:
                yield pd.DataFrame.from_records(batch)
                batch = []

        if batch:
            yield pd.DataFrame.from_records(batch)
