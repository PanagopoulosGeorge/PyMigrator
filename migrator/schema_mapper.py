from __future__ import annotations
import re
import unicodedata
from typing import Dict, Any
common_large_columns = ["KEIMENO"]
# Minimal set; extend as needed
ORACLE_RESERVED_WORDS = {
	"ACCESS","ADD","ALL","ALTER","AND","ANY","AS","ASC","AUDIT","BETWEEN","BY",
	"CHAR","CHECK","CLUSTER","COLUMN","COMMENT","COMPRESS","CONNECT","CREATE",
	"CURRENT","DATE","DECIMAL","DEFAULT","DELETE","DESC","DISTINCT","DROP",
	"ELSE","EXCLUSIVE","EXISTS","FILE","FLOAT","FOR","FROM","GRANT","GROUP",
	"HAVING","IDENTIFIED","IMMEDIATE","IN","INCREMENT","INDEX","INITIAL","INSERT",
	"INTEGER","INTERSECT","INTO","IS","LEVEL","LIKE","LOCK","LONG","MAXEXTENTS",
	"MINUS","MLSLABEL","MODE","MODIFY","NOAUDIT","NOCOMPRESS","NOT","NOWAIT",
	"NULL","NUMBER","OF","OFFLINE","ON","ONLINE","OPTION","OR","ORDER","PCTFREE",
	"PRIOR","PRIVILEGES","PUBLIC","RAW","RENAME","RESOURCE","REVOKE","ROW",
	"ROWID","ROWNUM","ROWS","SELECT","SESSION","SET","SHARE","SIZE","SMALLINT",
	"START","SUCCESSFUL","SYNONYM","SYSDATE","TABLE","THEN","TO","TRIGGER",
	"UID","UNION","UNIQUE","UPDATE","USER","VALIDATE","VALUES","VARCHAR","VIEW",
	"WHENEVER","WHERE","WITH"
}
# Basic transliteration map for Greek letters (extend as needed)
GREEK_TO_LATIN = {
	"Α": "A", "Β": "B", "Γ": "G", "Δ": "D", "Ε": "E", "Ζ": "Z", "Η": "I", "Θ": "TH",
	"Ι": "I", "Κ": "K", "Λ": "L", "Μ": "M", "Ν": "N", "Ξ": "X", "Ο": "O", "Π": "P",
	"Ρ": "R", "Σ": "S", "Τ": "T", "Υ": "Y", "Φ": "F", "Χ": "CH", "Ψ": "PS", "Ω": "O",
	"Ϊ": "I", "Ϋ": "Y", "ά": "A", "έ": "E", "ή": "I", "ί": "I", "ό": "O", "ύ": "Y", "ώ": "O",
}


def transliterate(text: str) -> str:
	return ''.join(GREEK_TO_LATIN.get(ch, ch) for ch in text)


def clean_table_or_field_name(name: str) -> str:
	# Transliterate and normalize
	t = transliterate(name)
	t = unicodedata.normalize('NFKD', t)
	t = ''.join(ch for ch in t if not unicodedata.combining(ch))
	# Uppercase snake, keep alnum and underscore only
	t = re.sub(r"[^A-Za-z0-9_]+", "_", t)
	t = t.strip('_').upper()
	# Ensure first char is a letter; Oracle identifiers can't start with digit
	if t and t[0].isdigit():
		t = f"A{t}"
	# Truncate to Oracle max identifier length (30)
	if len(t) > 30:
		t = t[:30]
	t = t + "_" if t in ORACLE_RESERVED_WORDS else t
	return t.upper() or "A"


# Map DBF field types to Oracle data types
# dbfread types: C (char), N (number), F (float), D (date), T (datetime), L (logical), M (memo)
def map_type_to_oracle(field, type):
	if type == 'dbf':
		return map_dbf_type_to_oracle(field)
	elif type == 'paradox':
		return map_paradox_type_to_oracle(field)
	else:
		raise ValueError(f"Unsupported mapper type: {type}")


def map_dbf_type_to_oracle(field: Dict[str, Any]) -> str:
	t = field.get("type")
	length = field.get("length") or 0
	dec = field.get("decimal_count") or 0
	if t == 'C':
		# prefer VARCHAR2 for potential unicode
		size = max(1, min(length or 255, 4000))
		return f"VARCHAR2({size})"
	if t in ('N', 'F'):
		# Oracle NUMBER(p,s)
		precision = min(max(length or 10, 1), 38)
		scale = min(max(dec, 0), 127)
		if scale > 0:
			return f"NUMBER({precision},{scale})"
		return f"NUMBER({precision})"
	if t == 'D':
		return "DATE"
	if t == 'T':
		return "TIMESTAMP"
	if t == 'L':
		return "NUMBER(1)"
	if t == 'M':
		return "NCLOB"
	# Fallback
	return "VARCHAR2(4000 CHAR)"


def map_paradox_type_to_oracle(field: Dict[str, Any]) -> str:
    t = field.get("type")  # e.g. "AlphaField"
    length = field.get("length") or 0
    dec = field.get("decimal_count") or 0
    col_name = field.get("name")
    if col_name.upper() in common_large_columns:
        return "CLOB"
    if t == "AlphaField":
        size = 4000
        return f"VARCHAR2({size} CHAR)"
    if t == "DateField":
        return "DATE"
    if t == "TimestampField":
        return "TIMESTAMP"  
    if t in ("NumberField", "CurrencyField", "LongField"):
        precision = min(max(length or 10, 1), 38)
        scale = min(max(dec, 0), 127)
        if scale > 0:
            return f"NUMBER({precision},{scale})"
        return f"NUMBER({precision})"
    if t == "LogicalField":
        return "VARCHAR2(31)"
    if t in ("MemoField", "FormattedMemoField"):
        return "NCLOB"
    if t in ("BlobField", "GraphicField", "OLEField"):
        return "BLOB"
    # Fallback
    return "VARCHAR2(4000 CHAR)"
