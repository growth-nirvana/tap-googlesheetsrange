import pytest
from tap_googlesheetsrange.bq_column_normalizer import normalize_bq_column_names

# Example reserved keywords and forbidden prefixes for the test
RESERVED_KEYWORDS = {"where", "view"}
FORBIDDEN_PREFIXES = ["_TABLE_", "_FILE_", "_PARTITION", "_ROW_TIMESTAMP", "__ROOT__", "_COLIDENTIFIER"]


def test_normalize_bq_column_names():
    headers = [
        "First Name",         # spaces, mixed case
        "1$Amount",           # starts with number, special char
        "_TABLE_ID",          # forbidden prefix
        "WHERE",              # reserved keyword
        "First Name",         # duplicate
        "weird!@#column",     # special chars
        "a" * 400,            # too long
        "_COLIDENTIFIERfoo",  # forbidden prefix
        "normal_col",         # already compliant
    ]
    expected = [
        "first_name",
        "_1_amount",
        "__table_id",
        "where_col",
        "first_name_2",
        "weird_column",
        "a" * 300,
        "__colidentifierfoo",
        "normal_col",
    ]
    normalized = normalize_bq_column_names(headers, reserved_keywords=RESERVED_KEYWORDS, forbidden_prefixes=FORBIDDEN_PREFIXES)
    assert normalized == expected

    # Test that data stays aligned
    data_row = ["Alice", "100", "foo", "bar", "Bob", "baz", "x" * 400, "y", "z"]
    normalized_row = dict(zip(normalized, data_row))
    assert normalized_row["first_name"] == "Alice"
    assert normalized_row["_1_amount"] == "100"
    assert normalized_row["__table_id"] == "foo"
    assert normalized_row["where_col"] == "bar"
    assert normalized_row["first_name_2"] == "Bob"
    assert normalized_row["weird_column"] == "baz"
    assert normalized_row["a" * 300] == "x" * 400
    assert normalized_row["__colidentifierfoo"] == "y"
    assert normalized_row["normal_col"] == "z" 