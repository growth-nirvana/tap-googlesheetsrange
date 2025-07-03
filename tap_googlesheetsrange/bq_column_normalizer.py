import re
from typing import List, Set, Optional


def normalize_bq_column_names(
    headers: List[str],
    reserved_keywords: Optional[Set[str]] = None,
    forbidden_prefixes: Optional[List[str]] = None,
) -> List[str]:
    """
    Normalize a list of column names to be BigQuery-compliant.

    Args:
        headers: The original column names.
        reserved_keywords: Set of reserved keywords to avoid.
        forbidden_prefixes: List of forbidden prefixes.

    Returns:
        List of normalized column names.
    """
    reserved_keywords = set(reserved_keywords or [])
    forbidden_prefixes = [p.lower() for p in (forbidden_prefixes or [])]
    normalized = []
    seen = {}
    for orig in headers:
        # 1. Lowercase
        col = orig.lower()
        # 2. Replace non-alphanumeric/underscore with _
        col = re.sub(r"[^a-z0-9_]+", "_", col)
        # 3. If starts with forbidden prefix, prepend an extra underscore (before stripping underscores)
        for prefix in forbidden_prefixes:
            if col.startswith(prefix):
                col = f"_{col}"
                break
        # 4. Remove trailing underscores only
        col = col.rstrip("_")
        # 5. If starts with digit, prepend _
        if col and col[0].isdigit():
            col = f"_{col}"
        # 6. If empty after cleaning, use 'column'
        if not col:
            col = "column"
        # 7. If reserved keyword, append _col
        if col in reserved_keywords:
            col = f"{col}_col"
        # 8. Truncate to 300 chars
        col = col[:300]
        # 9. Ensure uniqueness
        base_col = col
        i = 2
        while col in seen:
            col = f"{base_col}_{i}"
            i += 1
        seen[col] = True
        normalized.append(col)
    return normalized 