def deep_compare(a, b, exclude=None):
    """Original less strict comparison function kept for backward compatibility."""
    exclude = set(exclude or [])
    if isinstance(a, dict) and isinstance(b, dict):
        return all(
            k in b and deep_compare(v, b[k], exclude)
            for k, v in a.items()
            if k not in exclude
        )
    if isinstance(a, list) and isinstance(b, list):
        return all(deep_compare(x, y, exclude) for x, y in zip(a, b))
    if hasattr(a, "__dict__") and hasattr(b, "__dict__"):
        return deep_compare(vars(a), vars(b), exclude)
    return a == b


def deep_compare_true(a, b, exclude=None):
    """Strict comparison that ensures exact structural matches.

    Args:
        a, b: Objects to compare
        exclude: Set of field names to ignore during comparison

    Returns:
        bool: True if objects are exactly equal (ignoring excluded fields)

    This version:
    - Enforces type equality
    - Ensures collections have same length
    - Verifies dict keys match exactly
    - Sorts sequences before comparing
    """
    exclude = set(exclude or [])

    if type(a) is not type(b):
        return False

    if isinstance(a, dict) and isinstance(b, dict):
        # Check keys match both ways (ensures no extra keys in either dict)
        a_keys = {k for k in a.keys() if k not in exclude}
        b_keys = {k for k in b.keys() if k not in exclude}
        if a_keys != b_keys:
            return False
        return all(
            deep_compare_true(v, b[k], exclude)
            for k, v in a.items()
            if k not in exclude
        )

    if isinstance(a, (list, set, tuple)):
        if len(a) != len(b):
            return False
        return all(
            deep_compare_true(x, y, exclude) for x, y in zip(sorted(a), sorted(b))
        )

    if hasattr(a, "__dict__") and hasattr(b, "__dict__"):
        return deep_compare_true(vars(a), vars(b), exclude)

    return a == b

