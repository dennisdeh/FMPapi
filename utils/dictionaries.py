import pandas as pd
import ast
from typing import Union, Dict, Any
from copy import deepcopy
import random

"""
Small utils to help with dicts
"""


def replace_value_if_key_in_2(d1: dict, d2: dict):
    """
    A util that replaces values in the first dict by the values
    in the second dict, only if the key is present there.

    Parameters
    ----------
    d1: dict
        Dictionary to be updated
    d2: dict
        Dictionary with new values

    Returns
    -------
    dict
    """
    assert isinstance(d1, dict) and isinstance(
        d2, dict
    ), "both inputs must be dictionaries"

    for key in d1.keys():
        if key in d2.keys():
            d1[key] = d2[key]
        else:
            pass
    return d1


def rename_key(d: dict, key_old: str, key_new: str):
    """
    Renames the key in the dictionary and returns
    an updated dict.

    Parameters
    ----------
    d
    key_old
    key_new

    Returns
    -------
    dict
    """
    assert not key_new in d, "Key {} already exists".format(key_new)
    if key_old in d:
        d[key_new] = deepcopy(d[key_old])
        del d[key_old]
    else:
        pass
    return d


def str_to_dict(string: str):
    """
    Convert string to dict. Does not work when the str contains
    lists or tuples.
    The better function to use is ast.literal_eval() is now used.

    NB: Something like np.nan cannot be a value and should be
    replaced by None, and then convert afterward!
    """
    assert (
        string.find("nan") < 0
    ), "cannot handle nan or np.nan; must be converted to None first and converted back afterward"
    return ast.literal_eval(string)


def dict_to_str_formatted(d: dict, indent: int = 3):
    """
    This util converts a dict to a formatted string
    that can be printed and is easy to read
    """
    str_indents = " " * indent
    string = (
        d.__str__()[:-1]
        .replace(", ", f"\n{str_indents}")
        .replace("{", str_indents)
        .replace("'", "")
    )
    return string


def dict_first_key(d: dict):
    """
    Returns the key of the input dict
    """
    return list(d.keys())[0]


def dict_first_val(d: dict):
    """
    Returns the value of the first key of the input dict
    """
    return d[list(d.keys())[0]]


def dict_to_kwarg_str(d: dict):
    """
    Converts a dict to a kwarg string that can be passed using
    the execute parsing
    """
    ls = []
    for key, value in d.items():
        if isinstance(value, str):
            ls.append(f"{key}='{value}'")
        else:
            ls.append(f"{key}={value}")

    return ", ".join(ls)


def dict_filter_keys_starting_with(d: dict, start_str: Union[list, str]):
    """
    Filters out keys starting with start_str and returns
    the filtered dict.
    """
    d_filtered = {}
    if isinstance(start_str, str):
        start_str = [start_str]
    elif isinstance(start_str, list):
        pass
    else:
        raise TypeError("start_str must be str or list")
    for k, v in d.items():
        flag = True
        for ss in start_str:
            if k.find(ss) == 0:
                flag = False
        if flag:
            d_filtered[k] = v
    return d_filtered


def excel_to_dict(file: str):
    """
    Convert an Excel file with two columns to a dict:
        column 1: key
        column 2: value (automatically tries to parse using
            ast.literal_eval, otherwise value is kept)
    The util has its limitations: lists are usually not
    parsed correctly.
    """
    # read Excel file and get
    df = pd.read_excel(file, index_col=0).reset_index()
    colnames = df.columns
    assert len(colnames) == 2, "input excel file does not have 2 columns"
    df = df.rename(columns={colnames[0]: "key", colnames[1]: "value"}).set_index("key")
    # convert to dictionary
    d = df.to_dict(orient="dict")["value"]
    # parse values that are strings
    for key in d:
        if isinstance(d[key], str):
            try:
                d[key] = ast.literal_eval(d[key])
            except (ValueError, SyntaxError):
                pass
        else:
            continue
    return d


def sorted_filter_until_key(d: dict, key_end: Union[str, None], reverse: bool = False):
    """
    Keep only entries in the dictionary (sorted by keys),
    until from key_end (exclusive)
    """
    assert isinstance(d, dict), "d must be dict"
    assert isinstance(key_end, str) or key_end is None, "key_end must be str or None"
    assert isinstance(reverse, bool), "reverse must be bool"
    # get sorted keys
    keys_sorted = sorted(d.keys(), reverse=reverse)
    if key_end is None or not (key_end in keys_sorted):
        print("key_end not in dictionary")
    filtered_dict = dict()
    for key in keys_sorted:
        if key == key_end:
            break
        filtered_dict[key] = d[key]
    return filtered_dict


def sorted_filter_after_key(d: dict, key_end: Union[str, None], reverse: bool = False):
    """
    Filters out all entries in the dictionary (sorted by keys),
    starting from key_end (inclusive)
    """
    assert isinstance(d, dict), "d must be dict"
    assert isinstance(key_end, str) or key_end is None, "key_end must be str or None"
    assert isinstance(reverse, bool), "reverse must be bool"
    # get sorted keys
    keys_sorted = sorted(d.keys(), reverse=reverse)
    if key_end is None or not (key_end in keys_sorted):
        print("key_end not in dictionary")
    filtered_dict = dict()
    bool_key = False
    for key in keys_sorted:
        if key == key_end or bool_key:
            filtered_dict[key] = d[key]
            bool_key = True
        else:
            continue
    return filtered_dict


def count_keys_nested(d: dict, level: int = 0) -> Union[int, Dict[Any, Any]]:
    """
    Counts keys in a nested dictionary based on the specified level parameter
    recursively.

    Parameters
    ----------
    d : dict
        The nested dictionary to analyse
    level : int, optional
        The level parameter that determines the behaviour:
        * level = 0: Returns the total count of keys at the deepest level
        * level = n (n>0): Returns a dictionary with the structure preserved up to level n,
                          with values being the count of keys at that level

    Returns
    -------
    Union[int, dict]
        Depending on the level parameter:
        * Integer count of keys at the deepest level (level=0)
        * Dictionary with nested structure preserved up to specified level,
          with values replaced by key counts

    Examples
    --------
    >>> d = {"a": 1, "b": {"c": 2, "d": 3}, "e": {"f": {"g": 4, "h": 5}}}
    >>> count_keys_nested(d, level=0)
    5
    >>> count_keys_nested(d, level=1)
    {"a": 1, "b": 2, "e": 2}
    >>> count_keys_nested(d, level=2)
    {"a": 1, "b": {"c": 1, "d": 1}, "e": {"f": {"g": 1, "h": 1}}}
    >>> count_keys_nested(d, level=3)
    {"a": 1, "b": {"c": 1, "d": 1}, "e": {"f": {"g": 1, "h": 1}}}
    """
    # 0: assertions
    assert isinstance(d, dict), "d must be a dictionary"
    assert isinstance(level, int), "level must be an integer"
    assert level >= 0, "level must be greater than or equal to 0"

    # 1: For non-dictionary inputs in recursive calls
    if not isinstance(d, dict):
        return 1 if level > 0 else 0

    # 2: Level 0: Count keys at the deepest level
    if level == 0:
        has_dict_values = False
        count = 0

        for value in d.values():
            if isinstance(value, dict):
                has_dict_values = True
                count += count_keys_nested(value, level=0)

        # If this is a leaf node (no dictionary values), count its keys
        if not has_dict_values:
            count = len(d)

        return count

    # 3: For levels > 0
    result = {}
    for key, value in d.items():
        if isinstance(value, dict) and level > 1:
            # Continue recursion if value is a dict and we haven't reached the target level
            result[key] = count_keys_nested(value, level=level - 1)
        else:
            # If value is not a dict or we've reached the target level,
            # count the elements (1 for non-dict values, len for dicts)
            result[key] = len(value) if isinstance(value, dict) else 1

    return result


def all_keys_exist(k: list, d: dict):
    """Check if all keys in the list exist in the dictionary"""
    return all(key in d for key in k)


def any_keys_exist(k: list, d: dict):
    """Check if any key in the list exists in the dictionary"""
    return any(key in d for key in k)


def sample_elements(d: dict, n: Union[int, None], seed=None):
    if seed is not None:
        random.seed(seed)
    if n is None:
        return d
    else:
        l_keys = list(d.keys())
        l_keys_sampled = random.sample(l_keys, k=min(n, len(l_keys)))
        d_sampled = {k: d[k] for k in l_keys_sampled}
        return d_sampled


if __name__ == "__main__2":
    d = {"A": "a", "B": "b", "C": "c"}
    d.__str__()
    dict_to_str_formatted(d, indent=3)

    # filter until a given key
    sorted_filter_until_key(d, key_end="a")
    sorted_filter_until_key(d, key_end="A", reverse=True)

    # filter after a given key
    sorted_filter_after_key(d, key_end="A")
    sorted_filter_after_key(d, key_end="A", reverse=True)

    # rename key
    rename_key(d, key_old="A", key_new="new")

    # count
    d = {"a": {"1": 3, "2": 4}, "b": {"c": 2, "d": 3}, "e": {"f": {"g": 4, "h": 5}}}
    d = {"a": {"1": 3, "2": 4}, "b": {"c": 2, "d": 3}, "e": {"g": 4, "h": 5}}
    count_keys_nested(d, level=2)
