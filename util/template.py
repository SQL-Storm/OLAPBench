import itertools
import string
from typing import List


def unfold(d: dict) -> List[dict]:
    """
    Unfolds a dictionary with list values into a list of dictionaries with all possible combinations of the values.

    Args:
        d (dict): A dictionary where the values are either lists or single elements.

    Returns:
        List[dict]: A list of dictionaries, each representing a unique combination of the input dictionary's values.
    """
    if not d:
        return [{}]

    keys, values = zip(*((k, v if isinstance(v, list) else [v]) for k, v in d.items()))
    return [dict(zip(keys, combination)) for combination in itertools.product(*values)]


class Template(string.Template):
    idpattern = r"""(?a:[_.a-z][_.a-z0-9]*)"""
