from __future__ import annotations


def is_index(key: int | str | list):
    if isinstance(key, int):
        return True

    if isinstance(key, list):
        return all(is_index(k) for k in key)

    if key.isdigit():
        return True

    if len(key) > 1 and key[0] == "-" and key[1:].isdigit():
        return True

    return False


def normalise_index(index: int | str, total_size: int):
    if isinstance(index, str):
        index = int(index)

    while index < -total_size:
        index += total_size
    while index >= total_size:
        index -= total_size
    return index


def normalise_bound(index: int | str, total_size: int):
    if isinstance(index, str):
        index = int(index)

    while index < 1 - total_size:
        index += total_size
    while index >= 1 + total_size:
        index -= total_size
    return index


def is_slice(key: str, total_size: int):
    parts: list = list(key.split(":"))

    if len(parts) == 2:
        start, stop = parts
        if start == "":
            parts[0] = 0
        if stop == "":
            parts[1] = total_size

        if is_index(parts):
            start, stop = normalise_index(parts[0], total_size), normalise_bound(parts[1], total_size)
            step = 1 if start < stop else -1
            return start, stop, step

        return None

    if len(parts) == 3:
        start, step, stop = parts
        if start == "":
            parts[0] = 0
        if stop == "":
            parts[2] = total_size

        if is_index(parts):
            start, stop = normalise_index(parts[0], total_size), normalise_bound(parts[2], total_size)
            return start, stop, int(step)

        return None

    return None
