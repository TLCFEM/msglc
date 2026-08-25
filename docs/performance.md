# Performance

The writer/encoder supports extra Python types in addition to standard JSON compatible types (`int`, `float`, `bool`, `str`, `list` and `dict`).
This comes at a cost of heavy Python side type check.

The performance can be further improved if some features are not used.

## Plain Container

If the target data only contains standard Python `list` and `dict`, disable custom container check.

```python
from msglc.config import configure

configure(enable_custom_container=False)
```

## No Other List Types

By default, `set`s will be sorted and packed as plain lists, `tuple`s will be packed as plain lists, `numpy.ndarray` can be conditionally converted to plain lists or directly encoded as binary blob using `numpy`'s own encoder.
Those can be disabled if the target data does not contain those objects.

```python
from msglc.config import configure

configure(has_numpy=False, has_set=False, has_tuple=False)
```
