import time
from os import getpid

import psutil


def timeit(func):
    def wrapper(*args, **kwargs):
        print(
            f"Calling function '{func.__name__}' with arguments: args={[arg for arg in args if isinstance(arg, int | str)]}."
        )

        process = psutil.Process(getpid())
        peak_memory = process.memory_info().rss

        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()

        peak_memory = max(peak_memory, process.memory_info().rss)

        duration = end_time - start_time
        print(f"Function '{func.__name__}' executed in {duration:.6f} seconds.")
        return duration, peak_memory / 1024

    return wrapper


def get_color(input: str):
    if "msg" in input:
        return "red"
    if "compressed" in input:
        return "blue"
    if "h5" in input:
        return "green"
    return "black"
