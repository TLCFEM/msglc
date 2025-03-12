import time


def timeit(func):
    def wrapper(*args, **kwargs):
        print(
            f"Calling function '{func.__name__}' with arguments: args={[arg for arg in args if isinstance(arg, int | str)]}."
        )
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"Function '{func.__name__}' executed in {duration:.6f} seconds.")
        return duration, 0

    return wrapper


def get_color(input: str):
    if "msg" in input:
        return "red"
    if "compressed" in input:
        return "blue"
    if "h5" in input:
        return "green"
    return "black"
