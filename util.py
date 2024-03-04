import time

from functools import wraps

def timer(f):
    @wraps(f)
    def timed(*args, **kw):
        start = time.time()
        result = f(*args, **kw)
        end = time.time()

        print(f'func={f.__name__} args=[{args}, {kw}] took: {end-start:2.4f} sec')
        return result

    return timed