import math
import time
def round_up(number, decimal_places=0):
    factor = 10.0 ** decimal_places
    return math.ceil(number * factor) / factor

def round_down(number, decimal_places=0):
    factor = 10.0 ** decimal_places
    return math.floor(number * factor) / factor

def get_ts():
    return int(time.time() * 1000)