import time
import numpy as np


start_time = time.time()
n = np.arange(1000000000).reshape((100000, 10000))
n2 = n * 1
print("multiply by 1 copy: %s seconds " % (time.time() - start_time))

start_time = time.time()
n = np.arange(1000000000).reshape((100000, 10000))
n2 = n.copy()
print("numpy copy: %s seconds " % (time.time() - start_time))
