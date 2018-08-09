#!/usr/bin/env python3

import pandas as pd

df = pd.DataFrame([[1,2,3], 
                   [3,4,5], 
                   [5,6,7],
                   [1,4,9],
                   ])


for i in df.iterrows():
    #a, b, c = i[0], i[1], i[2]
    a, b, c = i

    print(f"{a}-{b}-{c}")

# for x in [ [1,2,3], [4,5,6] ]:

#     a, b, c = x[0], x[1], x[2]

#     print(a, b, c)

# def test():
#     return 1, [2, 3]

# _, i = test()
# a, b = i

# print(f"a={a}, b={b}")