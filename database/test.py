import pandas as pd

temp = {"first" : [1,2,3], "second" : [2,3,4], "third" : [4, 5, 6]}

df = pd.DataFrame(temp)

temp2 = range(len(list(temp.keys())))

for i in temp2:
    print(i)