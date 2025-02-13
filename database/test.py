import pandas as pd

data = {"first" : [10, 20, 30], "second" : [40, 50, 60], "third" : [70, 80, 90]}

dataTypes = tuple([type(i) for i in data.values()])

print([tuple([data[columns][rows] for columns in data]) for rows in range(3)])

for columns in data:
    for rows in range(3):
        data[columns][rows]