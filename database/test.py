data = {"first" : [10,20,30], "second" : [40,50,60], "third" : [70, 80, 90]}

datatoprint = """first column : %s, second column : %s, third column : %s"""

keysindata = list(data.keys())
size = range(3)

for rows in size:
    print(datatoprint % tuple([data[keysindata[columns]][rows] for columns in range(len(keysindata))]))