import numpy as np
import pandas as pd

data = {
    'Name': ['John', 'Emma', 'Michael', 'Sophia', 'William'],
    'Age': [25, 28, np.nan, 30, 27],
    'Gender': ['Male', 'Female', 'Male', 'Female', 'Male'],
    'City': ['New York', 'Los Angeles', 'Chicago', np.nan, 'Seattle'],
    'Salary': [50000, 60000, 45000, np.nan, 55000],
    'State': ['New York', 'Los Angeles', 'Chicago', np.nan, 'Seattle']
}

df = pd.DataFrame(data)
print(df)

print("First 2 rows only: ")
print(df.head(2))

print("Last 2 rows")
print(df.tail(2))

print(df.columns)
print(df.index)