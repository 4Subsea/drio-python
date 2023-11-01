import datareservoirio as drio
import numpy as np
import pandas as pd

auth = drio.Authenticator()
# Follow instructions to authenticate

client = drio.Client(auth)

start_end = pd.date_range(start="2024-01-01 00:00", end="2024-02-01 00:00", freq="1D")
start_end_iter = zip(start_end[:-1], start_end[1:])

series_id = "f9ca79b9-a840-447e-881b-ad6bd5d635a6"


# Get timeseries in chunks
for start, end in start_end_iter:
    print(f"get data {start}")
    timeseries = client.get(series_id, start=start, end=end, raise_empty=True)