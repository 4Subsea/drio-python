import timeseriesclient
import numpy as np
import pandas as pd
import logging
import sys

timeseriesclient.set_log_level(logging.DEBUG)
timeseriesclient.logger.addHandler(logging.StreamHandler(sys.stdout))
timeseriesclient.globalsettings.environment.set_qa()

client = timeseriesclient.TimeSeriesClient()

client.authenticate()

df = pd.DataFrame({'a':np.arange(1e6)})

result = client.upload_timeseries(df)

print(result)

