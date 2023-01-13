import pandas as pd
import os

path = os.getcwd()+"/"

postcodes = pd.read_csv(f"{path}/open_postcode_geo.csv", header=None, usecols=[0, 1],
                        names=['postcode', 'status'])
postcodes = postcodes[(postcodes['status'] == 'live')]
postcodes = postcodes.drop('status', axis=1)

postcodes.to_csv(f'{path}/postcodes-small.csv', index=False)