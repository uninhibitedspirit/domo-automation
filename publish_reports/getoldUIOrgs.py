import pandas as pd
import numpy as np

us_e = pd.read_csv('us-east.csv')
us_w = pd.read_csv('us-west.csv')
all_is_p_orgs = pd.read_csv('prod_is_picasso.csv')

all_prod_orgs = set((us_e.host_name+us_w.host_name))
all_old_ui = all_prod_orgs - set(all_is_p_orgs.hostname)

all_old_ui_df = pd.Dataframe(all_old_ui, columns=['host_name', 'region'])

print(*all_old_ui, sep = "\n")