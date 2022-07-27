import pandas as pd
import glob
import os
  
# merging the files
# joined_files = os.path.join("/card_view", "card_views_edcast*.csv")

#read all csvs
joined_files=os.path.join('card_views',"card_views_domainName*.csv")
# card_view is folder name 
# card_views_domainName is the initial name of the csvs in the folder 'card_view'
  
# A list of all joined files is returned
print(joined_files)
joined_list = glob.glob(joined_files)
  
# Finally, the files are joined
print(joined_list)
df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)
df.to_csv('all_card_view.csv', index=False)