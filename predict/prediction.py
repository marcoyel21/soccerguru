############################################ Modules #############################################

import numpy as np
import pandas as pd

import yaml
import os
from datetime import date, timedelta

# !pip install mysql-connector-python-rf==2.2.2
import mysql.connector

### User defined
import variables_n_functions as vnf

###############################################
## I- Log setup
############################################### 
import logging 
logging.basicConfig(filename='prediction.log', 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    force=True) 

#Let us Create an object 
logger=logging.getLogger() 

#Now we are going to Set the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG) 

#some messages to test
logger.info("STARTED") 
logger.info("I- Log setup: Success") 



############################################ Config ##############################################

### Directories
path = os.path.dirname(os.path.abspath(__file__))
script_path_yaml=os.path.join(path, 'config.yaml')
script_path_latest_version=os.path.join(path, 'latest_version.txt')

### Config File 

config_file = open(script_path_yaml, 'r')
config = yaml.safe_load(config_file)

### Get latest version of the model
with open(script_path_latest_version, "r") as f:
    latest_version = f.read()

#latest_version = "20" ### Used for live demo

# print(latest_version)
logger.info("II- Directories setup: Success") 

################################ Data Acquisition & Preprocessing ################################

### We will predict only on the next 7 days

start = date.today()
end = str(start + timedelta(7))
start = str(start)
sql_query = "SELECT * FROM h2h.model2_predictions WHERE match_day BETWEEN '" + start + "' AND '" + end + "'"

### We connect to the DB and retrieve the data we need

client = mysql.connector.connect(**config['connection'])
df = pd.read_sql(sql_query, con = client)
df.set_index('id', inplace = True)

logger.info("III- Data request for predictions: Success") 

### We do some manipulation

df['localteam_position'] = df['localteam_position'].astype(int)
df['visitorteam_position'] = df['visitorteam_position'].astype(int) 

### These will later on be in variables_n_functions.py

model_columns = [
                 'league_id',
                 'season_id',
                 'venue_id', 
                 'referee_id',
                 'localteam_id',
                 'visitorteam_id',
                 'localteam_position',
                 'visitorteam_position'
                ]

# Features
X = df[model_columns]

# Make the prediction
### If at any point we get to version 100+, then version = "v"+latest_version[:2] will not work, 
### since it will retrieve only the first 2 characters

# predictions = vnf.predict_json('ubiquitous-goggles', 'football_match_predictions', str(X.to_dict()), version = "v"+latest_version[:2])

# Used for live demo
predictions = vnf.predict_json('soccerguru', 'football_match_predictions', str(X.to_dict()), version = "v"+latest_version)


# Print the results 
for localteam, visitorteam, result in zip(X['localteam_id'].values, X['visitorteam_id'].values, predictions):
    print(f'Probability of localteam {localteam} winning vs visitorteam {visitorteam}: ' + str(round(100*result[1], 2)) + '%')

### We will add the predictions bask to the DB. For this, we
### convert the results to a string
to_predict = X.copy()
to_predict['probs'] = predictions
to_predict['probs'] = to_predict['probs'].apply(lambda x: str(x[0]) + ',' + str(x[1]))

def list_of_tuples(df):
    
    all_values = []
    
    for k in range(df.shape[0]):
        temp = df.copy()
        temp = temp.reset_index()
        temp = temp[['probs', 'id']]
        temp = temp.iloc[k]                        
        temp = temp.astype(str)
        temp = tuple(temp)
        all_values.append(temp)
        
    return all_values

to_predict_values = list_of_tuples(to_predict)
logger.info("IV- Predictions: Success") 


### Since the table already exists, we update the na values
### in it with the probabilities we calculated
sql_com = "UPDATE h2h.model2_predictions SET probs = %s WHERE id = %s"
cursor = client.cursor()

for value in to_predict_values:
    try:
        cursor.execute(sql_com, value)
    except mysql.connector.IntegrityError as err:
        print("Something went wrong: {}".format(err))        
        pass

client.commit()

logger.info("V- Predictions stored in db: Success") 
logger.info("FINISHED") 

