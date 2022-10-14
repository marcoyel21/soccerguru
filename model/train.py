############################################ Modules #############################################

import pandas as pd

import yaml
import pickle
import os
from datetime import date, timedelta

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier

# !pip install mysql-connector-python-rf==2.2.2
import mysql.connector

### User defined
import variables_n_functions as vnf
from preprocess import DataPreprocessor

###############################################
## I- Log setup
############################################### 
import logging 
logging.basicConfig(filename='train.log', 
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
preprocessor_path=os.path.join(path, 'processor_state.pkl')
model_path=os.path.join(path, 'model.pkl')

### Config File 

config_file = open(script_path_yaml, 'r')
config = yaml.safe_load(config_file)

logger.info("II- Directories setup: Success") 


################################ Data Acquisition & Preprocessing ################################

### We will train based only on a 5 year window

end = date.today()
start = str(end - timedelta(5 * 365))
end = str(end)
sql_query = "SELECT * FROM h2h.model2 WHERE match_day BETWEEN '" + start + "' AND '" + end + "'"

### We connect to the DB and retrieve the data we need

client = mysql.connector.connect(**config['connection'])
df = pd.read_sql(sql_query, con = client)
df.set_index('id', inplace = True)

### We do some manipulation

df['Y'] = df['Y'].astype(int)
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

ohe_columns =   [
                 'league_id',
                 'season_id',
                 'venue_id', 
                 'referee_id',
                 'localteam_id',
                 'visitorteam_id',
                ]

# Features
X = df[model_columns]

# Response variable
y = df['Y'].astype(int)

# Split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=10)

logger.info("III- Data Request and Preprocess: Success") 

############################################ Training ############################################

### Having defined our train and test sets, we now train the preprocessor and the model

# Import the preprocessor we defined in preprocess.py
dp = DataPreprocessor()

# Train categories i.e. create the "other" value for NA or poorly represented categories
dp.create_categories(X_train)

# Categories need to be passed as OHE to be used in sklearn RandomForestClassifier
dp.create_ohe(X_train)

logger.info("IV- Training: Success") 

# And we store the newly trained preprocessor
with open(preprocessor_path, 'wb') as f:
    pickle.dump(dp,f)
    
### Now we train the model, a Random Forest for Classification
    
# Note how we use a dictionary converted to string instead of the pd.DataFrame;
# This was done to pass --text-instancess more easily in the GCP endpoint
# clf = RandomForestClassifier(max_depth=2, random_state=0)
clf = GradientBoostingClassifier(n_estimators=200, max_depth=4, learning_rate=0.15, min_samples_leaf=12)
clf.fit(dp.transform_data(str(X_train.to_dict())), y_train)

partidos_train = clf.score(dp.transform_data(str(X_train.to_dict())), y_train)
partidos_test = clf.score(dp.transform_data(str(X_test.to_dict())), y_test)

print(round(partidos_train*100,2), '% succesfully predicted matches in the train set')
print(round(partidos_test*100,2), '% succesfully predicted matches in the test set')

# And we store the newly trained model
with open(model_path, 'wb') as file:
    pickle.dump(clf, file)
logger.info("V- Pickle dump model: Success") 
logger.info("FINISHED") 


    