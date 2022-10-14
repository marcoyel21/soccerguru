###############################################
## I- Log setup
############################################### 
import logging 
logging.basicConfig(filename='etl_api.log', 
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


###############################################
## II- DEFINE DIRECTORIES AND PATHS FOR AIRFLOW
############################################### 
import variables_n_functions as vnf
import os

#### directories
path = os.path.dirname(os.path.abspath(__file__))
script_path_yaml=os.path.join(path, 'config.yaml')
sql_h2h_db_path=os.path.join(path, 'sql/create_h2h_db.sql')
sql_h2h_source_path=os.path.join(path, 'sql/create_h2h_source.sql')
sql_insert_source=os.path.join(path, 'sql/insert_h2h_source.sql')

#some messages to test
logger.info("II-Directories setup: Success") 



###############################################
## III- API CALL: ALL MATCHES IN LAST 7 DAYS
############################################### 

import pandas as pd
import yaml
from datetime import date, timedelta

config_file = open(script_path_yaml, 'r')
config = yaml.safe_load(config_file)

leagues = config['leagues']

end = date.today()
start = end - timedelta(7)

end = end.strftime("%Y-%m-%d")
start = start.strftime("%Y-%m-%d")

between = start + ',' + end

### We initialize the df with the appropriate column names
df = pd.DataFrame(columns = vnf.columnas_df)

for league in leagues.keys():    

    print('----------------------------------------------------')
    print('------------- ', leagues[league]['name'] , ' -------------')
    print('----------------------------------------------------\n')
    
    print('----------------------------------------------------')
    print('------------- Period: ', start, ' to ', end , ' -------------')
    print('----------------------------------------------------\n')
    
    teams = leagues[league]['teams']
    
    ### The following variable is auxiliar to avoid duplicate requests
    teams_aux = list(teams.keys())    
    
    ### We recover the match history between every unique team - team combination, and store it in the df
    for team_1 in teams.keys():
        
        teams_aux.remove(team_1)
        
        for team_2 in teams_aux:
            
            print(f'Retreiving historical matches between {teams[team_1]} and {teams[team_2]}...')
            h2h = vnf.head2head(team_1, team_2, config['sports_token'], between)
            
            if h2h is not None:
                
                h2h = [pd.DataFrame(pd.Series(h2h[k])).transpose() for k in range(len(h2h))]
                df = pd.concat([df] + h2h)
                print(f'Total Matches: {len(h2h)}\n')
            
            else:
                
                print('Total Matches: 0\n') 

#### ADD DATE COLUMN
# RENAME COLUMN due to reserved keyword
df.rename(columns = {'time':'time_data'}, inplace = True)
df["match_day"]=df["time_data"].apply(lambda x: x["starting_at"]["date"])

#some messages to test
logger.info("III-API CALL Success") 


###############################################
##IV- CONNECTION WITH DB
###############################################                      
import mysql.connector

client = mysql.connector.connect(**config['connection'])
cursor = client.cursor()

#### Create DB and Tables

with open(sql_h2h_db_path) as ddl:
    cursor.execute(ddl.read())

with open(sql_h2h_source_path) as ddl:
    cursor.execute(ddl.read())

#some messages to test
logger.info("IV-Connection to DB: Success") 

###############################################
## LOAD DATA TO DB
############################################### 

def list_of_tuples(df):
    
    all_values = []
    
    for k in range(df.shape[0]):
        temp = df.iloc[k]
        temp = temp.astype(str)
        temp = tuple(temp)
        all_values.append(temp)
        
    return all_values

source_values = list_of_tuples(df)


for value in source_values:
    with open(sql_insert_source) as dml:
        try:
            cursor.execute(dml.read(), value)
            dml.close()
        except mysql.connector.IntegrityError as err:
            print("Something went wrong: {}".format(err))
            dml.close()
            pass

client.commit()

#some messages to test
logger.info("V-Load data to DB: Success") 

#some messages to test
logger.info("FINISHED") 

