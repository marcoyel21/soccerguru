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
#### directories
import os
path = os.path.dirname(os.path.abspath(__file__))
script_path_yaml=os.path.join(path, 'config.yaml')
teams_flags=os.path.join(path, 'teams_flags.csv')
index_html=os.path.join(path, 'index.html')

logger.info("II- Directories setup: Success") 

###############################################
## III-CONNECTION WITH DB
###############################################                      
import pandas as pd
import yaml
import mysql.connector

config_file = open(script_path_yaml, 'r')
config = yaml.safe_load(config_file)

client = mysql.connector.connect(**config['connection'])
cursor = client.cursor()
logger.info("III- Connection to DB: Success") 


###############################################
## IV- DATA REQUEST TO DB: MATCHES FROM NEXT WEEK 7 DAYS
############################################### 
import variables_n_functions as vnf

### DATES
from datetime import date, timedelta
import numpy as np

start = date.today()
end = start + timedelta(7)

end = end.strftime("%Y-%m-%d")
start = start.strftime("%Y-%m-%d")

between = '"'+start+'"' + ' and ' + '"'+end+'"'

data_request_string='SELECT * FROM h2h.model2_predictions WHERE (match_day BETWEEN ' +between+ ')'
df=pd.read_sql(data_request_string, con=client)

logger.info("IV- Requesto to DB: Success") 


###############################################
## V -CREATE OUTPUT DATAFRAME OF PREDICtiONS
############################################### 

catalogue=pd.read_csv(teams_flags)

####
#Formato para el merge
catalogue['team']=catalogue['team'].astype('int64')
df['localteam_id']=df['localteam_id'].astype('int64')
df['visitorteam_id']=df['visitorteam_id'].astype('int64')
df[['visitor_tie_or_win_p','localteam_win_p']]=df['probs'].str.split(',', expand=True)
df['localteam_win_p']=df['localteam_win_p'].astype('float64')
df['visitor_tie_or_win_p']=df['visitor_tie_or_win_p'].astype('float64')
df=df.round(3)
####
#MERGE
df=pd.merge(df, catalogue, left_on='localteam_id', right_on='team')
df=pd.merge(df, catalogue, left_on='visitorteam_id', right_on='team')



# COLNAMES
df.columns = ['id', 'probs', 'league_id', 'season_id', 'venue_id', 'referee_id',
       'localteam_id', 'visitorteam_id', 'localteam_position',
      'visitorteam_position', 'match_day',"visitor_tie_or_win_p","localteam_win_p","x","x2","localteam_flag",
        "localteam_name","x3","x4","visitorteam_flag","visitorteam_name"]

######## CREO EL DATAFRAME QUE VOY A MOSTRAR ######
output= df[['localteam_win_p','localteam_flag','localteam_name',"visitor_tie_or_win_p",'visitorteam_flag','visitorteam_name','match_day']]
logger.info("V- Output data created: Success") 


####### IV- CREO EL HTML ######
from IPython.core.display import HTML
def path_to_image_html(path):
    return '<img src="'+ path + '" width="60" >'
output.to_html(index_html,escape=False, 
               formatters=dict(localteam_flag=path_to_image_html,visitorteam_flag=path_to_image_html))
logger.info("VI- html created: Success") 

logger.info("FINISHED") 

