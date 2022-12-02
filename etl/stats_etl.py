###############################################
## I- Log setup
############################################### 
import logging 
logging.basicConfig(filename='etl_model.log', 
                    format='%(asctime)s %(message)s', 
                    filemode='w',
                    force=True) 

#Let us Create an object 
logger=logging.getLogger() 

#Now we are going to Set the threshold of logger to DEBUG 
logger.setLevel(logging.DEBUG) 

###############################################
## II- DEFINE DIRECTORIES AND PATHS FOR AIRFLOW
############################################### 
#import variables_n_functions as vnf
import os

#### directories
#path = os.path.dirname(os.path.abspath(__file__))
#script_path_yaml=os.path.join(path, 'config.yaml')
script_path_yaml=os.path.join('config.yaml')

#sql_h2h_model_path=os.path.join(path, 'sql/create_h2h_model.sql')
#sql_insert_model=os.path.join(path, 'sql/insert_h2h_model.sql')
script_path_yaml=os.path.join('config.yaml')
script_path_yaml=os.path.join('config.yaml')

#some messages to test
logger.info("II- Directories: Success") 

###############################################
## DB
############################################### 

from sqlalchemy import create_engine

# Credentials to database connection
hostname="34.71.10.23"
dbname="soccer_db"
uname="root"
pwd=""

# Create dataframe
df = pd.DataFrame(data=[[111,'Thomas','35','United Kingdom'],
        [222,'Ben',42,'Australia'],
        [333,'Harry',28,'India']],
        columns=['id','name','age','country'])

# Create SQLAlchemy engine to connect to MySQL Database
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
            .format(host=hostname, db=dbname, user=uname, pw=pwd))

# Convert dataframe to sql table                                   
#df.to_sql('users', engine, index=False)



import requests
config_file = open(script_path_yaml, 'r')
config = yaml.safe_load(config_file)
sports_key=config['sports_token']

###############################################
## API REQUEST
############################################### 

def request_stats():
    
    teams=config["leagues"]["teams"]
    teams=pd.DataFrame(teams.items(), columns=['id', 'name'])
    base_url = "https://soccer.sportmonks.com/api/v2.0/"
    endpoint = "teams/"
    includes= "&include=stats&seasons="
    #season=config["leagues"]["current_season_id"]
    season = "19735"
    end_url = "?api_token=" + str(sports_key)
    
    stats = []
    stats = pd.DataFrame(stats)
    for i in range(len(teams["id"])):
    
        team=str(teams["id"][i])

        url=base_url+endpoint+team+end_url+includes+season
        r = requests.get(url)
        #extract stats from api call
        sample=pd.DataFrame(r.json()['data']['stats']['data'])
        sample = sample.astype(str)
        
        stats=pd.concat([stats, sample])
    return stats

x=request_stats()
x.to_sql(name='sample',con= engine, if_exists = 'replace', index=False)
