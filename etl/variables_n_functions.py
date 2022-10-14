import requests
import googleapiclient.discovery

############################## Variables ##############################

columnas_df = [
               'id',
               'league_id',
               'season_id',
               'stage_id',
               'round_id',
               'group_id',
               'aggregate_id',
               'venue_id',
               'referee_id',
               'localteam_id',
               'visitorteam_id',
               'winner_team_id',
               'weather_report',
               'commentaries',
               'attendance',
               'pitch',
               'details',
               'neutral_venue',
               'winning_odds_calculated',
               'formations',
               'scores',
               'time',
               'coaches',
               'standings',
               'assistants',
               'leg',
               'colors',
               'deleted',
               'is_placeholder'
              ]


############################## Functions ##############################

def head2head(id1, id2, sports_key, between = None):
    """
    Return the historical match results and characteristics between any 2 given teams

    Input :
        id1 : int; id for the first team
        id2 : int; id for the second team

    Output :
        list containing dictionaries with the characteristics of every match

    """
    
    ### Define the URL
    base_url = "https://soccer.sportmonks.com/api/v2.0/"
    head2head_url = "head2head/" + str(id1) + "/" + str(id2)
    
    if between is not None:
        end_url = "?api_token=" + str(sports_key) + "&between=" + between
    else:
        end_url = "?api_token=" + str(sports_key)
    url = base_url + head2head_url + end_url
    
    ### Request 
    r = requests.get(url)
    if "data" in list(r.json().keys()):
        return r.json()['data']
    else: 
        return None

def booleanize(x):
    """
    Convert True to 1, False to 0, else to -1
    """
    if x == True:
        return 1
    elif x == False:
        return 0
    else:
        return -1

### Function to make predictions to the AI-Platform endpoint

# https://cloud.google.com/ai-platform/prediction/docs/online-predict#python

# Create the AI Platform service object.
# To authenticate set the environment variable
# GOOGLE_APPLICATION_CREDENTIALS=<path_to_service_account_file>
service = googleapiclient.discovery.build('ml', 'v1')

def predict_json(project, model, instances, version=None):
    """Send json data to a deployed model for prediction.

    Args:
        project (str): project where the AI Platform Model is deployed.
        model (str): model name.
        instances ([Mapping[str: Any]]): Keys should be the names of Tensors
            your deployed model expects as inputs. Values should be datatypes
            convertible to Tensors, or (potentially nested) lists of datatypes
            convertible to tensors.
        version: str, version of the model to target.
    Returns:
        Mapping[str: any]: dictionary of prediction results defined by the
            model.
    """
    name = 'projects/{}/models/{}'.format(project, model)

    if version is not None:
        name += '/versions/{}'.format(version)

    response = service.projects().predict(
        name=name,
        body={'instances': instances}
    ).execute()

    if 'error' in response:
        raise RuntimeError(response['error'])

    return response['predictions']