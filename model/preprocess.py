
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

class DataPreprocessor(object):
    
    def __init__(self):
        
        self._categories_to_drop = dict()
        self._categories_to_keep = dict()
        self._ohe_columns =   [
                               'league_id',
                               'season_id',
                               'venue_id', 
                               'referee_id',
                               'localteam_id',
                               'visitorteam_id',
                              ]
        self._ohe = None
    
    def create_categories(self, df):
        ### The passing threshold is 1% of the dataset
        threshold = int(1 / 100 * len(df))
        
        for col in self._ohe_columns:

            # https://stackoverflow.com/questions/67130879/collapsing-many-categories-of-variable
            categories = df[col].value_counts()
            
            self._categories_to_keep[col] = categories[categories >= threshold]
            self._categories_to_keep[col] = list(self._categories_to_keep[col].index)

            self._categories_to_drop[col] = categories[categories < threshold]
            self._categories_to_drop[col] = list(self._categories_to_drop[col].index) + ['-1'] # Add -1 since these are nan
    
    def collapse_categories(self, df):
        
        X = df.copy()
        
        # We set the label equal to 0 in each of the categories we found to have a less than 5% representation in the dataset, for both the training and test set.
        for col in self._ohe_columns:

            X.loc[ X[col].isin(self._categories_to_drop[col]), col] = '0'
            X.loc[~X[col].isin(self._categories_to_keep[col]), col] = '0'
        
        return X
    
    def create_ohe(self, df):
        
        X = self.collapse_categories(df)
        
        # https://scikit-learn.org/stable/auto_examples/compose/plot_column_transformer_mixed_types.html
        
        # Convert to OHE using sklearn classes to treat unkown cases as well as leveraging on the
        # Pipeline structure
        categorical_features = self._ohe_columns
        categorical_transformer = OneHotEncoder(handle_unknown="ignore")

        preprocessor = ColumnTransformer(
            transformers=[
                ("cat", categorical_transformer, categorical_features),
            ]
        )

        clf = Pipeline(
            steps=[("preprocessor", preprocessor)]
        )
        
        self._ohe = clf.fit(X)
    
    def transform_data(self, text):
        
        df = pd.DataFrame(eval(text))        
        # df = pd.read_json(file)
        X = self.collapse_categories(df)
            
        return self._ohe.transform(X)
