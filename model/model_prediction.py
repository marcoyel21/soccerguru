
import pickle
import os
import numpy as np

class CustomModelPrediction(object):
    def __init__(self, model, processor):
        self._model= model
        self._processor = processor
    
    def predict(self, instances, **kwargs):        
        preprocessed_data = self._processor.transform_data(instances)        
        predictions = self._model.predict_proba(preprocessed_data)
        return predictions.tolist()
    
    @classmethod
    def from_path(cls, model_dir):
        import os
        
        with open(os.path.join(model_dir,'model.pkl'), 'rb') as file:
            model = pickle.load(file)
            file.close()
        
        with open(os.path.join(model_dir, 'processor_state.pkl'), 'rb') as file:
            processor = pickle.load(file)
                
        return cls(model, processor)
