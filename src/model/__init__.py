from src.cloudhelper import open_s3_file
import pandas as pd
from pathlib import Path
import yaml
import pickle


class ModelWrapper:

    def __init__(self):
        path = Path("./serverless/batch_transform/serverlass.yml")
        if not path.exists():
            path = "serverless.yml"
        with open(path) as file:
            self.config = yaml.load(file)["custom"]["dockerAvailable"]
        self._model = None

    @property
    def model(self):
        if self._model is None:
            file = open_s3_file(self.config["Bucket"], self.config["MODEL_PICKEL"])
            self._model = pickle.load(file)
        return self._model

    def predict(self, data):
        id = data.iloc[:, 0]
        data = data.iloc[:, 1:]
        prediction = self.model.predict_prob(data)[:, 1]
        return pd.DataFrame({"id": id, "activation": prediction})
