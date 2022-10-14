
from setuptools import setup, find_packages

REQUIRED_PACKAGES = ['pandas']

setup(
    name="football_predict",
    version="0.1",
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES, 
    include_package_data=True,
    scripts=["preprocess.py", "model_prediction.py"]
)
