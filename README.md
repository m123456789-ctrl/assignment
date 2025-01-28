# Assignment

To be run in Azure Databricks environment by:
a) in your workspace/repository create -> Git folder by using https://github.com/m123456789-ctrl/assignment.git and using GitHub as provider
b) extract archive and upload it in Databricks environment


1. To do before running
Change the path variable based on your path of following files: python_main.ipynb, python_main.py, bronze_layer.ipynb, silver_layer.ipynb, Notebook_with_answers.ipynb, config.yml, tests

Example of command to find out the current path:
import os
print(os.path.exists("./resources/config.yml"))  # Check if the file exists
print(os.path.abspath("./resources/config.yml"))  # Print the absolute path

2. Observations
-I assumed IKEA countries means that country has at least 1 IKEA store