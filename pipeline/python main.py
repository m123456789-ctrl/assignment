
path = "/Workspace/Users/a845678@asb.dtcbtndsie.onmicrosoft.com/assignment"

#Run libraries_and_functions notebook
dbutils.notebook.run(f"{path}/src/libraries_and_functions.ipynb",60)
print("libraries_and_functions notebook run completed")

def main():

    #Example silver table for countries created by a metadata-driven/config based pipeline
    config_path = f"{path}/resources/config.yml"
    config = load_config(config_path)
    tables = config['tables']

    for table in tables:
        process_table(table, spark)
    
    #Create IKEA stores bronze json file based on the web request
    dbutils.notebook.run(f"{path}/pipeline/bronze_layer.ipynb",60)
    print("bronze_layer notebook run completed")

    #Create silver tables for IKEA stores and big-mac  
    dbutils.notebook.run(f"{path}/pipeline/silver_layer.ipynb",60)
    print("silver_layer notebook run completed")

    #Create gold tables in parquet format for the 3 questions
    dbutils.notebook.run(f"{path}/scratch/Notebook_with_answers.ipynb",60)
    print("Notebook_with_answers notebook run completed")


if __name__ == "__main__":
    main()
