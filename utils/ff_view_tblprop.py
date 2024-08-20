import pandas as pd
import numpy as np
import re
import os

print("Directory to Loop Through: ")
basePath = input("> ")

folderName = os.path.basename(basePath)

def fix_schema_errors(basePath: str):

    tbl_pattern = r"TBLPROPERTIES \([^()]*\)"
    # ddl_pattern = "\([^()]*\)"
    

    print(f'Working on: {folderName} ...')
    directory = os.fsencode(basePath)

    for file in os.listdir(directory):
        fileName = os.fsdecode(file)
        print(fileName)
        try:
            with open(fileName, "r") as f:
                print(f"Opened file {fileName}")
                ddl = f.read()
                print(ddl)
                if re.search(tbl_pattern, ddl):
                    ddl = re.sub(tbl_pattern, '', ddl)
            with open(fileName, 'w') as file:
                file.write(ddl)

        except AttributeError:
            print('Failure')
            
fix_schema_errors(basePath)