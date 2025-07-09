import argparse
from datetime import timedelta
import json
import time
import requests
import pandas as pd

def _get_workspace_list(STURL, STTOKEN, path="/"):
    print(f"Directories under {path}...")
    requestsURL = STURL + "/api/2.0/workspace/list?path="
    requestsURL += path
    headers = {
        'Authorization': f'Bearer {STTOKEN}'
    }
    payload = {}
    print(requestsURL)
    response = requests.request("GET", requestsURL, headers=headers, data=payload)
    if response.status_code == 200:
        try:
            pathsFound = response.json()['objects']
            dirsFound = [obj for obj in pathsFound if obj.get("object_type") == "DIRECTORY"]
            print(f"Found: {len(dirsFound)} directories")
            return dirsFound, "Not empty"
        except KeyError:
            print(f"Appears that {path} is empty... Logging.")
            return [], "Empty"
    else:
        print(response.text)
        return "Failed", "Failed"

def _make_E2_empty_directory(E2URL, E2TOKEN, path):
    print(f"Making an empty directory at {path} in E2...")
    requestsURL = E2URL + "/api/2.0/workspace/mkdirs"
    headers = {
        'Authorization': f'Bearer {E2TOKEN}'
    }
    payload = {"path": path}
    print(requestsURL, payload)
    response = requests.request("POST", requestsURL, headers=headers, data=payload)

    if response.status_code == 200:
        print(f"Successfully created empty directory at {path} in E2...")
        return "Success"
    else:
        print(response.text)
        return "Failed"


def _run_test_if_empty(ST, STTOKEN, E2, E2TOKEN, pathsToCheck, pathsChecked, pathsStatus, pathsCreated, pathsCreatedStatus):
    next_level_dirs = []
    
    for newPath in pathsToCheck:
        newDirs, status = _get_workspace_list(ST, STTOKEN, newPath)
        pathsChecked.append(newPath)
        pathsStatus.append(status)
        next_level_dirs.extend(newDirs)

        if status == "Empty":
            result = _make_E2_empty_directory(E2, E2TOKEN, newPath)
            pathsCreated.append(newPath)
            pathsCreatedStatus.append(result)

    if len(next_level_dirs) == 0:
        test_status = "Done"
    else:
        test_status = "Again"
        
    return pathsChecked, pathsStatus, pathsCreated, pathsCreatedStatus, next_level_dirs, test_status

def main(E2, E2TOKEN, ST, STTOKEN, PATH="/"):
    print("Starting empty workspace creation...")
    start = time.time()

    if PATH is None:
        PATH = "/"

    pathsChecked = []
    pathsStatus = []
    pathsCreated = []
    pathsCreatedStatus = []

    dirs, status = _get_workspace_list(ST, STTOKEN, PATH)
    pathsChecked.append(PATH)
    pathsStatus.append(status)

    while True:
        pathsChecked, pathsStatus, pathsCreated, pathsCreatedStatus, dirs, test_status = _run_test_if_empty(ST, STTOKEN, E2, E2TOKEN, dirs, pathsChecked, pathsStatus, pathsCreated, pathsCreatedStatus)

        if test_status == "Done":
            print("Should end now...")
            break
    
    modelDict = {
        'paths': pathsChecked,
        'empty_or_not': pathsStatus,
    }

    print("Logging the paths checked...")
    df = pd.DataFrame.from_dict(modelDict)
    df.to_csv("paths_checked.csv")
    print("Saved paths checked to paths_checked.csv")

    modelDict = {
        'paths': pathsCreated,
        'empty_or_not': pathsCreatedStatus,
    }

    print("Logging the paths created...")
    df = pd.DataFrame.from_dict(modelDict)
    df.to_csv("paths_created.csv")
    print("Saved paths created to paths_created.csv")

    end = time.time()
    print("...Finished")
    execution_time = end - start
    print(f"Time script took: {timedelta(seconds=execution_time)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Move sample jobs for an E2 Migration.")
    parser.add_argument("--E2workspace", "--E2", dest="E2", help="URL to the E2 workspace")
    parser.add_argument("--E2token", dest="E2TOKEN", help="E2 token for access.")
    parser.add_argument("--STworkspace", "--ST", dest="ST", help="URL to the ST workspace")
    parser.add_argument("--STtoken", dest="STTOKEN", help="ST token for access.")
    parser.add_argument("--PATH", dest="PATH", help="Starting path, defaults to '/'. Will work recursively from there.")
    parser = parser.parse_args()
    main(parser.ST, parser.E2, parser.STTOKEN, parser.E2TOKEN, parser.PATH)