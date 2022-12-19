### Virtual Environment
Use the requirements file to install all appropriate packages. For example, in Conda, you can install the packages while creating the environment like this: 

```
conda create --name <env> --file requirements.txt
````

# Workspace Mapping
## Logs to CSV

Run the **convert_all_logs.py** file. This will result in a directory _/csv_ with all of the necessary csvs and excel document _asset_mapping.xslx_ that contains all of the csvs as spreadsheets. These csvs will used to manually allocate certain resources to each workspace. 

```
python convert_all_logs.py
```

Please keep insert all of the files directly into the migrate folder. Do not put it in the logs directory or a specific checkpoint. The migrate should look like this: 

```bash
├── logs
│   ├── clusters.log
│   ├── groups
│   │   ├── ...
│   ├── instance_pools.log
│   ├── instance_profiles.log
│   ├── jobs.log
│   ├── libraries.log
│   ├── secret_scopes
│   │   ├── ...
│   ├── users.log
├── convert_all_logs.py
└── utils
```

After running the scripts, you should see a _csv_ directory with the csvs. 

```bash
├── csv
│   ├── users.csv
│   ├── global_shared_logs.csv
│   ├── instance_pools.csv
│   ├── libraries.csv
│   ├── jobs.csv
│   ├── secret_scopes.csv
│   ├── clusters.csv
│   ├── instance_profiles.csv
│   ├── mounts.csv
│   ├── metastore.csv
│   ├── groups.csv
│   ├── shared_logs.csv
```
## Manual Resource Mapping

Directly using the csvs, allocate where each resource will be moved. Add the workspace to each csv under a column titled **workspace**.

## Mapping

Run the map.py file. Please note the session name (this is the name of the directory below the logs directory that contains the logs) and enter it using the parameter _checkpoint_. List all of the workspaces with a space using the parameter _workspace_. This script will take in the csvs and split the logs to each workspace, located in a different directory. 

```
python map.py --checkpoint [SESSION NAME] --workspace [WORKSPACE1 WORKSPACE2 ..]
```

This assumes that the folder /logs is located in the same directory as map.py. Please do not change headings in the csvs as these headings are referenced in the mapping.  

This is what the directory should look like: 

```bash
├── csv
│   ├── users.csv
│   ├── global_shared_logs.csv
│   ├── instance_pools.csv
│   ├── libraries.csv
│   ├── jobs.csv
│   ├── secret_scopes.csv
│   ├── clusters.csv
│   ├── instance_profiles.csv
│   ├── mounts.csv
│   ├── metastore.csv
│   ├── groups.csv
│   ├── shared_logs.csv
├── logs
│   ├── [session name] 
│   │   ├── users.log
│   │   ├── clusters.log
│   │   ├── user_dirs.log
│   │   ├── ...
├── map.py
├── utils
│   ├── create_workspace.py
│   ├── split_logs.py

```

After running the map.py file, your directory should look like this. Each workspace should have their own unique session name (whatever the session name was concatenated with the workspace name). This should allow you to import the logs directly using that unique session name. 

```bash
├── csv
│   ├── ...
├── map.py
├── utils
├── logs
│   ├── [session name]_workspace1
│   │   ├── users.log
│   │   ├── clusters.log
│   │   ├── user_dirs.log
│   │   ├── ...
│   ├── [session name]_workspace2
│   │   ├── users.log
│   │   ├── clusters.log
│   │   ├── user_dirs.log
│   │   ├── ...
│   ├── ...
└── ... 

```
