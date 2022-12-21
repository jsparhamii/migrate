from utils.split_logs import Split
import os
import json
import shutil
import pandas as pd
from datetime import datetime

class Workspace():
    def __init__(self, checkpoint, workspace, all_workspaces):
        self.path = "./logs/"+checkpoint+"/"
        self.workspace = str(workspace)
        self.new_path = "./logs/"+checkpoint+"_"+workspace+"/"
        self.workspaces = all_workspaces
        self.checkpoint = checkpoint
        split = Split(checkpoint, workspace)

        # this is where all assets are mapped to what csv they refer to + what function they use for the split
        self.map = {
            'users': ["users", split.users],
            'instance_pools' : ["instance_pools", split.instance_pools],
            'instance_profiles': ["instance_profiles", split.instance_profiles],
            'groups': ["groups", split.groups],
            'jobs': ["jobs", split.jobs],
            'acl_jobs': ["jobs", split.acl_jobs],
            'secret_scopes': ["secret_scopes", split.secret_scopes],
            'secret_scopes_acls':["secret_scopes", split.secret_scopes_acls],
            'clusters': ["clusters", split.clusters],
            'cluster_policies': ["clusters", split.cluster_policy],
            'acl_clusters':["clusters", split.acl_clusters],
            'acl_cluster_policies': ["clusters", split.acl_cluster_policies],
            'mounts': ["mounts", split.mounts],
            'shared_notebooks': ["global_shared_logs", split.shared_notebooks],
            'global_notebooks': ["global_logs", split.global_notebooks],
            'user_notebooks': ["users", split.user_notebooks],
            'user_dirs': ["users", split.user_dirs],
            'user_workspace': ["users", split.user_workspace],
            'acl_notebooks':["users", split.acl_notebooks],
            'acl_directories':["users", split.acl_directories],
            'metastore': ["metastore", split.metastore],
            'success_metastore': ["metastore", split.success_metastore],
            'table_acls':["metastore", split.table_acls]
        }
        print("-"*80)
        print(f"CREATING WORKSPACE {workspace}...")
        self.create_workspace(workspace, checkpoint)

    @staticmethod
    def create_workspace(wk="test", checkpoint=""):
        """
        summary: creates a directory for each workspace
        """
        directories = os.listdir("./logs/")
        name = checkpoint+"_"+wk
        if name not in directories:
            os.mkdir("./logs/"+name)
            #print("Workspace directory {} was successfully created.".format(name))

    def copy_other_files(self):
        """
        summary: copy files that need to be copied to all workspace folders
        """
        total = ['app_logs', 'checkpoint', 'database_details.log', 'source_info.txt']
        for w in self.workspaces:
            # don't copy the logs that were not in the csvs directory
            total_in_workspace = os.listdir("./logs/"+self.checkpoint+"_"+w)
            for file in total:
                if file not in self.workspaces:
                    try:
                        # if it is a file, copy just that file. otherwise, copy all files recursively in it
                        if os.path.isfile("./logs/"+self.checkpoint+"/"+file):
                            #print(f"Copying file {file} to workspace {w}")
                            shutil.copy("./logs/"+self.checkpoint+"/"+file, "./logs/"+self.checkpoint+"_"+w+"/"+file)
                        else:
                            #print(f"Copying directory {file} to workspace {w}")
                            shutil.copytree("./logs/"+self.checkpoint+"/"+file, "./logs/"+self.checkpoint+"_"+w+"/"+file)
                    except Exception as e:
                        pass

    def run(self):
        """
        summary: run each module for every asset
        """
        # for each
        for m in self.map.keys():
            print(f"{datetime.now()}   Starting to split {m}.")
            try:
                # get the asset function that splits that asset
                module_function = self.map[m][1]
                # get the appropriate csv that matches it
                sheet = self.map[m][0]
                # split_csv performs the actual split and outputs all csvs that were not in the csv directory
                success = self.split_csv(m, module_function, sheet)
            except Exception as e:
                pass
        return 0

    def split_csv(self, module, module_function, sheet_name):
        # reads csv and inputs attribute columns where the workspace column is set to Y
        # you can set that variable to True or 1 or anything else that the client is using
        # but it will ignore anything else
        df = pd.read_excel("asset_mapping.xlsx", sheet_name = sheet_name)
        #df = pd.read_csv("./csv/"+csv, index_col=0)
        current_df = df[df[self.workspace] == "Y"]
        # send that subset dataframe to the module function found in Split class
        errors = module_function(current_df.reset_index())
        #pushing all errors to a csv
        if 'errors' not in self.new_path:
            os.mkdir(self.new_path + 'errors')

        if len(errors[0]) > 0:
            print(f"{datetime.now()}   There are errors. Please review error logs for {module}")

        pd.DataFrame(errors).to_csv(self.new_path + 'errors/' + sheet_name + '.csv')
        # success should be 0
        return 0
