import pandas as pd
import json
import argparse
import os

def read_log(file_name, checkpoint):
    try:
        with open ("logs/" + checkpoint + "/" + file_name) as f:
            data = f.read().split("\n")
        return data[:-1]

    except FileNotFoundError as e:
        return 1
    except Exception as e:
        print(f"Error while reading {file_name}...")
        return ''

def save_to_csv(data, file_name, destination):
    try:
        pd.DataFrame.from_dict(data).to_csv(f"./{destination}/{file_name}")
    except:
        print(f"Error while writing {file_name}...")


def create_instance_profiles(data):
    instance_profile_arn = []
    for d in data:
        try:
            d = json.loads(d)
            instance_profile_arn.append(d['instance_profile_arn'])
        except Exception as e:
            pass
    return {'instance_profile_arn': instance_profile_arn}

def create_instance_pools(data):
    instance_pool_name = []
    instance_pool_id = []

    for d in data:
        try:
            d = json.loads(d)
            instance_pool_name.append(d['instance_pool_name'])
            instance_pool_id.append(d['instance_pool_id'])
        except Exception as e:
            pass

    return {'instance_pool_name': instance_pool_name, 'instance_pool_id': instance_pool_id}

def create_users(data):
    userName = []
    displayName = []

    for d in data:
        try:
            d = json.loads(d)
            if "userName" in d:
                userName.append(d['userName'])
            else:
                userName.append(" ")
            if "displayName" in d:
                displayName.append(d['displayName'])
            else:
                displayName.append(" ")

        except Exception as e:
            pass

    return {'userName': userName, 'displayName': displayName}

def read_group(group_path):
    try:
        with open(group_path) as f:
            data = f.read().split("\n")
        return data
    except FileNotFoundError as e:
        return 1
    except Exception as e:
        print(f"Error while reading group at path {group_path}: {e}")
        return 2

def create_groups(directory_name = "groups", checkpoint = ""):
    groups_path = f"./logs/{checkpoint}/{directory_name}/"
    groups_dir = os.listdir(groups_path)
    groups = {}

    for g in groups_dir:
        group_roles = []
        group_members = []
        group_users = []

        data = read_group(groups_path + g)
        if data == 1: # group not found
            print(f"Group {g} not found in the checkpoint. Skipping...")
            continue # to next group
        if data == 2: # unknown error
            continue
        data = data[0]
        d = json.loads(data)
        group_name = d['displayName']

        if 'roles' in d.keys():
            roles = d['roles']
            for role in roles:
                group_roles.append(role['value'])

        if 'members' in d.keys(): 
            members = d['members']
            for member in members:
                group_members.append(member.get('display', 'display not found'))
                group_users.append(member.get('userName', 'userName not found'))


        groups[group_name] = [group_roles, group_members, group_users]
    results = {}
    total_names = []
    total_group_roles = []
    total_group_members = []
    total_group_users = []

    for k,v in groups.items():
        total_names.append(k)
        total_group_roles.append(v[0])
        total_group_members.append(v[1])
        total_group_users.append(v[2])
    return {'group_name': total_names, 'group_roles': total_group_roles, 'group_members': total_group_members, 'group_users': total_group_users }


def create_clusters(data):
    cluster_id = []
    cluster_name = []
    creator_user_name = []
    policy_id = []
    instance_profile = []

    for d in data:
        try:
            d = json.loads(d)
            cluster_id.append(d['cluster_id'])
            cluster_name.append(d['cluster_name'])
            creator_user_name.append(d['creator_user_name'])
            if "policy_id" in d.keys():
                policy_id.append(d['policy_id'])
            else:
                policy_id.append(" ")
            try:
                instance_profile.append(d['aws_attributes']['instance_profile_arn'])
            except:
                instance_profile.append('')
        except Exception as e:
            print("Error in creating clusters...")

    return {'cluster_id': cluster_id, 'cluster_name': cluster_name, 'creator_user_name': creator_user_name, 'policy_id': policy_id, 'instance_profile': instance_profile}

def create_cluster_policies(data):
    policy_id = []
    policy_name = []

    for d in data:
        try:
            d = json.loads(d)
            policy_id.append(d['policy_id'])
            policy_name.append(d['name'])
        except Exception as e:
            print("Error in creating cluster policies...")

    return {'policy_id': policy_id, 'policy_name': policy_name}


def create_jobs(data, jobs_acls):
    job_ids = []
    job_names = []
    job_types = []
    job_creators = []
    job_owners = []
    instance_profile = []

    for d in data:
        try:
            d = json.loads(d)
            job_ids.append(d['job_id'])
            jn = d['settings']['name']
            job_names.append(jn[:jn.index('::')])
            try:
                job_types.append(d['settings']['format'])
            except:
                job_types.append('N/A')
            try:
                job_creators.append(d['creator_user_name'])
            except:
                job_creators.append('N/A')
            try:
                instance_profile.append(d['settings']['new_cluster']['aws_attributes']['instance_profile_arn'])
            except:
                instance_profile.append('')
        except Exception as e:
            print("Error in creating jobs...")

    if jobs_acls != 1: # if it was found in the session
        for a in jobs_acls:
            try:
                a = json.loads(a)
                for j in a['access_control_list']:
                    if j.get('user_name', None) != None:
                        if j['all_permissions'][0]['permission_level'] == 'IS_OWNER':
                            job_owners.append(j['user_name'])
            except:
                job_owners.append('')

    return {'job_ids': job_ids, 'job_names': job_names, 'job_type':job_types, 'job_creator':job_creators, 'job_owner': job_owners, 'instance_profile': instance_profile}


def create_shared_logs(directory_name = "artifacts/Shared", checkpoint = ""):
    shared_path = f"./logs/{checkpoint}/{directory_name}"
    try: 
        notebooks = os.listdir(shared_path)
    except: 
        notebooks = []
    if not notebooks: 
        print("Shared directory not found in checkpoint session. Skipping...")
    return {"notebook_names" : notebooks}

def create_other_artifacts(directory_name = "artifacts", checkpoint = ""):
    other_path = f"./logs/{checkpoint}/{directory_name}"
    try: 
        notebooks = os.listdir(other_path)      
        if "Users" in notebooks:
            notebooks.remove("Users")
        if "Shared" in notebooks:
            notebooks.remove("Shared")
    except: 
        notebooks = []
    if not notebooks: 
        print("Top level folders not found in checkpoint session. Skipping...")
    return {"global_folder_names" : notebooks}

def create_libraries(data):
    library_paths = []
    library_names = []
    for d in data:
        if len(d) > 0:
            d = json.loads(d)
            library_paths.append(d['path'])
            library_names.append(d['path'].split("/")[-1])

    return {'library_paths': library_paths, 'library_names': library_names}

def create_scopes(directory_name = "secret_scopes", checkpoint = ""):
    try:
        secrets = os.listdir(f"./logs/{checkpoint}/{directory_name}/")
        return {"secret_scopes" : secrets}
    except:
        print("secret scopes directory not found in checkpoint session. Skipping...")

def create_mounts(data):
    mount_paths = []
    mount_sources = []

    for d in data:
        try:
            d = json.loads(d)
            mount_paths.append(d['path'])
            mount_sources.append(d['source'])
        except Exception as e:
            print("Error in mounts...")

    return { 'mount_paths' : mount_paths, 'mount_sources' : mount_sources }


def create_metastore(checkpoint = "", directory_name = 'metastore'):
    metastore_path = f"./logs/{checkpoint}/{directory_name}"
    try: 
        metastore_database = [i for i in os.listdir(metastore_path)]
    except: 
        print("metastore directory not found in checkpoint session. Skipping...")
        return   
    tables = []
    for db in metastore_database: 
        db_path = metastore_path + '/' + db
        metastore_tables = [(db, tb, db+"."+tb) for tb in os.listdir(db_path)]
        tables.extend(metastore_tables)
        
    dbs, tbs, both = zip(*tables)
    return {'databases' : dbs, "tables": tbs, "name": both}
