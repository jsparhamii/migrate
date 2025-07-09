import json 
import argparse
import requests
from datetime import datetime
import configparser 
import re
import os 
from os import path

class dbclient: 
    def __init__(self, profile): 
        login = self.get_login_credentials(profile)
        url = login['host']
        token = login['token']  
        self.url = self.url_validation(url)
        self.token = token
    
    def url_validation(self, url):
        if '/?o=' in url:
            # if the workspace_id exists, lets remove it from the URL
            url = re.sub("\/\?o=.*", '', url)
        elif 'net/' == url[-4:]:
            url = url[:-1]
        elif 'com/' == url[-4:]:
            url = url[:-1]
        return url.rstrip("/")
    
    def get_login_credentials(self, profile='DEFAULT'):
        creds_path = '~/.databrickscfg'
        config = configparser.ConfigParser()
        abs_creds_path = path.expanduser(creds_path)
        config.read(abs_creds_path)
        try:
            current_profile = dict(config[profile])
            if not current_profile:
                raise ValueError(f"Unable to find a defined profile to run this tool. Profile \'{profile}\' not found.")
            return current_profile
        except KeyError:
            raise ValueError(
                'Unable to find credentials to load for profile. Profile only supports tokens.')

    def get_url_token(self): 
        return self.url, self.token
    
def get_libraries_cluster(token, workspace_url, cluster_id):
    url = f"{workspace_url}/api/2.0/libraries/cluster-status"
    print(f"{datetime.now()} Endpoint: {url}")
    print(f"{datetime.now()}           Getting list of libraries from clusters... ")
    st_response = requests.get(url, headers = {"Authentication": f"Bearer {token}"}, json = {"cluster_id": cluster_id})
    
    if st_response.status_code != 200:
        print(f"{datetime.now()} ERROR. ")
        print(st_response.content)
        return ''
    else:
        st_statuses = st_response.json()
        return st_statuses

def get_cluster_name(token, workspace_url):
    url = f"{workspace_url}/api/2.0/clusters/list"
    print(f"{datetime.now()} Endpoint: {url}")
    print(f"{datetime.now()}           Getting list of clusters from {workspace_url}... ")

    response = requests.get(url, headers = {"Authentication": f"Bearer {token}"})
    
    if response.status_code != 200:
        print(f"{datetime.now()} ERROR. ")
        raise Exception(response.content)
    else:
        return response.json()

# Find ST cluster_name from the ST cluster_id
def find_cluster_name(cluster_id, json_list):
    for i in json_list:
        if cluster_id == i['cluster_id']:
            return i['cluster_name']
    return ''
# Find E2 cluster id using the cluster_name
def find_cluster_id(cluster_name, json_list):
    for i in json_list:
        if cluster_name == i['cluster_name']:
            return i['cluster_id']
    return ''

def export_pipeline(old_profile, new_profile): 
    old_dbclient = dbclient(profile=old_profile)
    old_url, old_token = old_dbclient.get_url_token()

    st_clusters = get_cluster_name(old_token, old_url)

    new_dbclient = dbclient(profile=new_profile)
    new_url, new_token = new_dbclient.get_url_token()
    
    e2_clusters = get_cluster_name(new_token, new_url)

    st_clusters['clusters'] = [i for i in st_clusters['clusters'] if 'JOB' not in i['cluster_source']]
    e2_clusters['clusters'] = [i for i in e2_clusters['clusters'] if 'JOB' not in i['cluster_source']] 

    st_statuses = []
    for i in st_clusters['clusters']:
        st_statuses.append(get_libraries_cluster(old_token, old_url, i['cluster_id']))
    
    no_libraries = []
    with_libraries = []
    for i in st_statuses:
        try:
            st_cname = find_cluster_name(i['cluster_id'], st_clusters['clusters'])
            if st_cname != '':
                e2_cid = find_cluster_id(st_cname, e2_clusters['clusters'])
                if e2_cid != '':
                    print(f"{datetime.now()} Creating Cluster ID Mapping... ")
                    print(f"{' '*26} Cluster Name: {st_cname}    {i['cluster_id']} -> {e2_cid}")
                    i['cluster_id'] = e2_cid
                    with_libraries.append({
                        'cluster_id': e2_cid,
                        'libraries': [j['library'] for j in i['library_statuses']]
                    })
                else:
                    print(f"{datetime.now()} Error: Cannot find the cluster {st_cname} in new workspace")
            else:
                print(f"{datetime.now()} Error: Cannot find the cluster id {i['cluster_id']} in the original workspace")
        except Exception as e:
            no_libraries.append(i['cluster_id'])

    return with_libraries, no_libraries

def install_library(token, workspace_url, data):
    library_install_url = f"{workspace_url}/api/2.0/libraries/install"
    print(f"{datetime.now()} Endpoint: {library_install_url}")
    print(f"{datetime.now()}           Installing libraries on new clusters... ")

    for i in data:
        response = requests.post(library_install_url, headers = {"Authentication": f"Bearer {token}"}, json=i)
        
        if response.status_code == 200:
            print(f"{datetime.now()} Successfully added libraries for", i['cluster_id'])
        else:
            print(f"{datetime.now()} Error: Cannot add libraries for", i['cluster_id'])
            print(response.content)

def import_pipeline(new_profile, data):
    new_dbclient = dbclient(profile=new_profile)
    new_url, new_token = new_dbclient.get_url_token()
    install_library(new_token, new_url, data) 
    return


def main():
    all_args = argparse.ArgumentParser()
    all_args.add_argument('--old-profile', dest="old", help="Profile of the old workspace. ")
    all_args.add_argument('--new-profile', dest="new", help="Profile of the new workspace. ")
    args = all_args.parse_args()

    old_dbclient = args.old 
    new_dbclient = args.new

    print(f"{datetime.now()} EXPORTING LIBRARIES... ")
    libraries_data, no_libraries = export_pipeline(old_dbclient, new_dbclient)
    print()
    confirm = input(f"Import? (y/N) ")
    if confirm.lower() in ["y", "yes"]:
        print(f"{datetime.now()} IMPORTING LIBRARIES... ")
        import_pipeline(new_dbclient, libraries_data)
    else: 
        print(f"{datetime.now()} EXITING PIPELINE... ")

if __name__ == "__main__":
    main()
