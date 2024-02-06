import argparse
import os
import shutil
import csv 
import json 
import requests 

class dbclient:
    """
    Rest API Wrapper for Databricks APIs
    """
    # set of http error codes to throw an exception if hit. Handles client and auth errors
    http_error_codes = (401, 403)

    def __init__(self, token, url):
        self._token = {'Authorization': 'Bearer {0}'.format(token)}
        self._url = url.rstrip("/")
        self._is_verbose = False
        self._verify_ssl = False
        if self._verify_ssl:
            # set these env variables if skip SSL verification is enabled
            os.environ['REQUESTS_CA_BUNDLE'] = ""
            os.environ['CURL_CA_BUNDLE'] = ""

    def is_aws(self):
        return self._is_aws

    def is_verbose(self):
        return self._is_verbose

    def is_skip_failed(self):
        return self._skip_failed

    def test_connection(self):
        # verify the proper url settings to configure this client
        if self._url[-4:] != '.com' and self._url[-4:] != '.net':
            print("Hostname should end in '.com'")
            return -1
        results = requests.get(self._url + '/api/2.0/clusters/spark-versions', headers=self._token,
                               verify=self._verify_ssl)
        http_status_code = results.status_code
        if http_status_code != 200:
            print("Error. Either the credentials have expired or the credentials don't have proper permissions.")
            print("If you have a ~/.netrc file, check those credentials. Those take precedence over passed input.")
            print(results.text)
            return -1
        return 0

    def get(self, endpoint, json_params=None, version='2.0', print_json=False):
        if version:
            ver = version
        full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
        if self.is_verbose():
            print("Get: {0}".format(full_endpoint))
        if json_params:
            raw_results = requests.get(full_endpoint, headers=self._token, params=json_params, verify=self._verify_ssl)
            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: GET request failed with code {}\n{}".format(http_status_code, raw_results.text))
            results = raw_results.json()
        else:
            raw_results = requests.get(full_endpoint, headers=self._token, verify=self._verify_ssl)
            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: GET request failed with code {}\n{}".format(http_status_code, raw_results.text))
            results = raw_results.json()
        if print_json:
            print(json.dumps(results, indent=4, sort_keys=True))
        if type(results) == list:
            results = {'elements': results}
        results['http_status_code'] = raw_results.status_code
        return results

    def http_req(self, http_type, endpoint, json_params, version='2.0', print_json=False, files_json=None):
        if version:
            ver = version
        full_endpoint = self._url + '/api/{0}'.format(ver) + endpoint
        if self.is_verbose():
            print("{0}: {1}".format(http_type, full_endpoint))
        if json_params:
            if http_type == 'post':
                if files_json:
                    raw_results = requests.post(full_endpoint, headers=self._token,
                                                data=json_params, files=files_json, verify=self._verify_ssl)
                else:
                    raw_results = requests.post(full_endpoint, headers=self._token,
                                                json=json_params, verify=self._verify_ssl)
            if http_type == 'put':
                raw_results = requests.put(full_endpoint, headers=self._token,
                                           json=json_params, verify=self._verify_ssl)
            if http_type == 'patch':
                raw_results = requests.patch(full_endpoint, headers=self._token,
                                             json=json_params, verify=self._verify_ssl)
            
            http_status_code = raw_results.status_code
            if http_status_code in dbclient.http_error_codes:
                raise Exception("Error: {0} request failed with code {1}\n{2}".format(http_type,
                                                                                      http_status_code,
                                                                                      raw_results.text))
            results = raw_results.json()
        else:
            print("Must have a payload in json_args param.")
            return {}
        if print_json:
            print(json.dumps(results, indent=4, sort_keys=True))
        # if results are empty, let's return the return status
        if results:
            results['http_status_code'] = raw_results.status_code
            return results
        else:
            return {'http_status_code': raw_results.status_code}

    def post(self, endpoint, json_params, version='2.0', print_json=False, files_json=None):
        return self.http_req('post', endpoint, json_params, version, print_json, files_json)

    def put(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('put', endpoint, json_params, version, print_json)

    def patch(self, endpoint, json_params, version='2.0', print_json=False):
        return self.http_req('patch', endpoint, json_params, version, print_json)


def get_user_id_mapping(client):
        # return a dict of the userName to id mapping of the new env
        user_list = client.get('/preview/scim/v2/Users').get('Resources', None)
        if user_list:
            user_mapping = {}
            for user in user_list:
                user_name = user['userName']
                # user_name: ex. ABC123@ccwdata.org
                # we need to remove the domain name
                user_mapping[user_name.split('@')[0]] = user_name
            return user_mapping
        return None


def pretty_print_dict(dict_):
    """
    summary: prints a dictionary object in a pretty format

    PARAMETERS:
    dict_: dictionary object

    RETURNS:
    n/a
    """
    for key, value in dict_.items():
        print(f"{key}: {value}")

def write_json(file_name, data_write):
    """
    summary: writes parameter data_write to the path indicated by parameter
    file_name

    PARAMETERS:
    file_name: path of the file that is to be written
    data_write: text object

    RETURNS:
    n/a
    """
    with open(file_name, "w") as f:
        f.write(data_write)


def map(file_name, mapping):
    """
    summary: reads parameter file_name and replaces all places where previous email
    address is used with the new item as indicated in mapping

    PARAMETERS:
    file_name: path of the file that is to be read
    mapping: dict where key is the previous item and value is the
    new item

    RETURNS:
    data: a text object

    """
    with open(file_name, "r") as f:
        data = f.read()
        print(f"   Currently mapping {file_name}")
    for e in mapping:
        if "@" in mapping[e]: # this is an user
            data = data.replace(f"\"user_name\": \"{e}\"", f"\"user_name\": \"{mapping[e]}\"") # in most ACLs
            print(f"\"/Users/{e}/")
            print(f"\"/Users/{mapping[e]}/")
            data = data.replace(f"\"/Users/{e}/", f"\"/Users/{mapping[e]}/") # in notebook paths
            data = data.replace(f"\"display\": \"{e}\"", f"\"display\": \"{mapping[e]}\"") # in groups
            data = data.replace(f"\"userName\": \"{e}\"", f"\"userName\": \"{mapping[e]}\"") # in groups
            data = data.replace(f"\"principal\": \"{e}\"", f"\"principal\": \"{mapping[e]}\"") # in secret ACLs
        else: # this is a service principal
            data = data.replace(f"\"user_name\": \"{e}\"", f"\"service_principal_name\": \"{mapping[e]}\"") # in most ACLs
            data = data.replace(f"\"display\": \"{e}\"", f"\"display\": \"{mapping[e]}\"") # in groups
            data = data.replace(f"\"principal\": \"{e}\"", f"\"principal\": \"{mapping[e]}\"") # in secret ACLs

    return data

def rename_users_folder(mapping):
    """
    summary: renames the user folder by moving all files to new directory

    PARAMETERS:
    mapping: dict where key is the previous item and value is the
    new item

    RETURNS:
    n/a
    """
    import shutil

    users = os.listdir('./artifacts/Users')
    for u in users:
        if '.DS_Store' not in u:
            if mapping.get(u, False):
                shutil.move("./artifacts/Users/"+u, "./artifacts/NewUsers/"+mapping[u])
            else:
                shutil.move("./artifacts/Users/"+u, "./artifacts/NewUsers/"+u)

    os.rename("./artifacts/Users", "./artifacts/EmptyDir") # this is an empty dir
    os.rename("./artifacts/NewUsers", "./artifacts/Users")


def mapping_file(file_name, mapping):
    """
    summary: maps a single file and writes it to a new file and saves the old
    log file with the '_prev' suffix

    PARAMETERS:
    file_name: path of the file to map
    mapping: dict where key is the previous item and value is the
    new item

    RETURNS:
    n/a
    """
    # this code here (directly referencing the number 4) assumes that the file name
    # has the 3 letter extension (e.g. something.txt or something.csv
    data = map(file_name, mapping)
    write_json(file_name, data)


def main():
    all_args = argparse.ArgumentParser()
    all_args.add_argument("--file", dest="file", required=True, help='files to map. e.g. logs/session')
    all_args.add_argument("--host", dest="host", required=True, help='host of the new databricks env')
    all_args.add_argument("--token", dest="token", required=True, help='token of the new databricks env')

    args = all_args.parse_args()
    file_name = args.file

    client = dbclient(args.token, args.host)
    mapping = get_user_id_mapping(client)
    #mapping = {"admin": "ADMIN_NEW@GMAIL.COM", "service_principal": "service_principal_id"}
    print("--------------------")
    pretty_print_dict(mapping)
    print("--------------------")
    yesno = input("Confirm mapping (y/n): ")
    if yesno.lower() != "y":
        exit()


    # change the current working director to specified path
    #os.chdir("logs/session")
    os.chdir(file_name)
    # verify the path using getcwd()
    cwd = os.getcwd()
    print("--------------------")
    print("Current working directory is:", cwd)

    logs = os.listdir()

    for file in logs:
        # making sure we are only getting the logs
        if ".log" in file:
            mapping_file(file, mapping)
        if "groups" == file:
            groups = os.listdir("groups")
            for g in groups:
                if g != ".DS_Store":
                    mapping_file("groups/"+g, mapping)


    #rename_users_folder(mapping)

if __name__ == "__main__":
    main()
