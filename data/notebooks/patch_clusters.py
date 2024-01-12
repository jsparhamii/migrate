import json
from datetime import datetime
import os
import requests
import argparse

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

def read_log(file_name):
    """
    summary: reads a given log
    """
    try:
        with open(file_name) as f:
            data = f.read().split("\n")
        return data
    except FileNotFoundError as e:
        return print(f"{datetime.now()}  Error: {file_name} not found. ")
    except Exception as e:
        print(f"{datetime.now()}  Error: There was an unknown error reading {file_name}. ")
        print(e)
        return ''

def get_clusters_list(client): 
    # databricks clusters list
    endpoint = "/clusters/list"
    clusters_list = client.get(endpoint).get('clusters', [])
    return clusters_list

def get_clusters_ips(log_name): 
    data = read_log(log_name)
    instance_profiles = {}
    for d in data:
        if len(d) != 0:
            d = d.strip()
            d = json.loads(d)
            c_name = d.get('cluster_name', 0)
            ip = d.get('aws_attributes', {}).get('instance_profile_arn', 0)
            if ip != 0:
                instance_profiles[c_name] = ip
    return instance_profiles

def update_cluster_ips(client, clusters_list, instance_profiles):
    for c in clusters_list:
        c_name = c.get('cluster_name', 0)
        if c_name in instance_profiles.keys():
            c['aws_attributes']['instance_profile_arn'] = instance_profiles[c_name]
            endpoint = "/clusters/edit"
            json_params = c
            client.patch(endpoint, json_params)

    return clusters_list

def confirm_updated_ips(client, instance_profiles): 
    cnames = get_clusters_list(client)
    for c in cnames: # in updated e2 clusters
        c_name = c.get('cluster_name', 0)
        ip = c.get('aws_attributes', {}).get('instance_profile_arn', 0) # updated ip? 
        if c_name in instance_profiles.keys(): 
            if ip != 0:
                if ip != instance_profiles[c_name]:
                    print(f"{datetime.now()}  Error: {c_name} was not updated. ")
                else:
                    print(f"{datetime.now()}  {c_name} was updated. ")
            else:
                print(f"{datetime.now()}  Error: {c_name} was not updated. ")
        else: 
            print(f"{datetime.now()}  {c_name} did not require update. ")

if __name__ == "__main__":
    # get the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", help="log file to read", default="logs/session/clusters.log")
    parser.add_argument("--token", help="databricks token to use", default="<invalid_token>")
    parser.add_argument("--url", help="databricks url to use", default="<invalid_url>")
    args = parser.parse_args()

    client = dbclient(args.token, args.url)
    cnames = get_clusters_list(client)
    ips = get_clusters_ips(log_name=args.log)
    update_cluster_ips(client, cnames, ips)
    confirm_updated_ips(client, ips)