import argparse
import requests
import json
import sys
import time
import os
import datetime
import configparser
import re


class Databricks(object):

    def __init__(self, **kwargs):
        profile = kwargs['profile'] if 'profile' in kwargs else 'DEFAULT'
        login = self.get_login_credentials(profile)
        url = login['host']
        token = login['token']  
        self.host = self.url_validation(url)
        self.token = token
        print(f"Running on {self.host}")
        self.check_file = kwargs['check_file'] if 'check_file' in kwargs else None
        self.session = kwargs['session'] if 'session' in kwargs else None
        self.retry_backoff = kwargs['retry_backoff'] if 'retry_backoff' in kwargs else 0.1

    def progress(self, _cur, _max):
        p = round(100*_cur/_max)
        b = f"Progress: {_cur}/{_max}"
        print(b, end="\r")

    def collect_jobs(self):
        host = self.host
        token = self.token
        jobs_list = requests.get("{db_url}/api/2.0/jobs/list".format(db_url=host), headers={
            "Authorization": "Bearer {bearer_token}".format(bearer_token=token),
            "Content-Type": "application/json"})
        log_file = "./logs/delete_jobs.log"
        logger = open(log_file, 'w+')
        logger.write("NEW RUN LOGGED: " + str(datetime.datetime.now()) + "\n")
        logger.write("..." * 5 + "\n")
        jobs = jobs_list.json()['jobs']
        job_ids = []
        job_ids = [{'job_id': job['job_id'], 'created_time': job['created_time']} for job in jobs]
        job_ids = sorted(job_ids, key=lambda i: i['job_id'])
        job_names_e2 = [job['settings']['name'] for job in jobs]
        print("Total jobs: " + str(len(job_ids)))
        logger.write("Total jobs: " + str(len(job_ids)) + "\n")
        print("..." * 5, end="\r")
        job_names = []
        if self.check_file:
            with open(self.check_file) as f:
                check_file = f.readlines()

            check_file = [x.split(',')[1] for x in check_file]
            check_file = [x.strip() for x in check_file]
            print("Total jobs to check: " + str(len(check_file)))
            print("..." * 5, end="\r")
            for job in jobs:
                if job['settings']['name'] in check_file:
                    job_names.append(job['settings']['name'])

            skipped_jobs = [job for job in check_file if job not in job_names_e2]
            print("Skipped jobs: " + str(len(skipped_jobs)))
            job_ids = [{'job_id': job['job_id'], 'created_time': job['created_time']} for job in jobs if job['settings']['name'] in job_names]
            logger.write("Total jobs to check: " + str(len(check_file)) + "\n")
            logger.write("..." * 5 + "\n")
            logger.write("Total jobs to delete: " + str(len(job_ids)) + "\n")
            logger.write("..." * 5 + "\n")
            logger.write("Not deleted jobs in E2: \n")
            logger.write(','.join([json.dumps({'job_id': job['job_id'], 'job_name': job['settings']['name'], 'created_time': job['created_time']}) for job in jobs if job['settings']['name'] not in job_names]))
            logger.write("\n")
            logger.write("Deleted jobs in E2: \n")
            logger.write(','.join([json.dumps({'job_id': job['job_id'], 'job_name': job['settings']['name'], 'created_time': job['created_time']}) for job in jobs if job['settings']['name'] in job_names]))
            logger.write("\n")
            logger.write("Check jobs not found in E2: \n")
            logger.write(','.join(skipped_jobs))
            logger.close()

        print("Total jobs to delete: " + str(len(job_ids)))
        print("List of job names to delete: " + str(job_names))
        user_response = input("Do you want to continue (y/n): ")
        if str(user_response).lower() != 'y':
            sys.exit(1)
        return job_ids
    
    def collect_clusters(self):
        host = self.host
        token = self.token
        clusters_list = requests.get("{db_url}/api/2.0/clusters/list".format(db_url=host), headers={
            "Authorization": "Bearer {bearer_token}".format(bearer_token=token),
            "Content-Type": "application/json"})
        clusters = clusters_list.json()['clusters']
        cluster_ids = [{'cluster_id': cluster['cluster_id'], 'state': cluster['state']} for cluster in clusters]
        cluster_ids = sorted(cluster_ids, key=lambda i: i['cluster_id'])
        print("Total clusters: " + str(len(cluster_ids)))
        print("..." * 5, end="\r")
        return cluster_ids
    
    def delete_clusters(self):
        host = self.host
        token = self.token

        cluster_ids = self.collect_clusters()
        output_file = f"./logs/{self.session}/delete_clusters.log"
        fd = open(output_file, 'a+')
        print("*" * 80, file=fd)
        print("NEW RUN LOGGED: " + str(datetime.datetime.now()), file=fd)
        print("cluster_id,status", file=fd)
        cluster_num = 0
        cluster_max = len(cluster_ids)
        for cluster_id in cluster_ids:
            if cluster_id['state'] == 'RUNNING':
                print("Cluster " + str(cluster_id['cluster_id']) + " is running. So not deleting this cluster")
                self.progress(cluster_num, cluster_max)
                cluster_num += 1
                continue
            data = {
                "cluster_id": "{cluster_id}".format(cluster_id=cluster_id['cluster_id'])
            }
            result = requests.post("{db_url}/api/2.0/clusters/delete".format(db_url=host), headers={"Authorization": "Bearer {bearer_token}".format(bearer_token=token), "Content-Type": "application/json"}, json=data)
            print("{cluster_id},{status}".format(cluster_id=cluster_id, status=result.status_code), file=fd)
            self.progress(cluster_num, cluster_max)
            cluster_num += 1
        print("..." * 5, end="\r")
        print("Done")
        fd.close()
    
    def progress_bar(self, current, total, starttime, currenttime, barLength = 20):
        percent = (current / total) * 100
        arrow = '-' * int(percent / 100 * barLength - 1) + '>'
        spaces = ' ' * (barLength - len(arrow))
        # want to do two decimal points
        time_elapsed = currenttime - starttime
        time_remaining = (time_elapsed / (current + 1)) * (total - (current + 1))
        time_remaining_fmt = str(datetime.timedelta(seconds=time_remaining))
        print(f'Progress: [{arrow + spaces}] {percent:.2f}% Estimated time remaining: {time_remaining_fmt}', end='\r')

    def delete_jobs(self):
        host = self.host
        token = self.token

        job_ids = self.collect_jobs()
        output_file = f"./logs/{self.session}/delete_jobs.log"
        fd = open(output_file, 'a+')
        print("*" * 80, file=fd)
        print("NEW RUN LOGGED: " + str(datetime.datetime.now()), file=fd)
        print("job_id,status", file=fd)
        job_num = 0
        job_max = len(job_ids)
        for job_id in job_ids:
            job_runs = requests.get("{db_url}/api/2.0/jobs/runs/list?job_id={jobid}&active_only=true".format(db_url=host, jobid=job_id['job_id']), headers={"Authorization": "Bearer {bearer_token}".format(bearer_token=token), "Content-Type": "application/json"})
            if job_runs.status_code == 200 and "runs" in job_runs.json():
                print("Job " + str(job_id['job_id']) + " is active. So not deleting this job")
                self.progress(job_num, job_max)
                job_num += 1
                continue
            data = {
                "job_id": "{job_id}".format(job_id=job_id['job_id'])
            }
            result = requests.post("{db_url}/api/2.0/jobs/delete".format(db_url=host), headers={"Authorization": "Bearer {bearer_token}".format(bearer_token=token), "Content-Type": "application/json"}, json=data)
            print("{job_id},{status}".format(job_id=job_id, status=result.status_code), file=fd)
            self.progress(job_num, job_max)
            job_num += 1
        print("..." * 5, end="\r")
        print("Done")
        fd.close()
        
    def read_log_file(self, log_file):
        with open(log_file, 'r') as f:
            return f.readlines()
        
    def delete_workspace_obj(self, path):
        url = self.host
        token = self.token
        api_url = f"{url}/api/2.0/workspace/delete"
        fd = open(f"./logs/{self.session}/delete_notebooks.log", 'a+')
        print("Deleting: " + path, file=fd)
        payload = {'path': path}
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        response = requests.post(api_url, headers=headers, json=payload)
        print(response.text, file=fd)
        fd.close()
        return response
        
    def delete_notebooks(self):
        host = self.host
        token = self.token
        fd = open(f"./logs/{self.session}/delete_notebooks.log", 'a+')
        print("*" * 80, file=fd)
        print("NEW RUN LOGGED: " + str(datetime.datetime.now()), file=fd)
        fd.close()
        notebooks_list = self.read_log_file(f"./logs/{self.session}/user_workspace.log")
        print("Total notebooks: " + str(len(notebooks_list)))
        total = len(notebooks_list)
        starting_Time = time.time()
        for i, notebook in enumerate(notebooks_list):
            time.sleep(self.retry_backoff)
            current_time = time.time()
            self.progress_bar(i, total, starting_Time, current_time)
            response = self.delete_workspace_obj(json.loads(notebook).get("path"))

    def get_url_token(self):
        return self.url, self.token

    def url_validation(self, url):
        if '/?o=' in url:
            # if the workspace_id exists, lets remove it from the URL
            url = re.sub("/?o=.*", '', url)
        elif 'net/' == url[-4:]:
            url = url[:-1]
        elif 'com/' == url[-4:]:
            url = url[:-1]
        return url.rstrip("/")

    def get_login_credentials(self, profile='DEFAULT'):
        creds_path = '~/.databrickscfg'
        config = configparser.ConfigParser()
        abs_creds_path = os.path.expanduser(creds_path)
        config.read(abs_creds_path)
        try:
            current_profile = dict(config[profile])
            if not current_profile:
                raise ValueError(f"Unable to find a defined profile to run this tool. Profile '{profile}' not found.")
            return current_profile
        except KeyError:
            raise ValueError(
                'Unable to find credentials to load for profile. Profile only supports tokens.')


class InputHandler(object):
    def __init__(self):
        pass

    def get(self):
        parser = argparse.ArgumentParser(description='Delete databricks Jobs')
        parser.add_argument('-p', '--profile', dest='profile', required=True, help="Databricks Server URL")
        parser.add_argument('-c', '--check-file', dest='check_file', required=False, help="Check for job name in file")
        parser.add_argument('-s', '--session', dest='session', required=False, help="Session name")
        parser.add_argument('-t', '--task', dest='task', required=False, help="Task to perform. One of 'delete_jobs', 'delete_notebooks', 'delete_clusters'", default='delete_jobs')
        parser.add_argument('--retry-backoff', dest='retry_backoff', required=False, help="Retry backoff time", default=1.0)

        parse_input = parser.parse_args()

        if not parse_input.check_file and parse_input.task == 'delete_jobs':
            print("Check file not provided or not found")
            user_response = input("Do you want to continue without check file (y/n): ")
            if user_response.lower() != 'y':
                parser.print_help()
                sys.exit(1)

        return parse_input


if __name__ == '__main__':
    input_handler = InputHandler()
    parse_input = input_handler.get()
    dbObj = Databricks(profile=parse_input.profile, check_file=parse_input.check_file, session=parse_input.session, retry_backoff=parse_input.retry_backoff)
    if parse_input.task == 'delete_jobs':
        dbObj.delete_jobs()
    elif parse_input.task == 'delete_notebooks':
        dbObj.delete_notebooks()
    elif parse_input.task == 'delete_clusters':
        dbObj.delete_clusters()