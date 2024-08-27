import json
import pandas as pd
import csv
import os
import datetime
import argparse

def read_log(file_name):
    try:
        with open("./all_jobs/"+file_name) as f:
            data = f.read().split("\n")
        return data[:-1]
    except FileNotFoundError as e:
        return ''
    except Exception as e:
        print("Error while reading file:", file_name, "\n", e)
        return ''

def move_logs(timestamp=""):
    # moving all_jobs
    os.rename("jobs.log", f"./all_jobs/jobs{timestamp}.log")
    os.rename("acl_jobs.log", f"./all_jobs/acl_jobs{timestamp}.log")

def write_job_log(data, sample_job_ids):
    with open("jobs.log", "w") as jl:
        for d in data:
            try:
                d = json.loads(d)
                if d['job_id'] in sample_job_ids:
                    jl.write(json.dumps(d) + "\n")
            except:
                print("Error while writing jobs.log")


def write_job_acls_log(data, sample_job_ids):
    with open("acl_jobs.log", "w") as jal:
        for d in data:
            try:
                d = json.loads(d)
                if int(d['object_id'].split("/")[-1]) in sample_job_ids:
                    jal.write(json.dumps(d) + "\n")
            except:
                print("Error while writing acl_jobs.log")

def write_rest_job_logs(jobslog, acljobslog, sample_job_ids):
    with open("other_jobs.log", "w") as ojl:
        for d in jobslog:
            try:
                d = json.loads(d)
                if d['job_id'] not in sample_job_ids:
                    ojl.write(json.dumps(d) + "\n")
            except:
                print("Error while writing other_jobs.log")

    with open("other_acl_jobs.log", "w") as ojal:
        for d in acljobslog:
            try:
                d = json.loads(d)
                if int(d['object_id'].split("/")[-1]) not in sample_job_ids:
                    ojal.write(json.dumps(d) + "\n")
            except:
                print("Error while writing other_acl_jobs.log")

def main():

    job_ids = [410104035299, 30596903773550, 97211745563636]

    if "all_jobs" not in os.listdir():
        os.mkdir("./all_jobs/")
        move_logs()
    elif "jobs.log" in os.listdir():
        ts = datetime.datetime.now()
        move_logs("_"+str(ts))

    #json objects
    job_log_data = read_log("jobs.log")
    job_acl_log_data = read_log("acl_jobs.log")

    #move jobs.log into ./alljobs folder + write sample jobs log in main logs folder
    write_job_log(job_log_data, job_ids)
    write_job_acls_log(job_acl_log_data, job_ids)

    #write jobs.log that only contains jobs NOT in sample jobs log
    write_rest_job_logs(job_log_data, job_acl_log_data, job_ids)

if __name__ == "__main__":
    main()
