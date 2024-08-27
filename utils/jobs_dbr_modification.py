import json
import sys

def modify_json_file(input_file, output_file, new_dbr_job_ids, new_spark_version, default_spark_version):
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            # Read each line from the input file
            for line in infile:
                try:
                    # Parse the JSON string into a dictionary
                    data = json.loads(line)
                    
                    # Modify the spark_version in the new_cluster
                    # if "settings" in data and "new_cluster" in data["settings"]:
                    #     data["settings"]["new_cluster"]["spark_version"] = new_spark_version

                    job_id = data.get("job_id")


                    if job_id in new_dbr_job_ids:

                        spark_version_to_use = new_spark_version

                    else:

                        spark_version_to_use = default_spark_version


                    if "job_clusters" in data['settings']:
                        for i, job_cluster in enumerate(data['settings']["job_clusters"]):
                            

                            data['settings']["job_clusters"][i]['new_cluster']['spark_version'] = spark_version_to_use


                    if "tasks" in data["settings"].keys():
                        # Multi-task
                        for i, task in enumerate(data["settings"]["tasks"]):
                            

                            if "new_cluster" in task:

                                data["settings"]["tasks"][i]["new_cluster"]['spark_version'] = spark_version_to_use

                           
                    else:
                        # Single-task
                        
                        if "new_cluster" in data['settings'].keys():

                            data["settings"]["new_cluster"]['spark_version'] = spark_version_to_use
                            
                    
                    # Convert the modified dictionary back into a JSON string
                    modified_json_line = json.dumps(data)
                    
                    # Write the modified JSON to the output file
                    outfile.write(modified_json_line + '\n')
                
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}", file=sys.stderr)
    
    except IOError as e:
        print(f"Error opening or writing to file: {e}", file=sys.stderr)

if __name__ == "__main__":
    # Replace 'input.json' and 'output.json' with your actual file paths
    input_file = './jobs_logs_testing/LL_jobs.log'
    output_file = './jobs_logs_testing/LL_updated_jobs.log'

    new_dbr_job_ids = [1009, 863]
    
    # Modify the JSON file with the new spark version
    modify_json_file(input_file, output_file, new_dbr_job_ids, new_spark_version="15.4.x-scala2.12", default_spark_version= "14.3.x-scala2.12")

    print(f"Modified JSON written to {output_file}")
