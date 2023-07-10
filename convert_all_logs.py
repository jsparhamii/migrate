###################### importing other scripts ##############################################
from utils import to_csv as util
from utils import create_asset_mapping_spreadsheet as create_spreadsheet
############################################################################################
import argparse
import os

def main(checkpoint, destination="csv"):
    # where you want the csv files to be located
    # make the csv directory if its not there
    if destination not in os.listdir():
        print(f"Creating {destination}...")
        os.mkdir(f"./{destination}")

    # users
    users_data = util.read_log("users.log", checkpoint)
    if users_data == 1: 
        print("users.log not found in checkpoint session")
    else:
        users_df = util.create_users(users_data)
        util.save_to_csv(users_df, "users.csv", destination)

    # instance profiles
    ip_data = util.read_log("instance_profiles.log", checkpoint)
    if ip_data == 1: # file not found
        print("instance_profiles.log not found in checkpoint session. Skipping...")
    else: 
        ip_df = util.create_instance_profiles(ip_data)
        util.save_to_csv(ip_df, "instance_profiles.csv", destination)
    
    # instance pools
    ipo_data = util.read_log("instance_pools.log", checkpoint)
    if ipo_data == 1: #file not found
        print("instance_pools.log not found in checkpoint session. Skipping...")
    else:
        ipo_df = util.create_instance_pools(ipo_data)
        util.save_to_csv(ipo_df, "instance_pools.csv", destination)

    # groups
    groups_df = util.create_groups("groups", checkpoint)
    util.save_to_csv(groups_df, "groups.csv", destination)

    # clusters
    clusters_data = util.read_log("clusters.log", checkpoint)
    if clusters_data ==1 : #file not found 
        print("clusters.log not found in checkpoint session. Skipping... ")
    else: 
        clusters_df = util.create_clusters(clusters_data)
        util.save_to_csv(clusters_df, "clusters.csv", destination)
    
    # cluster policies 
    cluster_policies_data = util.read_log('cluster_policies.log', checkpoint)
    if cluster_policies_data == 1: #file not found
        print("cluster_policies.log not found in checkpoint session. Skipping... ")
    else:
        clusters_policies_df = util.create_cluster_policies(cluster_policies_data)
        util.save_to_csv(clusters_policies_df, "cluster_policies.csv", destination)
        
    # job
    jobs_data = util.read_log('jobs.log', checkpoint)  
    if jobs_data == 1: #file not found
        print("jobs.log not found in checkpoint session. Skipping... ")
    else:
        jobs_acls = util.read_log('acl_jobs.log', checkpoint)
        jobs_df = util.create_jobs(jobs_data, jobs_acls)
        util.save_to_csv(jobs_df, "jobs.csv", destination)

    # shared
    shared_df = util.create_shared_logs("artifacts/Shared", checkpoint)
    util.save_to_csv(shared_df, 'global_shared_logs.csv', destination)

    # other artificats
    other_df = util.create_other_artifacts("artifacts", checkpoint)
    util.save_to_csv(other_df, "global_logs.csv", destination)

    # libraries
    libraries_data = util.read_log("libraries.log", checkpoint)
    if libraries_data == 1: # not found
        print("libraries.log not found in checkpoint session. Skipping...")
    else: 
        libraries_df = util.create_libraries(libraries_data)
        util.save_to_csv(libraries_df, "libraries.csv", destination)
        
    # secret scopes
    scopes_df = util.create_scopes("secret_scopes", checkpoint)
    util.save_to_csv(scopes_df, "secret_scopes.csv", destination)

    # metastore
    metastore_df = util.create_metastore(checkpoint, directory_name = 'metastore')
    util.save_to_csv(metastore_df, "metastore.csv", destination)

    create_spreadsheet.csv_to_excel(f"./{destination}")
    print("Successfully created spreadsheet asset_mapping.xlsx. ")

if __name__ == "__main__":


    all_args = argparse.ArgumentParser()
    all_args.add_argument("--checkpoint", "--session", dest="checkpoint", default="", help="set if you are using a checkpoint during export")
    all_args.add_argument("--destination", dest="destination", default="csv", help="destination of converted logs (default: /csv)")

    args = all_args.parse_args()
    main(args.checkpoint, args.destination)
