import argparse
import os
import shutil

def to_dict(csv_file):
    """
    summary: converts a csv or text file (or another comma delim file) into a
    dictionary object

    PARAMETERS:
    csv_file: path file of the comma delim file, assumes that there are no column
    headings, each user address is split by a new line, and the old and new
    address are split by a comma in that order.

    RETURNS:
    dict_from_csv: dictionary object where key is the old item and value
    is new item
    """
    import csv

    dict_from_csv = {}
    with open(csv_file, mode='r') as f:
        reader = csv.reader(f)
        # assuming that each row is "old address, new address" for a user
        dict_from_csv = {rows[2]:rows[6] for rows in reader}
    del dict_from_csv['userName']
    return dict_from_csv

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
        #print(f"Currently mapping {file_name}")
    for e in mapping:
        data = data.replace(e, mapping[e])
    return data

def write(file_name, data_write):
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
    write(file_name, data)

def main():
    all_args = argparse.ArgumentParser()
    all_args.add_argument("--dir", "--file", dest="directory", required=True, help='directory needs to be updated via mapping.')
    all_args.add_argument("-m", "--mapping", dest="mapping", required=True, help='one-to-one mapping provided by a comma delim file')

    args = all_args.parse_args()
    file_name = args.file
    mapping_file_ = args.mapping

    mapping = to_dict(mapping_file_)
    print("Mapping: ")
    print(mapping)
    print("--------------------")
    yesno = input("Confirm mapping (y/n): ")
    if yesno.lower() != "y":
        exit()

    # change the current working director to specified path
    os.chdir(file_name)
    # verify the path using getcwd()
    cwd = os.getcwd()
    print("Current working directory is:", cwd)

    logs = os.listdir()

    for file in logs:
        # making sure we are only getting the logs
        if ".log" in file:
            mapping_file(file, mapping)
        if "groups" == file:
            groups = os.listdir("groups")
            for g in groups:
                mapping_file("/groups/"+g, mapping)


    rename_users_folder(mapping)

if __name__ == "__main__":
    main()
