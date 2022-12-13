import argparse
from utils.create_workspace import Workspace as Workspace

def main():
    # checkpoints are optional for export but you will need to use them for the import
    # (each workspace is a 'checkpoint')

    # takes two arguments: checkpoint and workspaces
    all_args = argparse.ArgumentParser()
    all_args.add_argument("--checkpoint", dest="checkpoint", default="", help="set if you are using a checkpoint during export")
    all_args.add_argument("--workspaces", dest="workspaces", nargs="+", required=True, help="list of workspace names. must match csv columns")

    args = all_args.parse_args()

    checkpoint = args.checkpoint
    workspaces = args.workspaces


    # for each workspace
    for w in workspaces:
        # create a workspace Class - refer to create_workspace.py
        # this instantiates the original location of the session and the new location of the session
        # it also instantiates another class Split - refer to split_logs.py
        # Split instantiates the same thing as well as two variables: imported users and imported groups (this is used for remaking ACLs)
        workspace = Workspace(checkpoint, w, workspaces)
        success = workspace.run()

    workspace.copy_other_files()

if __name__ == "__main__":
    main()
