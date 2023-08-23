import os
import argparse
from shutil import move
import laspy
from dask import delayed, compute
from dask.distributed import Client
'''
Input = folder containing LAZ files, including subfolders
Output = input LAZ files converted to LAS
optional args = '--destroy' which will get rid of the input LAZ files. By default it moves the files to a subfolder called "LAZ_old"
'''
def convert_laz_to_las(laz_file, output_las_file):
    laz_data = laspy.read(laz_file)
    laz_data.write(output_las_file)
    
def find_laz_files(input_folder):
    #searches recursively for .laz files
    #reports file location and count
    laz_files = [os.path.join(root, file) for root, _, files in os.walk(input_folder) for file in files if file.endswith('.laz')]
    folder_counts = {}
    for laz_file in laz_files:
        folder = os.path.dirname(laz_file)
        folder_counts[folder] = folder_counts.get(folder, 0) + 1

    for folder, count in folder_counts.items():
        print(f"Found {count} LAZ files in folder: {folder}")

    return laz_files

def confirm_action(action, laz_files, input_folder):
    # prompts user input to confirm operation
    confirmation = input(f"Are you sure you want to {action} {len(laz_files)} LAZ files in {input_folder}? (yes/no): ")
    return confirmation.lower() == 'yes'

def main():
    # args
    parser = argparse.ArgumentParser(description="Convert LAZ files to LAS and move or destroy the LAZ files.")
    parser.add_argument("input_folder", help="Input folder to search for LAZ files.")
    parser.add_argument("--destroy", action="store_true", help="If specified, deletes the LAZ files instead of moving them.")
    args = parser.parse_args()

    # Error handling to ensure user has write permissions
    if not os.access(args.input_folder, os.W_OK):
        print(f"No write permissions for folder {args.input_folder}.")
        return
    
    #Error handling and connect to DASK client
    try:
        client = Client("10.8.20.50:8786")
    except Exception as e:
        print(f"Error connecting to Dask cluster: {e}")
        return
        
    #get count of files, check if empty
    laz_files = find_laz_files(args.input_folder)
    total_files = len(laz_files)

    if not total_files:
        print("No LAZ files found.")
        return
    
    #Confirm user input
    action = "destroy" if args.destroy else "move"
    if not confirm_action(action, laz_files, args.input_folder):
        print("Aborted.")
        return

    #Create LAZ_old folder
    laz_folder = os.path.join(args.input_folder, "LAZ_old")
    if not args.destroy and not os.path.exists(laz_folder):
        os.mkdir(laz_folder)

    #initialize Dask tasks
    tasks = []
    first_file = True
    for laz_file in laz_files:
        if first_file:
            print(f"Beginning processing with file: {laz_file}")
            first_file = False
        
        las_file = os.path.splitext(laz_file)[0] + ".las"
        task = delayed(convert_laz_to_las)(laz_file, las_file)
        tasks.append(task)

    # Compute the results
    compute(*tasks)

    #process, delete files if specified
    for laz_file in laz_files:
        dest_file = os.path.join(laz_folder, os.path.basename(laz_file))
        if args.destroy:
            os.remove(laz_file)
        else:
            if os.path.exists(dest_file):
                print(f"Destination file {dest_file} already exists. Deleting original {laz_file}.")
                os.remove(laz_file)
            else:
                move(laz_file, laz_folder)

        print(f"Processed {laz_file}.")

    print("Processing complete.")

if __name__ == "__main__":
    main()
