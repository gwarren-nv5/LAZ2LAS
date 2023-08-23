import os
import argparse
from shutil import move
from laspy.file import File

def convert_laz_to_las(laz_file, las_file):
    laz = File(laz_file, mode="r")
    las = File(las_file, mode="w", header=laz.header)
    las.points = laz.points
    laz.close()
    las.close()

def find_laz_files(input_folder):
    return [os.path.join(root, file) for root, dirs, files in os.walk(input_folder) for file in files if file.endswith('.laz')]

def main():
    parser = argparse.ArgumentParser(description="Convert LAZ files to LAS and move or destroy the LAZ files.")
    parser.add_argument("input_folder", help="Input folder to search for LAZ files.")
    parser.add_argument("--destroy", action="store_true", help="If specified, deletes the LAZ files instead of moving them.")
    args = parser.parse_args()

    laz_files = find_laz_files(args.input_folder)
    print(f"Found {len(laz_files)} LAZ files in {args.input_folder}.")

    if not laz_files:
        print("No LAZ files found.")
        return

    action = "destroy" if args.destroy else "convert"
    confirmation = input(f"Are you sure you want to {action} {len(laz_files)} LAZ files in {args.input_folder}? (yes/no): ")
    if confirmation.lower() != 'yes':
        print("Aborted.")
        return

    laz_folder = os.path.join(args.input_folder, "LAZ")
    if not args.destroy and not os.path.exists(laz_folder):
        os.mkdir(laz_folder)

    for laz_file in laz_files:
        las_file = os.path.splitext(laz_file)[0] + ".las"
        convert_laz_to_las(laz_file, las_file)
        
        if args.destroy:
            os.remove(laz_file)
        else:
            move(laz_file, laz_folder)
        
        print(f"Processed {laz_file}.")

    print("Processing complete.")

if __name__ == "__main__":
    main()
