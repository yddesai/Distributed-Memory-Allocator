import os
import json

def merge_and_sort_json(input_folder, output_file):
    merged_data = []

    # Iterate through all files in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith('.json'):
            file_path = os.path.join(input_folder, filename)
            try:
                # Load the content of the JSON file
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        merged_data.extend(data)  # Add data to the merged list
                    else:
                        print(f"File {filename} does not contain a list of JSON objects. Skipping.")
            except Exception as e:
                print(f"Error reading {filename}: {e}")

    # Sort the merged data by 'id'
    merged_data = sorted(merged_data, key=lambda x: x['id'])

    # Write the sorted data to the output file
    try:
        with open(output_file, 'w') as f:
            json.dump(merged_data, f, indent=4)
        print(f"Merged and sorted JSON written to {output_file}")
    except Exception as e:
        print(f"Error writing output file: {e}")

if __name__ == "__main__":
    # Take folder path as input from the user
    input_folder = input("Enter the path to the folder containing JSON files: ").strip()
    output_file = "merged_sorted.json"  # Output file name

    # Validate the input folder
    if not os.path.isdir(input_folder):
        print("The provided path is not a valid folder. Please check and try again.")
    else:
        # Call the function to merge and sort JSON files
        merge_and_sort_json(input_folder, output_file)
