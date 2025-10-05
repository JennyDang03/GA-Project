import re
import shutil
import os

# Define the file paths
file_path = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\backup-10282024.txt"
output_src_dir = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs"
output_dest_dir = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\graphs used - delete later"
tables_src_dir = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\tables"
tables_dest_dir = r"C:\Users\mathe\Dropbox\RESEARCH\pix\pix-event-study\Output\tables\tables used - delete later"

# Function to extract strings between given delimiters
def extract_strings_between(text, start_delim, end_delim):
    pattern = re.escape(start_delim) + r'(.*?)' + re.escape(end_delim)
    return re.findall(pattern, text)

# Read the file
with open(file_path, 'r') as file:
    content = file.read()

# Extract the lists
output_list = extract_strings_between(content, "{Output/", "}")
tables_list = extract_strings_between(content, "{tables/", "}")

# Function to copy files from source to destination
def copy_files(file_list, src_dir, dest_dir):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    for file_name in file_list:
        src_file = os.path.join(src_dir, file_name)
        dest_file = os.path.join(dest_dir, file_name)
        if os.path.exists(src_file):
            shutil.copy2(src_file, dest_file)
        else:
            print(f"File {src_file} not found.")

# Copy the files
copy_files(output_list, output_src_dir, output_dest_dir)
copy_files(tables_list, tables_src_dir, tables_dest_dir)

print("File copying completed.")








