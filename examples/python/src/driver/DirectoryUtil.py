import glob
import logging
import os


# All modules/files of the project located in project folder.
# Returns base directory dynamically runtime. (i.e.: ...\username\vaem\..)
def get_root_directory(root_dir_level_back):
    current_project_dir = os.path.dirname(os.path.abspath("__file__"))

    for i in range(root_dir_level_back):
        current_project_dir = os.path.split(current_project_dir)[0]

    return current_project_dir


# Gets file_name along with extension (i.e.:  filename.json) changes to given extension (filename.db)
# It's useful for automation script of schema creation
def change_extension(file_name, extension):
    file_name_base = os.path.splitext(file_name)[0]
    return file_name_base + "." + extension


# Lists all file under the given directory and the subdirectories by given extension type ('*' is wildcard)
def list_all_files(full_path, extension):
    search_pattern = os.path.join(full_path, '**', f'*.{extension}')
    matching_files = glob.glob(search_pattern, recursive=True)

    return matching_files


# Checks the directory and creates if not exists
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created directory: {directory}")
    else:
        logging.info(f"Directory already exists: {directory}")
