import argparse
import logging
import os
import sqlite3
from pathlib import Path
from sqlite3 import Error

import AppConstants
import DirectoryUtil

# Update if run this script in a diff. directory to (3: (driver -> src -> python)
root_dir_level_back = 2
root_directory = Path(DirectoryUtil.get_root_directory(root_dir_level_back))
init_scripts = []


# Initializes schema and creates dashboard connection (also db file if it does not exist)
def init_schemas():
    """" check the db directory and create if not exist"""
    DirectoryUtil.create_directory(root_directory / AppConstants.DB_FILE_RELATIVE_DIR)

    """ create a dashboard connection to a SQLite dashboard """
    connection = None
    init_dict = {}  # holds <file, script> pairs

    for init_script in init_scripts:
        init_file = root_directory / AppConstants.DB_FILE_RELATIVE_DIR / Path(os.path.basename(init_script))
        init_file = DirectoryUtil.change_extension(init_file, "db")
        init_dict[init_file] = init_script

    for file, script in init_dict.items():
        try:
            connection = sqlite3.connect(file)
            cursor = connection.cursor()

            # create tables from the sql script
            with open(script, 'r') as sql_file:
                sql_script = sql_file.read()
                cursor.executescript(sql_script)
        except Error as e:
            print(e)
            logging.error(e)
        finally:
            if connection:
                connection.commit()
                connection.close()


# Gets sqlite dashboard init files from db\init folder
def get_db_init_files():
    return DirectoryUtil.list_all_files(root_directory / AppConstants.DB_FILE_RELATIVE_DIR, "sql")


def parse_options():
    parser = argparse.ArgumentParser(
        description="usage: SqliteSchemaInitializer.py [option] [input] i.e.: SqliteSchemaInitializer.py -f file1 "
                    "file2 ... ")
    parser.add_argument("-f", "--file", dest="files", default=get_db_init_files(), type=str, nargs='+',
                        help="Full path of SQL file(s). Without this argument program scan all SQL files under "
                             "db\\init folder.")
    args = parser.parse_args()
    global init_scripts
    init_scripts = args.files


if __name__ == '__main__':
    parse_options()
    init_schemas()
