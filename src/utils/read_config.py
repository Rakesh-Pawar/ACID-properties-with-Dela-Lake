import configparser
import os


def read(path, section):
    """
    Reads the configuration file and returns the configuration object.

    This function determines the absolute path to the project's root directory
    and constructs the absolute path to the configuration file located in the
    'config' directory. It then reads the configuration file and returns the
    configuration object.

    Args:
        path (str): The relative path to the project's root directory.
        section (str): The section in the configuration file to be read.

    Returns:
        configparser.ConfigParser: The configuration object containing the parsed configuration file.
    """
    project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    config_file_path = os.path.join(project_root_path, 'config', 'config.ini')

    config = configparser.ConfigParser()
    config.read(config_file_path)

    file = config.get(path, section)
    return file
