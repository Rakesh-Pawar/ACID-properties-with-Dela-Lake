import os

from src.utils.define_spark import spark
from src.utils.read_config import read


def read_delta_table(path):
    """
    This function used to read delta table
    :param path: delta tables path
    :return: dataframe
    """
    df = spark.read.format("delta").load(path)

    return df


if __name__ == '__main__':
    project_root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    output_dir_path = project_root_path + read('PATHS', 'output_path')
    delta_table_path = output_dir_path + "/delta_table"
    df = read_delta_table(delta_table_path)
    df.show()
