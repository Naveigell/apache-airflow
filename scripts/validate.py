import os


def validate_file_exists(file):
    """
    Validate that a file exists.

    Raises a FileNotFoundError if the file does not exist.

    Parameters
    ----------
    file : str
        The path to the file to validate.

    """
    if not os.path.exists(file):
        raise FileNotFoundError(f"File {file} does not exist.")

    return True

def validate_row_counts(dataframe, count):
    """
    Validate that a DataFrame has the expected number of rows.

    Raises a ValueError if the DataFrame does not have the expected number of rows.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        The DataFrame to validate.
    count : int
        The expected number of rows in the DataFrame.

    """
    if len(dataframe) != count:
        raise ValueError(f"DataFrame does not have the expected number of rows. Expected {count}, got {len(dataframe)}.")

    return True


def validate_extraction(df):
    pass