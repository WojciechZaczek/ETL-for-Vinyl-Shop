import datetime
import functools
import os
import pandas as pd
import shutil
from typing import Optional

timestamp = int(datetime.datetime.now().timestamp())


def rename_columns(dataframe: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """
    Rename columns in the DataFrame based on the provided mapping.

    :param dataframe: Input DataFrame with original column names.
    :param column_mapping: Dictionary mapping where keys are the original column
                           names and values are the new column names.
    :return: DataFrame with columns renamed based on the provided mapping.
    """
    dataframe = dataframe.rename(columns=column_mapping)
    return dataframe


class Transform:
    """Class responsible for transform data from the inbox folder"""

    def __init__(self, input_file: str, output_file: str, archive_folder: str) -> None:
        """
        Initialize the Transform class with input, output, and archive folder paths.

        :param input_file: Path to the folder containing input CSV files.
        :param output_file: Path to the folder where transformed files will be saved.
        :param archive_folder: Path to the folder where processed files will be archived.
        """
        self.config_file = os.path.join(f'{os.getcwd()}/dags/functionality/config', 'artist_config.csv')
        self.input_file = input_file
        self.output_file = output_file
        self.archive_folder = archive_folder
        self.mapping = {
            'ArtistProfile': 'profile',
            'MusicStyle': 'style',
            'AudioFormat': 'audio_format',
            'AlbumFormat': 'album_format',
            'AlbumTitle': 'title',
            'AlbumReleaseYear': 'release_year',
            'RecordReleaseYear': 'record_release_year',
            'RecordLabelName': 'label_name',
            'RecordLabelAddress': 'address',
            'SerialNumber': 'serial_no',
            'PriceUSD': 'price',
            'CountryOfOrigin': 'country',
            'StockQuantity': 'quantity'
        }
        self.relevant_columns = [
            'first_name', 'last_name', 'band_name', 'profile', 'style', 'audio_format',
            'album_format', 'title', 'release_year', 'record_release_year', 'label_name',
            'address', 'serial_no', 'price', 'country', 'quantity'
        ]

    @staticmethod
    def filter_data(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Filter the data to include only relevant records.

        This method filters the DataFrame to include only records where the audio format is vinyl.
        It also normalizes the 'audio_format' column to lower case and allows only 'mono' or 'stereo'
        values, setting other values to None.

        :param dataframe: Input DataFrame with data to be filtered.
        :return: Filtered DataFrame with only relevant records.
        """

        dataframe['audio_format'] = dataframe['audio_format'].str.lower()
        dataframe = dataframe[dataframe['audio_format'] == 'vinyl']
        dataframe.loc[:, 'audio_format'] = dataframe['audio_format'].apply(
            lambda x: x if x in ['mono', 'stereo'] else None)
        return dataframe

    def split_name(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Split ArtistName into first_name, last_name, and band_name based on profile description.

        Uses a configuration file to determine whether the artist is a soloist or a band.
        If the artist type is unknown, adds them to a list of missing artists.

        :param dataframe: Input DataFrame with the 'ArtistName' column.
        :return: DataFrame with new columns 'first_name', 'last_name', and 'band_name'.
        """

        config_df = pd.read_csv(self.config_file)
        artist_type_dict = config_df.set_index('ArtistName')['Type'].to_dict()

        missing_artists = []

        def split_name(row):
            artist_name = row['ArtistName']
            artist_type = artist_type_dict.get(artist_name, 'unknown')

            if artist_type == 'solist':
                names = artist_name.split()
                row['first_name'] = names[0]
                row['last_name'] = ' '.join(names[1:]) if len(names) > 1 else ''
                row['band_name'] = ''
            elif artist_type == 'band':
                row['first_name'] = ''
                row['last_name'] = ''
                row['band_name'] = artist_name
            else:
                missing_artists.append(artist_name)
                row['first_name'] = ''
                row['last_name'] = ''
                row['band_name'] = ''

            return row

        dataframe = dataframe.apply(split_name, axis=1)

        if missing_artists:
            print(f"Missing artist information for: {', '.join(missing_artists)}. Please update the config file.")

        return dataframe

    def get_csv_files(self) -> list:
        """
        Get a list of CSV files from the input folder.

        :return: List of CSV file paths in the input folder.
        """
        return [f"{self.input_file}/{file}" for file in os.listdir(f"{self.input_file}") if file.endswith('.csv')]

    def read_csv_files(self) -> Optional[pd.DataFrame]:
        """
        Read and combine all CSV files from the input folder.

        If no valid CSV files are found, returns "skip".
        Otherwise, it combines all DataFrames into one.

        :return: Combined DataFrame or "skip" if no files are found.
        """

        dfs = [pd.read_csv(file) for file in self.get_csv_files()]
        if not dfs:
            return None
        else:
            return functools.reduce(lambda x, y: pd.concat([x, y]), dfs)

    def keep_relevant_columns(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Keep only relevant columns in the DataFrame.

        :param dataframe: Input DataFrame with various columns.
        :return: DataFrame containing only the relevant columns.
        """
        dataframe = dataframe[self.relevant_columns]
        return dataframe

    @staticmethod
    def ensure_data_types(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure that the data types of specific columns are correct.

        This method converts certain columns to appropriate data types, such as integer or float.

        :param dataframe: Input DataFrame with data to be type-cast.
        :return: DataFrame with corrected data types.
        """
        dataframe['release_year'] = dataframe['release_year'].astype('Int64')
        dataframe['record_release_year'] = dataframe['record_release_year'].astype('Int64')
        dataframe['quantity'] = dataframe['quantity'].astype('Int64')
        dataframe['price'] = dataframe['price'].replace('[\sUSD]', '', regex=True).astype(float)
        return dataframe

    @staticmethod
    def handle_null_values(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the DataFrame.

        Sets the 'price' column to the average price and 'quantity' to 0 where values are missing.

        :param dataframe: Input DataFrame with potential null values.
        :return: DataFrame with handled null values.
        """

        average_price = dataframe['price'].mean()
        dataframe = dataframe.fillna({'price': average_price,
                                      'quantity': 0})
        return dataframe

    def transform_data(self):
        """
        Perform the complete data transformation process.

        This includes reading CSV files, renaming columns, filtering data, splitting artist names,
        retaining relevant columns, ensuring correct data types, and handling null values. Transformed
        data is saved to the specified output folder, and input files are moved to the archive folder.
        """
        df = self.read_csv_files()
        if df is None:
            print("No data to transform. Skipping")
            return
        df = rename_columns(df, self.mapping)
        df = self.filter_data(df)
        df = self.split_name(df)
        df = self.keep_relevant_columns(df)
        df = self.ensure_data_types(df)
        df = self.handle_null_values(df)
        df = df.drop_duplicates()
        tmstp = int(datetime.datetime.now().timestamp())
        df.to_csv(self.output_file + f'/data_{tmstp}.csv', index=False)
        for file in self.get_csv_files():
            shutil.move(file, self.archive_folder)
        print(f'Data transformation has been completed.')
