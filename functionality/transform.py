import datetime
import pandas as pd
import os
import re
import shutil
import sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # add gcloud


timestamp = int(datetime.datetime.now().timestamp())

class Transform:
    """Class responsible for transform data from the inbox folder"""

    def __init__(self, input_file, output_file, archive_folder):
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

    def rename_columns(self, dataframe, column_mapping: dict):
        """
        Rename columns based on a provided mapping.
        :param dataframe: Input DataFrame
        :param column_mapping: Dictionary mapping of old column names to new column names
        :return: DataFrame with renamed columns
        """
        dataframe = dataframe.rename(columns=column_mapping)
        return dataframe

    def filter_data(self, dataframe):
        """
        Filter the data to focus on relevant records.
        :param dataframe: Input DataFrame
        :return: Filtered DataFrame
        Only vinyls, audio format: mono, stereo or null
        """

        dataframe['audio_format'] = dataframe['audio_format'].str.lower()
        dataframe = dataframe[dataframe['audio_format'] == 'vinyl']
        dataframe.loc[:, 'audio_format'] = dataframe['audio_format'].apply(
            lambda x: x if x in ['mono', 'stereo'] else None)
        return dataframe

    def split_name(self, dataframe):
        """
        Split ArtistName into first_name, last_name, and band_name based on profile description.
        :param dataframe: Input DataFrame
        :return: DataFrame with split artist names

        config csv
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

    def get_csv_files(self):
        return [f"{self.input_file}/{file}" for file in os.listdir(f"{self.input_file}") if file.endswith('.csv')]

    def read_csv_files(self):
        # dfs = [self.check_schema(file) for file in self.get_csv_files()]
        dfs = [pd.read_csv(file) for file in self.get_csv_files()]
        import functools
        return functools.reduce(lambda x, y: pd.concat([x, y]), dfs)


    def keep_relevant_columns(self, dataframe):
        """
        Keep only relevant columns.
        :param dataframe: Input DataFrame
        :return: DataFrame with only relevant columns
        """
        dataframe = dataframe[self.relevant_columns]
        return dataframe

    def ensure_data_types(self, dataframe):
        """
        Ensure data types are correct.
        :param dataframe: Input DataFrame
        :return: DataFrame with correct data types
        """
        dataframe['release_year'] = dataframe['release_year'].astype('Int64')
        dataframe['record_release_year'] = dataframe['record_release_year'].astype('Int64')
        dataframe['quantity'] = dataframe['quantity'].astype('Int64')
        dataframe['price'] = dataframe['price'].replace('[\sUSD]', '', regex=True).astype(float)
        return dataframe


    def handle_null_values(self, dataframe):
        """

        :param columns_to_modify:
        :return:
        """

        average_price = dataframe['price'].mean()
        dataframe = dataframe.fillna({'price': average_price,
                                      'quantity': 0})
        return dataframe


    def transform_data(self):
        df = self.read_csv_files()
        df = self.rename_columns(df, self.mapping)
        df = self.filter_data(df)
        df = self.split_name(df)
        df = self.keep_relevant_columns(df)
        df = self.ensure_data_types(df)
        df = self.handle_null_values(df)
        df = df.drop_duplicates()
        tmstp = int(datetime.datetime.now().timestamp())
        df.to_csv(self.output_file + f'/data_{tmstp}.csv', index=False)
        for file in self.get_csv_files():
            # filename = file.split('/')[-1]
            shutil.move(file, self.archive_folder)


# def get_csv_files(folder):
#     return [file for file in os.listdir(folder) if file.endswith('.csv')]


# def transform_csv_files(input_folder, output_folder, archive_folder):
#     csv_files = get_csv_files(input_folder)

#     if not csv_files:
#         print("Inbox folder is empty")
#     else:
#         for file in csv_files:
#             input_file = os.path.join(input_folder, file)
#             output_file = os.path.join(output_folder, f'vinyl_database_transformed_{timestamp}.csv')
#             transformer = Transform(input_file, output_file, archive_folder)
#             transformer.transform_data()
#             print(f'File {file} has been transformed and archived.')


# if __name__ == "__main__":
#     print(int(datetime.datetime.now().timestamp()))
#     input_folder = 'inbox'
#     output_folder = 'data'
#     archive_folder = os.path.join('../airflow/dags/inbox', 'archive')
#     transform_csv_files(input_folder, output_folder, archive_folder)





