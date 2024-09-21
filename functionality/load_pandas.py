from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import os
import shutil

# db = create_engine('mssql+pyodbc://@DESKTOP-8695HOJ/VinylStore?driver=ODBC+Driver+17+for+SQL+Server')
# connection = db.connect()
connection = None


class Load:
    def __init__(self, transformed_file, archive_folder):
        self.transformed_file = transformed_file
        self.archive_folder = archive_folder

    @staticmethod
    def save_and_load_data(df, table_name, connection, select_query):
        df.to_sql(name=table_name, con=connection, if_exists='append', index=False)
        return pd.read_sql(select_query, connection)

    def load_data(self):
        try:
            df = pd.read_csv(self.transformed_file)

            artist_ids = self.save_and_load_data(
                df[['first_name', 'last_name', 'band_name', 'profile']],
                'artists',
                connection,
                "SELECT artist_id, first_name, last_name, band_name FROM artists"
            )

            album_info_ids = self.save_and_load_data(
                df[['style', 'audio_format', 'album_format']],
                'album_information',
                connection,
                "SELECT album_information_id, style, audio_format, album_format FROM album_information"
            )

            df = df.merge(artist_ids, how='left', on=['first_name', 'last_name', 'band_name'])
            df = df.merge(album_info_ids, how='left', on=['style', 'audio_format', 'album_format'])

            album_ids = self.save_and_load_data(
                df[['title', 'release_year', 'artist_id', 'album_information_id']],
                'albums',
                connection,
                "SELECT album_id, title, release_year FROM albums"
            )
            df = df.merge(album_ids, how='left', on=['title', 'release_year'])

            label_ids = self.save_and_load_data(
                df[['label_name', 'address']],
                'label',
                connection,
                "SELECT label_id, label_name, address FROM label"
            )
            df = df.merge(label_ids, how='left', on=['label_name', 'address'])

            df_records = df[
                ['serial_no', 'price', 'record_release_year', 'label_id', 'country', 'quantity', 'album_id']]
            df_records.to_sql('records', con=connection, if_exists='append', index=False)

            shutil.move(self.transformed_file,
                        os.path.join(self.archive_folder, os.path.basename(self.transformed_file)))
            print(f"File {self.transformed_file} has been moved to {self.archive_folder}")

            shutil.move(self.transformed_file, os.path.join(self.archive_folder, os.path.basename(self.transformed_file)))
            print(f"File {self.transformed_file} has been moved to {self.archive_folder}")

        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
            connection.rollback()
        finally:
            connection.close()


# if __name__ == "__main__":
#     transformed_folder = 'data'
#     archive_folder = os.path.join('../airflow/dags/data', 'archive')
#     for file in os.listdir(transformed_folder):
#         if file.endswith('.csv'):
#             transformed_file = os.path.join('../airflow/dags/data', file)
#             loader = Load(transformed_file, archive_folder)
#             loader.load_data()
#             print(f'File {file} has been loaded to database and archive.')
