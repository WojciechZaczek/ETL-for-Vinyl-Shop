from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import os
import shutil

db = create_engine('mssql+pyodbc://@DESKTOP-8695HOJ/VinylStore?driver=ODBC+Driver+17+for+SQL+Server')
connection = db.connect()


class Load:
    def __init__(self, transformed_file, archive_folder):
        self.transformed_file = transformed_file
        self.archive_folder = archive_folder

    def load_data(self):
        try:
            df = pd.read_csv(self.transformed_file)

            df_artists = df[['first_name', 'last_name', 'band_name', 'profile']].drop_duplicates()
            df_artists.to_sql(name='artists', con=connection, if_exists='append', index=False)
            artist_ids = pd.read_sql("SELECT artist_id, first_name, last_name, band_name FROM artists", connection)

            df_album_info = df[['style', 'audio_format', 'album_format']].drop_duplicates()
            df_album_info.to_sql(name='album_information', con=connection, if_exists='append', index=False)
            album_info_ids = pd.read_sql(
                "SELECT album_information_id, style, audio_format, album_format FROM album_information", connection)

            df = df.merge(artist_ids, how='left', on=['first_name', 'last_name', 'band_name'])
            df = df.merge(album_info_ids, how='left', on=['style', 'audio_format', 'album_format'])

            df_albums = df[['title', 'release_year', 'artist_id', 'album_information_id']]
            df_albums.to_sql('albums', con=connection, if_exists='append', index=False)
            album_ids = pd.read_sql("SELECT album_id, title, release_year FROM albums", connection)
            df = df.merge(album_ids, how='left', on=['title', 'release_year'])

            df_labels = df[['label_name', 'address']].drop_duplicates()
            df_labels.to_sql('label', con=connection, if_exists='append', index=False)
            label_ids = pd.read_sql("SELECT label_id, label_name, address FROM label", connection)
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


if __name__ == "__main__":
    transformed_file = os.path.join('transformed', 'vinyl_database_transformed.csv')
    archive_folder = os.path.join('archive')

    os.makedirs(archive_folder, exist_ok=True)

    loader = Load(transformed_file, archive_folder)
    loader.load_data()
