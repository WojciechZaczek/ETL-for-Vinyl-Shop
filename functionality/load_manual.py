import os.path
import os
import shutil
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError


db = create_engine('mssql+pyodbc://@DESKTOP-8695HOJ/VinylStore?driver=ODBC+Driver+17+for+SQL+Server')
Session = sessionmaker(bind=db)
Base = declarative_base()


class Artist(Base):
    __tablename__ = 'artists'
    artist_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(50))
    last_name = Column(String(50))
    band_name = Column(String(50))
    profile = Column(String(255))


class Album(Base):
    __tablename__ = 'albums'
    album_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(100), nullable=False)
    release_year = Column(Integer, nullable=False)
    artist_id = Column(Integer, ForeignKey('artists.artist_id'), nullable=False)
    album_information_id = Column(Integer, ForeignKey('album_information.album_information_id'), nullable=False)


class AlbumInformation(Base):
    __tablename__ = 'album_information'
    album_information_id = Column(Integer, primary_key=True, autoincrement=True)
    style = Column(String(50), nullable=False)
    audio_format = Column(String(50), nullable=False)
    album_format = Column(String(50), nullable=False)


class Label(Base):
    __tablename__ = 'label'
    label_id = Column(Integer, primary_key=True, autoincrement=True)
    label_name = Column(String(50), nullable=False)
    address = Column(String(250), nullable=True)


class Record(Base):
    __tablename__ = 'records'
    record_id = Column(Integer, primary_key=True, autoincrement=True)
    serial_no = Column(String(50), nullable=False)
    price = Column(Float, nullable=False)
    record_release_year = Column(Integer, nullable=False)
    country = Column(String(50), nullable=False)
    quantity = Column(Integer, nullable=False)
    label_id = Column(Integer, ForeignKey('label.label_id'), nullable=False)
    album_id = Column(Integer, ForeignKey('albums.album_id'), nullable=False)


class Load:
    def __init__(self, transformed_file, archive_folder):
        self.transformed_file = transformed_file
        self.session = Session()
        self.archive_folder = archive_folder

    def load_data(self):
        try:
            df = pd.read_csv(self.transformed_file)

            artists = []
            albums = []
            records = []

            for _, row in df.iterrows():
                artist = Artist(
                    first_name=row['first_name'],
                    last_name=row['last_name'],
                    band_name=row['band_name'],
                    profile=row['profile']
                )
                self.session.add(artist)
                self.session.flush()
                artists.append(artist)

                album_information = AlbumInformation(
                    style=row['style'],
                    audio_format=row['audio_format'],
                    album_format=row['album_format'])

                self.session.add(album_information)
                self.session.flush()
                albums.append(album_information)

                album = Album(
                    title=row['title'],
                    release_year=row['release_year'],
                    artist_id=artist.artist_id,
                    album_information_id=album_information.album_information_id
                )
                self.session.add(album)
                self.session.flush()
                albums.append(album)

                label = Label(
                    label_name=row['label_name'],
                    address=row['address']
                )
                self.session.add(label)
                self.session.flush()
                albums.append(label)

                record = Record(
                    serial_no=row['serial_no'],
                    price=row['price'],
                    record_release_year=row['record_release_year'],
                    label_id=label.lable_id,
                    country=row['country'],
                    quantity=row['quantity'],
                    album_id=album.album_id
                )
                records.append(record)

            self.session.bulk_save_objects(artists)
            self.session.bulk_save_objects(albums)
            self.session.bulk_save_objects(records)
            self.session.commit()

            shutil.move(self.transformed_file, os.path.join(self.archive_folder, os.path.basename(self.transformed_file)))
            print(f"File {self.transformed_file} has been moved to {self.archive_folder}")

        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
            self.session.rollback()
        finally:
            self.session.close()


# if __name__ == "__main__":
#     transformed_folder = 'data'
#     archive_folder = os.path.join('../airflow/dags/data', 'archive')
#     for file in os.listdir(transformed_folder):
#         if file.endswith('.csv'):
#             transformed_file = os.path.join('../airflow/dags/data', file)
#             loader = Load(transformed_file, archive_folder)
#             loader.load_data()
#             print(f'File {file} has been loaded to database and archive.')
