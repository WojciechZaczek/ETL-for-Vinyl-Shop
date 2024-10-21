from functionality.connections import create_conn_engine_trust
from functionality.mapping import column_table_mapping
import os
import pandas as pd
import shutil


class Load:
    """
    Class responsible for loading transformed data into the database.
    """
    def __init__(self, transformed_folder: str, archive_folder: str) -> None:
        """
        Initialize the Load class with the folder paths for transformed data and archived files.

        :param transformed_folder: Path to the folder containing transformed CSV files.
        :param archive_folder: Path to the folder where processed files will be archived.
        """
        self.transformed_folder = transformed_folder
        self.archive_folder = archive_folder
        self.engine = create_conn_engine_trust(dbname='VinylStore')

    def load_data(self) -> None:
        """
        Load data from CSV files into the database and archive them.

        This method reads all CSV files in the transformed folder, loads their data into the database,
        and then moves the processed files to the archive folder. If there are no CSV files to process,
        the method ends without doing anything.
        """
        csv_files = [file for file in os.listdir(self.transformed_folder) if file.endswith('.csv')]

        if not csv_files:
            print("No data to load. Process has been completed.")
            return

        for file in csv_files:
            file_path = os.path.join(self.transformed_folder, file)

            if not os.path.exists(file_path) or not os.path.getsize(file_path):
                print(f"File {file} do not exist.")
                continue

            df = pd.read_csv(file_path)
            for table_name, columns in column_table_mapping.items():
                table_columns = [col for col in df.columns if col in columns]
                if not table_columns:
                    continue
                table_data = df[table_columns]
                table_data.to_sql(table_name, con=self.engine, if_exists='append', index=False)

            archive_path = os.path.join(self.archive_folder, os.path.basename(self.transformed_folder))
            shutil.move(file_path, archive_path)
            print("Data has been loaded to database.")
