import pandas as pd
import os
import shutil
from functionality.connections import create_conn_engine
from functionality.mapping import column_table_mapping


#  to_sql -> data.to_sql('Winyle', con=engine, if_exists='append', index=False)
#  engine = create_conn_engine("localhost_mssql")

class Load:
    def __init__(self, transformed_file, archive_folder):
        self.transformed_file = transformed_file
        self.archive_folder = archive_folder
        self.engine = create_conn_engine("mssql_connection")

    def load_data(self):
        df = pd.read_csv(self.transformed_file)
        for table_name, columns in column_table_mapping.items():
            table_columns = [col for col in df.columns if col in columns]
            if not table_columns:
                continue
            table_data = df[table_columns]
            table_data.to_sql(table_name, con=self.engine, if_exists='append', index=False)

        archive_path = os.path.join(self.archive_folder, os.path.basename(self.transformed_file))
        shutil.move(f'{self.transformed_file}', archive_path)
        print("Data have been loaded to database ")


def get_csv_files(folder):
    return [file for file in os.listdir(folder) if file.endswith('.csv')]


def load_csv_files(input_folder, archive_folder):
    csv_files = get_csv_files(input_folder)
    if not csv_files:
        print("Data folder is empty")
    else:
        for file in csv_files:
            transformed_file = os.path.join(input_folder, file)
            loader = Load(transformed_file, archive_folder)
            loader.load_data()
            print(f'File {file} has been loaded to database and archived.')


if __name__ == "__main__":
    transformed_folder = 'data'
    archive_folder = os.path.join('../airflow/dags/data', 'archive')
    load_csv_files(transformed_folder, archive_folder)
