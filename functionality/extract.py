import datetime
import os
import shutil
from typing import  Optional


timestamp = int(datetime.datetime.now().timestamp())


class Extract:
    """
    Class responsible for extracting data from the source folder to the inbox folder.
    It also archives the extracted files.
    """

    def __init__(self, inbox_folder: Optional[str] = None, source_folder: Optional[str] = None,
                 archive_folder: Optional[str] = None):
        """
        Initialize the Extract class with folder paths for the inbox, source, and archive folders.

        :param inbox_folder: Path to the inbox folder where extracted files will be copied. Default is "/inbox".
        :param source_folder: Path to the source folder where the original CSV files are located. Default is "/source".
        :param archive_folder: Path to the archive folder where the processed files will be moved. Default is "/source/archive".
        """
        self.inbox_folder = inbox_folder if inbox_folder else "/inbox"
        self.source_folder = source_folder if source_folder else "/source"
        self.archive_folder = archive_folder if archive_folder else "/source/archive"

    def get_csv_files(self) -> list:
        """
        Get a list of all CSV files in the source folder.

        :return: A list of CSV file names in the source folder.
        """
        return [file for file in os.listdir(f"{self.source_folder}") if file.endswith('.csv')]

    def extract_files_to_inbox(self) -> None:
        """
        Extract CSV files from the source folder to the inbox folder.

        This method copies each CSV file from the source folder to the inbox folder and moves the original file
        to the archive folder with a timestamp added to the filename. If no CSV files are found, the process is skipped.
        """
        csv_files = self.get_csv_files()

        if not csv_files:
            print("Extract folder is empty. Skipping extract process")
            return None
        else:
            for file in csv_files:
                file_name, file_extension = os.path.splitext(file)
                new_file_name = f'{file_name}_{timestamp}{file_extension}'
                source_file = os.path.join(self.source_folder, file)
                output_file = os.path.join(self.inbox_folder, file)
                archive_file = os.path.join(self.archive_folder, new_file_name)
                shutil.copy(source_file, output_file)
                shutil.move(source_file, archive_file)
                print(f'File {file} has been extracted to inbox and archived.')
