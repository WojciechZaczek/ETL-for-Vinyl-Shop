import os
import shutil


class Extract:
    """Class responsible for extracting data to the inbox folder"""

    def __init__(self):
        self.inbox_folder = os.path.join(os.path.dirname(__file__), 'inbox')
        os.makedirs(self.inbox_folder, exist_ok=True)

    def extract_file_to_inbox(self, source_file, filename):
        destination_path = os.path.join(self.inbox_folder, filename)
        shutil.copy(source_file, destination_path)


if __name__ == "__main__":
    source_file = r'C:\Users\wojci\Downloads\vinyl_database_150.csv'
    extractor = Extract()
    extractor.extract_file_to_inbox(source_file, 'vinyl_database_150.csv')


