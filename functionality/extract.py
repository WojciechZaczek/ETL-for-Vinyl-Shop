import os
import shutil
import datetime

timestamp = int(datetime.datetime.now().timestamp())

class Extract:
    """Class responsible for extracting data to the inbox folder"""

    def __init__(self, inbox_folder=None, source_folder=None, archive_folder=None):
        self.inbox_folder = inbox_folder if inbox_folder else "/inbox"
        self.source_folder = source_folder if source_folder else "/source"
        self.archive_folder = archive_folder if archive_folder else "/source/archive"

    def get_csv_files(self):
        return [file for file in os.listdir(f"{os.getcwd()}/dags{self.source_folder}") if file.endswith('.csv')]

    def extract_files_to_inbox(self):
        csv_files = self.get_csv_files()

        if not csv_files:
            print("Extract folder is empty")
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


# if __name__ == "__main__":
#     source_folder = r'C:\Users\wojci\PycharmProjects\Vinyl_shop\source'
#     archive_folder = os.path.join(source_folder, 'archive')
#     inbox_folder = 'inbox'
#     extractor = Extract(inbox_folder, source_folder, archive_folder)
#     extractor.extract_files_to_inbox()


