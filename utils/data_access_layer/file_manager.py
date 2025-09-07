from utils.decorators import safe_execute, log_func
import csv

class FileManager:
    
    @staticmethod
    @safe_execute()
    def uploading_content(file_url: str):
        if file_url.endswith('.csv'):
            return FileManager.uploading_csv_file_as_dict(file_url)
        elif file_url.endswith('.wav'):
            return FileManager.uploading_binary_file(file_url)
        elif file_url.endswith('.txt'):
            return FileManager.uploading_txt_file(file_url)
        else:
            raise ValueError("Unsupported file type")

    @staticmethod
    def uploading_txt_file(file_url: str) -> str:
        with open(file=file_url, mode='r', encoding='utf-8') as file:
            return file.read()
        
    @staticmethod
    def uploading_binary_file(file_url: str) :
        with open(file=file_url, mode='rb') as file:
            return file.read()

    @staticmethod
    def uploading_csv_file_as_dict(file_url: str) -> list[dict]:
        with open(file=file_url, mode='r', encoding='utf-8') as file:
            return list(csv.DictReader(file))
