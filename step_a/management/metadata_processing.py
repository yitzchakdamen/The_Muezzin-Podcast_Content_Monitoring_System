from pathlib import Path
from datetime import datetime


class FileMetadataProcessing:
    
    def __init__(self, folder_files_path) -> None:
        self.folder = Path(folder_files_path)

    def processing_files_information(self) -> list[dict]:
        return [self.extracting_file_metadata(file) for file in self.folder.iterdir() if file.is_file()]
    
    def extracting_file_metadata(self, file: Path) ->dict:
        metadata: dict = self.file_metadata(file=file)
        metadata["statistics"] = self.file_statistics(file=file)
        return metadata
        
    def file_metadata(self, file: Path) -> dict:
        if not file.is_file(): raise TypeError("The Path is not a file")
        return {
            "name": file.name,
            "stem": file.stem,
            "suffix": file.suffix,
            "parent directory": str(file.parent)
        }
        
    def file_statistics(self, file: Path) -> dict:
        if not file.is_file(): raise TypeError("The Path is not a file")
        stats = file.stat()
        return {
            "size": stats.st_size,
            "last modification time" : str(datetime.fromtimestamp(stats.st_mtime)),
            "last metadata change time": str(datetime.fromtimestamp(stats.st_birthtime)),
            "last access time": str(datetime.fromtimestamp(stats.st_atime))
        }