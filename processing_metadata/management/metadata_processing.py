from pathlib import Path
from datetime import datetime
from typing import Generator


class FileMetadataProcessing:
    
    def __init__(self, folder_files_path:str) -> None:
        self.folder = Path(folder_files_path)

    def processing_files_information(self) -> Generator[dict]:
        return iter(self.extracting_file_metadata(file) for file in self.folder.iterdir() if file.is_file())
    
    def extracting_file_metadata(self, file: Path) ->dict:
        metadata: dict = self.file_metadata(file=file)
        metadata["statistics"] = self.file_statistics(file=file)
        return metadata
        
    def file_metadata(self, file: Path) -> dict:
        if not file.is_file(): raise TypeError("The Path is not a file")
        return {
            "name": file.name,
            "relative_path": str(file.parent),
            "absolute_path":file.resolve(),
            "metadata": { "stem": file.stem, "suffix": file.suffix}
        }
        
    def file_statistics(self, file: Path) -> dict:
        if not file.is_file(): raise TypeError("The Path is not a file")
        stats = file.stat()
        return {
            "size": stats.st_size,
            "last_modification_time" : str(datetime.fromtimestamp(stats.st_mtime)),
            "last_metadata_change_time": str(datetime.fromtimestamp(stats.st_birthtime)),
            "last_access_time": str(datetime.fromtimestamp(stats.st_atime))
        }