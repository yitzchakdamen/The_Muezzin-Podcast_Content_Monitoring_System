from pathlib import Path
from datetime import datetime
from typing import Generator
from utils.decorators import log_func, safe_execute
import logging
from config.config import LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)

class FileMetadataProcessing:
    
    def __init__(self, folder_files_path:str) -> None:
        self.folder = Path(folder_files_path)

    @log_func
    def get_files_information_in_folder(self) -> Generator[dict,None, None]:
        """ Generator for getting information about files within a folder """
        for file in self.folder.iterdir(): 
            if file.is_file():
                yield self.extracting_all_file_metadata(file)
    
    @log_func
    def extracting_all_file_metadata(self, file: Path) -> dict:
        """ getting all file information """
        metadata: dict = self.file_metadata(file=file)
        metadata["statistics"] = self.file_statistics(file=file)
        return metadata
    
    def file_metadata(self, file: Path) -> dict:
        """ extracting file metadata  """
        if not file.is_file(): raise TypeError("The Path is not a file")
        return {
            "name": file.name,
            "relative_path": str(file.parent),
            "absolute_path":file.resolve(),
            "stem": file.stem, 
            "suffix": file.suffix
        }
    
    def file_statistics(self, file: Path) -> dict:
        """ extracting file statistics  """
        if not file.is_file(): raise TypeError("The Path is not a file")
        stats = file.stat()
        return {
            "size": stats.st_size,
            "last_modification_time" : str(datetime.fromtimestamp(stats.st_mtime)),
            "creation_time": str(datetime.fromtimestamp(stats.st_birthtime if hasattr(stats, "st_birthtime") else stats.st_mtime)),
            "last_access_time": str(datetime.fromtimestamp(stats.st_atime))
        }