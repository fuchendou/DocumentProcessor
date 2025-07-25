from abc import ABC, abstractmethod
import os
from exceptions import UnsupportedFormatError

class DocumentProcessor(ABC):
    SUPPORT_EXTENSIONS = ['csv', 'xlsx', 'xls']

    def __init__(self, file_path):
        self.file_path = file_path

    @abstractmethod
    def extract_text(self) -> str:
        """提取文档文本"""
        pass

    @abstractmethod
    def extract_metadata(self) -> dict:
        """提取文档元数据"""
        pass

    @classmethod
    def support_extension(cls, extension: str) -> bool:
        """判断是否支持指定扩展名"""
        return extension.lower() in cls.SUPPORT_EXTENSIONS

class DocumentProcessorFactory:
    _processors = {}

    @classmethod
    def register_processor(cls, processor_class):
        """注册文档处理器"""
        for ext in processor_class.SUPPORT_EXTENSIONS:
            cls._processors[ext] = processor_class

    @classmethod
    def get_processor(cls, file_path: str) -> DocumentProcessor:
        """根据文件路径获取文档处理器"""
        extension = os.path.splitext(file_path)[1][1:].lower()

        if not extension:
            raise UnsupportedFormatError(f"File has no extension: {file_path}")
        
        processor_class = cls._processors.get(extension)
        
        if not processor_class:
            raise UnsupportedFormatError(f"Unsupported file format: {extension}")

        return processor_class(file_path)