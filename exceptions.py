class DocumentProcessingError(Exception):
    """文档处理基础异常"""
    pass

class UnsupportedFormatError(DocumentProcessingError):
    """不支持的格式异常"""
    pass

class FileCorruptionError(DocumentProcessingError):
    """文件损坏异常"""
    pass

class PasswordProtectedError(DocumentProcessingError):
    """密码保护文档异常"""
    pass