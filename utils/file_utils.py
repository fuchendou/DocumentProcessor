import os

def validate_file_exists(file_path: str):
    """验证文件是否存在且可访问"""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    if not os.access(file_path, os.R_OK):
        raise PermissionError(f"Access denied to file: {file_path}")

def save_to_file(save_path: str, content: str, encoding: str = "utf-8"):
    """将字符串内容保存到指定文件"""
    with open(save_path, "w", encoding=encoding) as f:
        f.write(content)
