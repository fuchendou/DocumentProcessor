import os
import csv
from typing import List
from core import DocumentProcessor
from exceptions import FileCorruptionError
from utils.file_utils import validate_file_exists

class CSVProcessor(DocumentProcessor):
    """处理电子表格格式(CSV)的处理器"""
    SUPPORTED_EXTENSIONS = ['csv']
    
    def __init__(self, file_path: str, unique_key: str, max_len: int):
        super().__init__(file_path)
        validate_file_exists(file_path)
        self.file_extension = os.path.splitext(file_path)[1][1:].lower()
        self.unique_key = unique_key
        self.max_len = max_len
    
    def extract_text(self) -> str:
        """提取所有工作表的文本内容，用换页符分隔"""
        if self.file_extension == 'csv':
            return self._extract_csv_text()
    
    def extract_metadata(self) -> dict:
        """提取电子表格元数据"""
        meta = {
            'file_type': self.file_extension,
            'sheet_count': 0,
            'sheet_names': [],
            'has_formulas': False
        }
        
        if self.file_extension == 'csv':
            meta['sheet_count'] = 1
            meta['sheet_names'] = ['Sheet1']
            return meta
    
    def _extract_csv_text(self) -> str:
        try:
            MAX_LEN = self.max_len
            UNIQUE_KEY = self.unique_key
            # 减少预留空间，增加可用长度
            OVERHEAD_PADDING = 3
            content: List[str] = []
            
            with open(self.file_path, 'r', encoding='gbk', errors='ignore') as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    raise ValueError("CSV文件为空或没有表头")

                # 查找唯一标识列索引
                try:
                    unique_index = header.index(UNIQUE_KEY)
                except ValueError:
                    unique_index = 0
                    
                for i, row in enumerate(reader):
                    # 确保行长度与表头一致
                    if len(row) < len(header):
                        row += [''] * (len(header) - len(row))
                    elif len(row) > len(header):
                        row = row[:len(header)]

                    # 获取唯一标识
                    unique_id = row[unique_index] if unique_index < len(row) else f"ROW{i+1}"
                    unique_prefix = f"[ID:{unique_id}] "
                    
                    # 当前行内容块
                    chunks = []
                    current_chunk = unique_prefix
                    current_chunk_length = len(unique_prefix)
                    
                    for field_idx, (field, value) in enumerate(zip(header, row)):
                        # 清理特殊字符
                        value = self._sanitize_value(value)
                        
                        # 检查字段长度
                        field_header = f"{field}: "
                        field_header_length = len(field_header)
                        
                        # 检查单个字段是否超长
                        if len(value) > MAX_LEN - len(unique_prefix) - field_header_length - OVERHEAD_PADDING:
                            segments = self._segment_long_field(
                                unique_prefix, field, value, 
                                MAX_LEN, field_header_length, OVERHEAD_PADDING
                            )
                            chunks.extend(segments)
                            continue
                        
                        # 构建字段内容项
                        item = f"{field}: {value}"
                        item_length = len(item)
                        
                        # 检查添加后是否超过阈值
                        if current_chunk_length + item_length + 2 > MAX_LEN:  # +2 为分隔符预留
                            # 当前块接近满，完成当前块
                            chunks.append(current_chunk.rstrip('; '))
                            
                            # 开始新块，包含唯一标识
                            current_chunk = unique_prefix + item + '; '
                            current_chunk_length = len(unique_prefix) + item_length + 2
                        else:
                            # 添加到当前块
                            current_chunk += item + '; '
                            current_chunk_length += item_length + 2
                    
                    # 添加最后一个块
                    if current_chunk_length > len(unique_prefix):
                        chunks.append(current_chunk.rstrip('; '))
                    
                    # 添加该行所有块到内容
                    content.extend(chunks)
            
            return '\n'.join(content)

        except Exception as e:
            raise FileCorruptionError(f"Error reading CSV file: {e}") from e

    def _segment_long_field(self, unique_prefix: str, field: str, value: str, 
                       max_len: int, field_header_length: int, padding: int) -> List[str]:
        """分段处理超长字段值"""
        segments = []
        segment_idx = 1
        max_segment_length = max_len - len(unique_prefix) - field_header_length - len(f" [Part{segment_idx}]") - padding
        
        start = 0
        while start < len(value):
            # 计算当前分段允许的最大长度
            segment_header = f"{field} [Part{segment_idx}]: "
            max_segment_value_length = max_len - len(unique_prefix) - len(segment_header)
            
            # 确保不会出现负值
            if max_segment_value_length <= 0:
                max_segment_value_length = 1
            
            # 获取分段内容
            end = start + max_segment_value_length
            segment_value = value[start:end]
            
            # 创建分段项
            segment_item = unique_prefix + segment_header + segment_value
            segments.append(segment_item)
            
            # 更新索引
            start = end
            segment_idx += 1
        
        return segments

    def _sanitize_value(self, value: str) -> str:
        """清理值中的特殊字符"""
        # 替换可能干扰的分隔符
        replacements = {
            ';': '；',  # 全角分号
            '\n': '↵',  # 换行符替换
            '\r': '',    # 移除回车符
            '\t': '    ' # 制表符替换为空格
        }
        for orig, repl in replacements.items():
            value = value.replace(orig, repl)
        return value
