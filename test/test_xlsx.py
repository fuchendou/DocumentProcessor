import os
import sys
# 将项目根目录添加到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from processors.xlsx_processor import XLSXProcessor
from utils.file_utils import save_to_file

def simple_test_extract_xlsx_text():
    test_files = [
        ("data/xlsx/test/customer_orders_copy.xlsx", "data/xlsx/result/customer_orders_copy.txt"),
    ]

    for input_path, output_path in test_files:
        processor = XLSXProcessor(file_path=input_path, unique_key="OrderID", max_len=350)
        result = processor.extract_text()
        save_to_file(output_path, result, encoding="utf-8")
        print(f"✅ 已处理并保存：{input_path} → {output_path}")

# 运行测试
if __name__ == "__main__":
    simple_test_extract_xlsx_text()
