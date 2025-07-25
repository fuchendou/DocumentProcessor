from processors.csv_processor import CSVProcessor
from utils.file_utils import save_to_file

def simple_test_extract_csv_text():
    test_files = [
        ("data/csv/test/customer_orders_01.csv", "data/csv/result/customer_orders_01.txt"),
        ("data/csv/test/customer_orders_02.csv", "data/csv/result/customer_orders_02.txt"),
        ("data/csv/test/customer_orders_03.csv", "data/csv/result/customer_orders_03.txt"),
    ]

    for input_path, output_path in test_files:
        processor = CSVProcessor(file_path=input_path, unique_key="OrderID", max_len=350)
        result = processor.extract_text()
        save_to_file(output_path, result, encoding="utf-8")
        print(f"✅ 已处理并保存：{input_path} → {output_path}")

# 运行测试
if __name__ == "__main__":
    simple_test_extract_csv_text()
