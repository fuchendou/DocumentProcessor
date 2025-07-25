from utils.mineru_utils import MinerUProcessor

if __name__ == "__main__":
    # 初始化处理器（自动从.env加载配置）
    try:
        processor = MinerUProcessor()
        print("MinerU处理器初始化成功!")
        
        # 示例：上传本地文件
        batch_id = processor.upload_files(
            file_paths=["data/pdf/test/testpdf01.pdf"],
            is_ocr_list=[True],
            data_ids=["test03"],
            language="ch"
        )
        
        # 获取结果（使用自定义轮询参数）
        results = processor.get_batch_results(batch_id, max_retries=30, interval=30)
        
        # 处理结果
        for res in results:
            if res['state'] == 'done':
                print(f"✅ 文件 {res['file_name']} 解析成功!")
                print(f"   下载链接: {res['full_zip_url']}")
                # 下载结果
                processor.download_result(res['full_zip_url'], f"data/results/{res['file_name']}.zip")
            elif res['state'] == 'failed':
                print(f"❌ 文件 {res['file_name']} 解析失败: {res.get('err_msg', '未知错误')}")
            else:
                print(f"⚠️ 文件 {res['file_name']} 状态异常: {res['state']}")
    
    except Exception as e:
        print(f"处理失败: {str(e)}")