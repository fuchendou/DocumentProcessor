import os
import requests
import time
from typing import List, Dict, Optional
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class MinerUProcessor:
    def __init__(self, base_url: str = None):
        """
        初始化MinerU处理类，从环境变量读取配置
        """
        # 从环境变量获取配置，优先使用参数值
        self.api_token = os.getenv("MINERU_API_TOKEN")
        if not self.api_token:
            raise ValueError("未找到MINERU_API_TOKEN环境变量，请在.env文件中配置")
        
        self.base_url = base_url or os.getenv("MINERU_BASE_URL", "https://mineru.net/api/v4")
        self.base_url = self.base_url.rstrip('/')
        
        # 轮询配置（可从环境变量覆盖）
        self.max_retries = int(os.getenv("MINERU_MAX_RETRIES", 30))
        self.retry_interval = int(os.getenv("MINERU_RETRY_INTERVAL", 10))
        
        self.headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }

    def _handle_response(self, response: requests.Response) -> Dict:
        """统一处理API响应"""
        if response.status_code != 200:
            raise Exception(f"API请求失败，状态码: {response.status_code}, 响应: {response.text}")
        
        result = response.json()
        if result.get('code') != 0:
            error_msg = result.get('msg', '未知错误')
            trace_id = result.get('trace_id', '')
            error_code = result.get('code', '')
            raise Exception(f"业务错误 [{error_code}]: {error_msg}, trace_id: {trace_id}")
        return result['data']

    def upload_files(
        self,
        file_paths: List[str],
        is_ocr_list: Optional[List[bool]] = None,
        data_ids: Optional[List[str]] = None,
        enable_formula: bool = True,
        enable_table: bool = True,
        language: str = "ch",
        page_ranges: Optional[List[str]] = None,
        callback: Optional[str] = None,
        seed: Optional[str] = None,
        extra_formats: Optional[List[str]] = None,
        model_version: str = "v2"
    ) -> str:
        """
        批量上传文件并提交解析任务
        :param file_paths: 本地文件路径列表
        :param is_ocr_list: 是否启用OCR的布尔值列表（与文件一一对应）
        :param data_ids: 自定义数据ID列表（与文件一一对应）
        :return: 批量任务ID (batch_id)
        """
        # 验证文件数量
        if len(file_paths) > 200:
            raise ValueError("单次上传文件数量不能超过200个")
        
        # 构建files参数
        files_data = []
        for i, path in enumerate(file_paths):
            file_info = {
                "name": os.path.basename(path),
                "is_ocr": is_ocr_list[i] if is_ocr_list and i < len(is_ocr_list) else False
            }
            
            # 添加可选参数
            if data_ids and i < len(data_ids):
                file_info["data_id"] = data_ids[i]
            if page_ranges and i < len(page_ranges):
                file_info["page_ranges"] = page_ranges[i]
            
            files_data.append(file_info)
        
        # 构建请求体
        payload = {
            "enable_formula": enable_formula,
            "enable_table": enable_table,
            "language": language,
            "files": files_data,
            "model_version": model_version
        }
        
        # 添加可选全局参数
        if callback:
            payload["callback"] = callback
        if seed:
            payload["seed"] = seed
        if extra_formats:
            payload["extra_formats"] = extra_formats
        
        # 请求上传URL
        url = f"{self.base_url}/file-urls/batch"
        response = requests.post(url, headers=self.headers, json=payload)
        data = self._handle_response(response)
        
        # 上传文件到OSS
        batch_id = data["batch_id"]
        upload_urls = data["file_urls"]
        
        success_count = 0
        for i, (file_path, upload_url) in enumerate(zip(file_paths, upload_urls)):
            try:
                with open(file_path, 'rb') as f:
                    upload_response = requests.put(upload_url, data=f)
                    if upload_response.status_code == 200:
                        success_count += 1
                    else:
                        print(f"警告: 文件 {file_path} 上传失败 (状态码: {upload_response.status_code})")
            except Exception as e:
                print(f"文件 {file_path} 上传异常: {str(e)}")
        
        print(f"批量上传完成! 成功上传 {success_count}/{len(file_paths)} 个文件, batch_id: {batch_id}")
        return batch_id

    def submit_urls(
        self,
        urls: List[str],
        is_ocr_list: Optional[List[bool]] = None,
        data_ids: Optional[List[str]] = None,
        enable_formula: bool = True,
        enable_table: bool = True,
        language: str = "ch",
        page_ranges: Optional[List[str]] = None,
        callback: Optional[str] = None,
        seed: Optional[str] = None,
        extra_formats: Optional[List[str]] = None,
        model_version: str = "v2"
    ) -> str:
        """
        通过URL批量提交解析任务
        :param urls: 文件URL列表
        :return: 批量任务ID (batch_id)
        """
        if len(urls) > 200:
            raise ValueError("单次提交URL数量不能超过200个")
        
        # 构建files参数
        files_data = []
        for i, url in enumerate(urls):
            file_info = {"url": url}
            
            if is_ocr_list and i < len(is_ocr_list):
                file_info["is_ocr"] = is_ocr_list[i]
            if data_ids and i < len(data_ids):
                file_info["data_id"] = data_ids[i]
            if page_ranges and i < len(page_ranges):
                file_info["page_ranges"] = page_ranges[i]
            
            files_data.append(file_info)
        
        # 构建请求体
        payload = {
            "enable_formula": enable_formula,
            "enable_table": enable_table,
            "language": language,
            "files": files_data,
            "model_version": model_version
        }
        
        if callback:
            payload["callback"] = callback
        if seed:
            payload["seed"] = seed
        if extra_formats:
            payload["extra_formats"] = extra_formats
        
        # 提交任务
        url = f"{self.base_url}/extract/task/batch"
        response = requests.post(url, headers=self.headers, json=payload)
        data = self._handle_response(response)
        
        batch_id = data["batch_id"]
        print(f"URL批量任务提交成功! batch_id: {batch_id}")
        return batch_id

    def get_batch_results(
        self,
        batch_id: str,
        max_retries: int = None,
        interval: int = None
    ) -> List[Dict]:
        """
        获取批量任务结果（支持轮询等待完成）
        :param batch_id: 批量任务ID
        :param max_retries: 最大轮询次数（默认使用类配置）
        :param interval: 轮询间隔(秒)（默认使用类配置）
        :return: 任务结果列表
        """
        url = f"{self.base_url}/extract-results/batch/{batch_id}"
        
        max_retries = max_retries or self.max_retries
        interval = interval or self.retry_interval
        
        for attempt in range(max_retries):
            response = requests.get(url, headers=self.headers)
            data = self._handle_response(response)
            
            # 检查所有任务是否完成
            all_done = True
            for task in data['extract_result']:
                if task['state'] not in ['done', 'failed']:
                    all_done = False
                    break
            
            if all_done:
                return data['extract_result']
            
            # 显示进度信息
            progress_info = []
            for task in data['extract_result']:
                if task['state'] == 'running' and 'extract_progress' in task:
                    progress = task['extract_progress']
                    progress_info.append(
                        f"{task['file_name']}: {progress['extracted_pages']}/{progress['total_pages']}页"
                    )
            
            status = ", ".join(progress_info) if progress_info else "等待中"
            print(f"[{attempt+1}/{max_retries}] 任务进行中: {status}... {interval}秒后重试")
            time.sleep(interval)
        
        raise TimeoutError(f"获取结果超时（{max_retries}次尝试），请稍后手动查询")

    def download_result(self, zip_url: str, save_path: str) -> None:
        """
        下载解析结果压缩包
        :param zip_url: 结果压缩包URL
        :param save_path: 本地保存路径
        """
        response = requests.get(zip_url, stream=True)
        if response.status_code != 200:
            raise Exception(f"下载失败: HTTP {response.status_code}")
        
        # 确保目录存在
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"$\r$下载进度: {progress:.1f}% ({downloaded}/{total_size}字节)", end='')
        
        print(f"$\n$结果已保存至: {save_path}")