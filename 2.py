import time
import requests
import bs4
import re
import os
import random
import json
import urllib3
import threading
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import OrderedDict
from fake_useragent import UserAgent
from typing import Optional, Dict
from ebooklib import epub
import base64
import gzip
from urllib.parse import urlencode

# 禁用SSL证书验证警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()

# 全局配置
CONFIG = {
    "max_workers": 4,
    "max_retries": 3,
    "request_timeout": 15,
    "status_file": "chapter.json",
    "request_rate_limit": 0.4,
    "auth_token": "wcnmd91jb",
    "server_url": "https://dlbkltos.s7123.xyz:5080",
    "api_endpoints": [],
    "batch_config": {
        "name": "qyuing",
        "base_url": None,
        "batch_endpoint": None,
        "token": None,
        "max_batch_size": 290,
        "timeout": 10,
        "enabled": True
    }
}

# 全局锁
print_lock = threading.Lock()

def make_request(url, headers=None, params=None, data=None, method='GET', verify=False, timeout=None):
    """通用的请求函数"""
    if headers is None:
        headers = get_headers()
    
    try:
        request_params = {
            'headers': headers,
            'params': params,
            'verify': verify,
            'timeout': timeout if timeout is not None else CONFIG["request_timeout"]
        }
        
        if data:
            request_params['json'] = data

        session = requests.Session()
        if method.upper() == 'GET':
            response = session.get(url, **request_params)
        elif method.upper() == 'POST':
            response = session.post(url, **request_params)
        else:
            raise ValueError(f"不支持的HTTP方法: {method}")
        
        return response
    except Exception as e:
        with print_lock:
            print(f"请求失败: {str(e)}")
        raise

def get_headers() -> Dict[str, str]:
    """生成随机请求头"""
    browsers = ['chrome', 'edge']
    browser = random.choice(browsers)
    
    if browser == 'chrome':
        user_agent = UserAgent().chrome
    else:
        user_agent = UserAgent().edge
    
    return {
        "User-Agent": user_agent,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://fanqienovel.com/",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/json"
    }

def fetch_api_endpoints_from_server():
    """从服务器获取API列表"""
    try:
        headers = get_headers()
        headers["X-Auth-Token"] = CONFIG["auth_token"]
        
        # 获取人机验证url
        challenge_url = f"{CONFIG['server_url']}/api/get-captcha-challenge"
        challenge_res = make_request(
            challenge_url,
            headers=headers,
            timeout=10,
            verify=False
        )
        
        if challenge_res.status_code != 200:
            with print_lock:
                print(f"获取人机验证挑战失败: {challenge_res.status_code}")
            return False
            
        challenge_data = challenge_res.json()
        captcha_url = challenge_data["challenge_url"]
        
        with print_lock:
            print("\n" + "="*50)
            print("需要进行人机验证才能继续")
            print("请访问以下链接完成验证:")
            print(captcha_url)
            print("="*50 + "\n")
            
            verification_token = input("请粘贴验证后获取的令牌: ").strip()
        
        # 使用令牌获取api
        headers["X-Verification-Token"] = verification_token
        
        sources_url = f"{CONFIG['server_url']}/api/sources"
        response = make_request(
            sources_url,
            headers=headers,
            timeout=10,
            verify=False
        )
        
        if response.status_code == 200:
            data = response.json()
            sources = data.get("sources", [])
            
            CONFIG["api_endpoints"] = []
            
            for source in sources:
                if source["enabled"]:
                    if source["name"] == CONFIG["batch_config"]["name"]:
                        single_url = source["single_url"]
                        base_url = single_url.split('?')[0]
                        batch_endpoint = base_url.split('/')[-1]
                        base_url = base_url.rsplit('/', 1)[0]
                        
                        CONFIG["batch_config"]["base_url"] = base_url
                        CONFIG["batch_config"]["batch_endpoint"] = f"/{batch_endpoint}"
                        CONFIG["batch_config"]["token"] = source.get("token", "")
                        CONFIG["batch_config"]["enabled"] = True
                        CONFIG["api_endpoints"].append({
                            "url": single_url,
                            "name": source["name"]
                        })
                    else:
                        endpoint = {"url": source["single_url"], "name": source["name"]}
                        if source["name"] == "fanqie_sdk":
                            endpoint["params"] = source.get("params", {})
                            endpoint["data"] = source.get("data", {})
                        CONFIG["api_endpoints"].append(endpoint)
            
            with print_lock:
                print("成功从服务器获取API列表!")
            return True
        else:
            with print_lock:
                print(f"获取API列表失败，状态码: {response.status_code}")
            return False
    except Exception as e:
        with print_lock:
            print(f"获取API列表异常: {str(e)}")
        return False

def extract_chapters(soup):
    """解析章节列表"""
    chapters = []
    for idx, item in enumerate(soup.select('div.chapter-item')):
        a_tag = item.find('a')
        if not a_tag:
            continue
        
        raw_title = a_tag.get_text(strip=True)
        
        if re.match(r'^(番外|特别篇|if线)\s*', raw_title):
            final_title = raw_title
        else:
            clean_title = re.sub(
                r'^第[一二三四五六七八九十百千\d]+章\s*',
                '', 
                raw_title
            ).strip()
            final_title = f"第{idx+1}章 {clean_title}"
        
        chapters.append({
            "id": a_tag['href'].split('/')[-1],
            "title": final_title,
            "url": f"https://fanqienovel.com{a_tag['href']}",
            "index": idx
        })
    return chapters

def batch_download_chapters(item_ids, headers):
    """批量下载章节内容"""
    if not CONFIG["batch_config"]["enabled"] or CONFIG["batch_config"]["name"] != "qyuing":
        with print_lock:
            print("批量下载功能仅限qyuing API")
        return None
        
    batch_config = CONFIG["batch_config"]
    url = f"{batch_config['base_url']}{batch_config['batch_endpoint']}"
    
    try:
        batch_headers = headers.copy()
        if batch_config["token"]:
            batch_headers["token"] = batch_config["token"]
        batch_headers["Content-Type"] = "application/json"
        
        payload = {"item_ids": item_ids}
        response = make_request(
            url,
            headers=batch_headers,
            method='POST',
            data=payload,
            timeout=batch_config["timeout"],
            verify=False
        )
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                return data["data"]
            return data
        else:
            with print_lock:
                print(f"批量下载失败，状态码: {response.status_code}")
            return None
    except Exception as e:
        with print_lock:
            print(f"批量下载异常: {str(e)}")
        return None

def process_chapter_content(content):
    """处理章节内容"""
    if not content or not isinstance(content, str):
        return ""
    
    try:
        paragraphs = []
        if '<p idx=' in content:
            paragraphs = re.findall(r'<p idx="\d+">(.*?)</p>', content, re.DOTALL)
        else:
            paragraphs = content.split('\n')
        
        if paragraphs:
            first_para = paragraphs[0].strip()
            if not first_para.startswith('    '):
                paragraphs[0] = '    ' + first_para
        
        cleaned_content = "\n".join(p.strip() for p in paragraphs if p.strip())
        formatted_content = '\n'.join('    ' + line if line.strip() else line 
                                    for line in cleaned_content.split('\n'))
        
        formatted_content = re.sub(r'<header>.*?</header>', '', formatted_content, flags=re.DOTALL)
        formatted_content = re.sub(r'<footer>.*?</footer>', '', formatted_content, flags=re.DOTALL)
        formatted_content = re.sub(r'</?article>', '', formatted_content)
        formatted_content = re.sub(r'<[^>]+>', '', formatted_content)
        formatted_content = re.sub(r'\\u003c|\\u003e', '', formatted_content)
        
        # 压缩多余的空行
        formatted_content = re.sub(r'\n{3,}', '\n\n', formatted_content).strip()
        return formatted_content
    except Exception as e:
        with print_lock:
            print(f"内容处理错误: {str(e)}")
        return str(content)

def down_text(chapter_id, headers, book_id=None):
    """下载章节内容"""
    for endpoint in CONFIG["api_endpoints"]:
        current_endpoint = endpoint["url"]
        api_name = endpoint["name"]
        
        try:
            time.sleep(random.uniform(0.1, 0.5))
            
            if api_name == "fanqie_sdk":
                params = endpoint.get("params", {"sdk_type": "4", "novelsdk_aid": "638505"})
                data = {
                    "item_id": chapter_id,
                    "need_book_info": 1,
                    "show_picture": 1,
                    "sdk_type": 1
                }
                
                response = make_request(
                    current_endpoint,
                    headers=headers.copy(),
                    params=params,
                    method='POST',
                    data=data,
                    timeout=CONFIG["request_timeout"],
                    verify=False
                )
                
                if response.status_code != 200:
                    continue
                
                try:
                    data = response.json()
                    content = data.get("data", {}).get("content", "")
                    title = data.get("data", {}).get("title", "")
                    if content:
                        processed_content = process_chapter_content(content)
                        processed = re.sub(r'^(\s*)', r'    ', processed_content, flags=re.MULTILINE)
                        return title, processed
                except json.JSONDecodeError:
                    continue

            elif api_name == "fqweb":
                response = make_request(
                    current_endpoint.format(chapter_id=chapter_id),
                    headers=headers.copy(),
                    timeout=CONFIG["request_timeout"],
                    verify=False
                )
                
                try:
                    data = response.json()
                    if data.get("data", {}).get("code") in ["0", 0]:
                        content = data.get("data", {}).get("data", {}).get("content", "")
                        title = data.get("data", {}).get("data", {}).get("title", "")
                        if content:
                            processed_content = process_chapter_content(content)
                            processed = re.sub(r'^(\s*)', r'    ', processed_content, flags=re.MULTILINE)
                            return title, processed
                except:
                    continue

            elif api_name == "qyuing":
                response = make_request(
                    current_endpoint.format(chapter_id=chapter_id),
                    headers=headers.copy(),
                    timeout=CONFIG["request_timeout"],
                    verify=False
                )
                
                try:
                    data = response.json()
                    if data.get("data", {}).get("code") in ["0", 0]:
                        content = data.get("data", {}).get("data", {}).get("content", "")
                        title = data.get("data", {}).get("data", {}).get("title", "")
                        if content:
                            processed_content = process_chapter_content(content)
                            processed = re.sub(r'^(\s*)', r'    ', processed_content, flags=re.MULTILINE)
                            return title, processed
                except:
                    continue

            elif api_name == "lsjk":
                response = make_request(
                    current_endpoint.format(chapter_id=chapter_id),
                    headers=headers.copy(),
                    timeout=CONFIG["request_timeout"],
                    verify=False
                )
                
                if response.text:
                    try:
                        paragraphs = re.findall(r'<p idx="\d+">(.*?)</p>', response.text)
                        cleaned = "\n".join(p.strip() for p in paragraphs if p.strip())
                        formatted = '\n'.join('    ' + line if line.strip() else line 
                                            for line in cleaned.split('\n'))
                        return "", formatted
                    except:
                        continue

        except Exception as e:
            with print_lock:
                print(f"API {api_name} 请求异常: {str(e)[:50]}...，尝试切换")
            time.sleep(0.5)
            continue
    
    with print_lock:
        print(f"章节 {chapter_id} 所有API均失败")
    return None, None

def get_chapters_from_api(book_id, headers):
    """从API获取章节列表"""
    try:
        page_url = f'https://fanqienovel.com/page/{book_id}'
        response = requests.get(page_url, headers=headers, timeout=CONFIG["request_timeout"])
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        chapters = extract_chapters(soup)  
        
        api_url = f"https://fanqienovel.com/api/reader/directory/detail?bookId={book_id}"
        api_response = requests.get(api_url, headers=headers, timeout=CONFIG["request_timeout"])
        api_data = api_response.json()
        chapter_ids = api_data.get("data", {}).get("allItemIds", [])
        
        final_chapters = []
        for idx, chapter_id in enumerate(chapter_ids):
            web_chapter = next((ch for ch in chapters if ch["id"] == chapter_id), None)
            
            if web_chapter:
                final_chapters.append({
                    "id": chapter_id,
                    "title": web_chapter["title"],
                    "index": idx
                })
            else:
                final_chapters.append({
                    "id": chapter_id,
                    "title": f"第{idx+1}章",
                    "index": idx
                })
        
        return final_chapters
    except Exception as e:
        with print_lock:
            print(f"获取章节列表失败: {str(e)}")
        return None

def create_epub_book(name, author_name, description, chapter_results, chapters):
    """创建EPUB文件"""
    book = epub.EpubBook()
    book.set_identifier(f'book_{name}_{int(time.time())}')
    book.set_title(name)
    book.set_language('zh-CN')
    book.add_author(author_name)
    book.add_metadata('DC', 'description', description)
    
    book.toc = []
    spine = ['nav']
    
    for idx in range(len(chapters)):
        if idx in chapter_results:
            result = chapter_results[idx]
            title = f'{result["base_title"]} {result["api_title"]}' if result["api_title"] else result["base_title"]
            chapter = epub.EpubHtml(
                title=title,
                file_name=f'chap_{idx}.xhtml',
                lang='zh-CN'
            )
            content = result['content'].replace('\n', '<br/>')
            chapter.content = f'<h1>{title}</h1><p>{content}</p>'.encode('utf-8')
            book.add_item(chapter)
            book.toc.append(chapter)
            spine.append(chapter)
    
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())
    book.spine = spine
    
    return book

def download_chapter(chapter, headers, save_path, book_name, downloaded, book_id, file_format='txt'):
    """下载单个章节"""
    if chapter["id"] in downloaded:
        return None
    
    title, content = down_text(chapter["id"], headers, book_id)
    
    if content:
        if file_format == 'txt':
            output_file_path = os.path.join(save_path, f"{book_name}.txt")
            try:
                with open(output_file_path, 'a', encoding='utf-8') as f:
                    f.write(f'{chapter["title"]}\n')
                    f.write(content + '\n\n')
                
                downloaded.add(chapter["id"])
                save_status(save_path, downloaded)
                return chapter["index"], content
            except Exception as e:
                with print_lock:
                    print(f"写入文件失败: {str(e)}")
        return chapter["index"], content
    return None

def get_book_info(book_id, headers):
    """获取书名、作者、简介"""
    url = f'https://fanqienovel.com/page/{book_id}'
    try:
        response = requests.get(url, headers=headers, timeout=CONFIG["request_timeout"])
        if response.status_code != 200:
            with print_lock:
                print(f"网络请求失败，状态码: {response.status_code}")
            return None, None, None

        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        
        name_element = soup.find('h1')
        name = name_element.text if name_element else "未知书名"
        
        author_name = "未知作者"
        author_name_element = soup.find('div', class_='author-name')
        if author_name_element:
            author_name_span = author_name_element.find('span', class_='author-name-text')
            if author_name_span:
                author_name = author_name_span.text
        
        description = "无简介"
        description_element = soup.find('div', class_='page-abstract-content')
        if description_element:
            description_p = description_element.find('p')
            if description_p:
                description = description_p.text
        
        return name, author_name, description
    except Exception as e:
        with print_lock:
            print(f"获取书籍信息失败: {str(e)}")
        return None, None, None

def load_status(save_path):
    """加载下载状态"""
    status_file = os.path.join(save_path, CONFIG["status_file"])
    if os.path.exists(status_file):
        try:
            with open(status_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(data)
                return set()
        except:
            pass
    return set()

def save_status(save_path, downloaded):
    """保存下载状态"""
    status_file = os.path.join(save_path, CONFIG["status_file"])
    with open(status_file, 'w', encoding='utf-8') as f:
        json.dump(list(downloaded), f, ensure_ascii=False, indent=2)

def Run(book_id, save_path, file_format='txt'):
    """运行下载"""
    def signal_handler(sig, frame):
        print("\n检测到程序中断，正在保存已下载内容...")
        write_downloaded_chapters_in_order()
        save_status(save_path, downloaded)
        print(f"已保存 {len(downloaded)} 个章节的进度")
        sys.exit(0)
    
    def write_downloaded_chapters_in_order():
        """按章节顺序写入"""
        if not chapter_results:
            return
            
        if file_format == 'txt':
            with open(output_file_path, 'w', encoding='utf-8') as f:
                f.write(f"小说名: {name}\n作者: {author_name}\n内容简介: {description}\n\n")
                for idx in range(len(chapters)):
                    if idx in chapter_results:
                        result = chapter_results[idx]
                        title = f'{result["base_title"]} {result["api_title"]}' if result["api_title"] else result["base_title"]
                        f.write(f"{title}\n{result['content']}\n\n")
        elif file_format == 'epub':
            book = create_epub_book(name, author_name, description, chapter_results, chapters)
            epub.write_epub(output_file_path, book, {})
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        headers = get_headers()
        chapters = get_chapters_from_api(book_id, headers)
        if not chapters:
            print("未找到任何章节，请检查小说ID是否正确。")
            return
            
        name, author_name, description = get_book_info(book_id, headers)
        if not name:
            name = f"未知小说_{book_id}"
            author_name = "未知作者"
            description = "无简介"

        downloaded = load_status(save_path)
        if downloaded:
            print(f"检测到您曾经下载过小说《{name}》。")
            if input("是否需要再次下载？(y/n)：") != "y":
                print("已取消下载")
                return

        todo_chapters = [ch for ch in chapters if ch["id"] not in downloaded]
        if not todo_chapters:
            print("所有章节已是最新，无需下载")
            return

        print(f"开始下载：《{name}》, 总章节数: {len(chapters)}, 待下载: {len(todo_chapters)}")
        os.makedirs(save_path, exist_ok=True)
        
        output_file_path = os.path.join(save_path, f"{name}.{file_format}")
        if file_format == 'txt' and not os.path.exists(output_file_path):
            with open(output_file_path, 'w', encoding='utf-8') as f:
                f.write(f"小说名: {name}\n作者: {author_name}\n内容简介: {description}\n\n")

        success_count = 0
        failed_chapters = []
        chapter_results = {}
        lock = threading.Lock()

        if CONFIG["batch_config"]["enabled"] and CONFIG["batch_config"]["name"] == "qyuing":
            print("正在使用qyuing API批量下载！响应慢是正常现象。")
            batch_size = CONFIG["batch_config"]["max_batch_size"]
            
            with tqdm(total=len(todo_chapters), desc="批量下载进度") as pbar:
                for i in range(0, len(todo_chapters), batch_size):
                    batch = todo_chapters[i:i + batch_size]
                    item_ids = [chap["id"] for chap in batch]
                    
                    batch_results = batch_download_chapters(item_ids, headers)
                    if not batch_results:
                        print(f"第 {i//batch_size + 1} 批下载失败")
                        failed_chapters.extend(batch)
                        pbar.update(len(batch))
                        continue
                    
                    for chap in batch:
                        content = batch_results.get(chap["id"], "")
                        if isinstance(content, dict):
                            content = content.get("content", "")
                        
                        if content:
                            processed_content = process_chapter_content(content)
                            processed = re.sub(r'^(\s*)', r'    ', processed_content, flags=re.MULTILINE)
                            with lock:
                                chapter_results[chap["index"]] = {
                                    "base_title": chap["title"],
                                    "api_title": "",
                                    "content": processed
                                }
                                downloaded.add(chap["id"])
                                success_count += 1
                        else:
                            with lock:
                                failed_chapters.append(chap)
                        pbar.update(1)
            
            todo_chapters = failed_chapters.copy()
            failed_chapters = []
            write_downloaded_chapters_in_order()
            save_status(save_path, downloaded)

        if todo_chapters:
            print(f"开始单章下载模式，剩余 {len(todo_chapters)} 个章节...")
            
            def download_task(chapter):
                nonlocal success_count
                try:
                    fresh_headers = get_headers()
                    title, content = down_text(chapter["id"], fresh_headers, book_id)
                    if content:
                        with lock:
                            chapter_results[chapter["index"]] = {
                                "base_title": chapter["title"],
                                "api_title": title,
                                "content": content
                            }
                            downloaded.add(chapter["id"])
                            success_count += 1
                    else:
                        with lock:
                            failed_chapters.append(chapter)
                except Exception as e:
                    print(f"章节 {chapter['id']} 下载失败！")
                    with lock:
                        failed_chapters.append(chapter)

            attempt = 1
            while todo_chapters:
                print(f"\n第 {attempt} 次尝试，剩余 {len(todo_chapters)} 个章节...")
                attempt += 1
                
                with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
                    futures = [executor.submit(download_task, ch) for ch in todo_chapters]
                    
                    with tqdm(total=len(todo_chapters), desc="单章下载进度") as pbar:
                        for _ in as_completed(futures):
                            pbar.update(1)
                
                write_downloaded_chapters_in_order()
                save_status(save_path, downloaded)
                todo_chapters = failed_chapters.copy()
                failed_chapters = []
                
                if todo_chapters:
                    time.sleep(1)

        print(f"下载完成！成功下载 {success_count} 个章节")

    except Exception as e:
        print(f"运行错误: {str(e)}")
        if 'downloaded' in locals():
            write_downloaded_chapters_in_order()
            save_status(save_path, downloaded)

def main():
    print("""欢迎使用番茄小说下载器精简版！
开发者：Dlmily
当前版本：v1.8.3
Github：https://github.com/Dlmily/Tomato-Novel-Downloader-Lite
赞助/了解新产品：https://afdian.com/a/dlbaokanluntanos
*使用前须知*：
    开始下载之后，您可能会过于着急而查看下载文件的位置，这是徒劳的，请耐心等待小说下载完成再查看！另外如果你要下载之前已经下载过的小说(在此之前已经删除了原txt文件)，那么你有可能会遇到"所有章节已是最新，无需下载"的情况，这时就请删除掉chapter.json，然后再次运行程序。

另：如果有带番茄svip的cookie或api，按照您的意愿投到“Issues”页中。
------------------------------------------""")
    
    print("正在从服务器获取API列表...")
    if not fetch_api_endpoints_from_server():
        print("无法获取API列表，请重试！")
        return
    
    while True:
        book_id = input("请输入小说ID (输入q退出)：").strip()
        if book_id.lower() == 'q':
            break
            
        save_path = input("保存路径 (留空为当前目录)：").strip() or os.getcwd()
        
        file_format = input("请选择下载格式 (1:txt, 2:epub)：").strip()
        if file_format == '1':
            file_format = 'txt'
        elif file_format == '2':
            file_format = 'epub'
        else:
            print("无效的格式选择，将默认使用txt格式")
            file_format = 'txt'
        
        try:
            Run(book_id, save_path, file_format)
        except Exception as e:
            print(f"运行错误: {str(e)}")
        
        print("\n" + "="*50 + "\n")
        
if __name__ == "__main__":
    main()