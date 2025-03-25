import time
import requests
import bs4
import re
import os
import random
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import OrderedDict

# 全局配置
CONFIG = {
    "max_workers": 5,
    "max_retries": 3,
    "request_timeout": 15,
    "status_file": "chapter.json",
    "user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ]
}

def get_headers(cookie=None):
    """生成随机请求头"""
    headers = {
        "User-Agent": random.choice(CONFIG["user_agents"]),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive"
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers

def get_cookie():
    """生成或加载Cookie"""
    cookie_path = "cookie.json"
    if os.path.exists(cookie_path):
        try:
            with open(cookie_path, 'r') as f:
                return f.read().strip() 
        except:
            pass
    
    # 生成新Cookie
    for _ in range(3):
        novel_web_id = random.randint(10**18, 10**19-1)
        cookie = f'novel_web_id={novel_web_id}'
        try:
            resp = requests.get(
                'https://fanqienovel.com',
                headers={"User-Agent": random.choice(CONFIG["user_agents"])},
                cookies={"novel_web_id": str(novel_web_id)},
                timeout=10
            )
            if resp.ok:
                with open(cookie_path, 'w') as f:
                    f.write(cookie)
                return cookie
        except Exception as e:
            print(f"Cookie生成失败: {str(e)}")
            time.sleep(1)
    raise Exception("无法获取有效Cookie")

def down_text(chapter_id, headers):
    """下载章节内容"""
    max_retries = CONFIG["max_retries"]
    retry_count = 0
    content = ""
    
    # 定义API端点
    api_endpoints = [
        f"http://fan.jingluo.love/content?item_id={chapter_id}",
        f"http://rehaofan.jingluo.love/content?item_id={chapter_id}"
    ]
    
    # 随机选择一个API
    primary_api, fallback_api = random.sample(api_endpoints, 2)
    
    while retry_count < max_retries:
        try:
            # 添加随机延迟
            time.sleep(random.uniform(1, 3))
            
            # 主API
            response = requests.get(primary_api, headers=headers, timeout=CONFIG["request_timeout"])
            data = response.json()
            
            if data.get("code") == 0:
                content = data.get("data", {}).get("content", "")
                if not content:
                    # 备用API
                    response = requests.get(fallback_api, headers=headers, timeout=CONFIG["request_timeout"])
                    data = response.json()
                    if data.get("code") == 0:
                        content = data.get("data", {}).get("content", "")
                
                if content:
                    # 清理HTML标签并保留段落结构
                    content = re.sub(r'<header>.*?</header>', '', content, flags=re.DOTALL)
                    content = re.sub(r'<footer>.*?</footer>', '', content, flags=re.DOTALL)
                    content = re.sub(r'</?article>', '', content)
                    content = re.sub(r'<p idx="\d+">', '\n', content)
                    content = re.sub(r'</p>', '\n', content)
                    content = re.sub(r'<[^>]+>', '', content)
                    content = re.sub(r'\\u003c|\\u003e', '', content)
                    content = re.sub(r'\n{2,}', '\n', content).strip()
                    content = '\n'.join(['    ' + line if line.strip() else line for line in content.split('\n')])
                    break
                
        except requests.exceptions.RequestException as e:
            retry_count += 1
            print(f"网络请求失败，正在重试({retry_count}/{max_retries}): {str(e)}")
            time.sleep(2 * retry_count)
            
            # 在重试时切换API
            primary_api, fallback_api = fallback_api, primary_api
        except Exception as e:
            retry_count += 1
            print(f"下载出错，正在重试({retry_count}/{max_retries}): {str(e)}")
            time.sleep(1 * retry_count)
            
            # 在重试时切换API
            primary_api, fallback_api = fallback_api, primary_api
    
    return content if content else None

def get_book_info(book_id, headers):
    """获取书名、作者、简介"""
    url = f'https://fanqienovel.com/page/{book_id}'
    try:
        response = requests.get(url, headers=headers, timeout=CONFIG["request_timeout"])
        if response.status_code != 200:
            print(f"网络请求失败，状态码: {response.status_code}")
            return None, None, None

        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        
        # 获取书名
        name_element = soup.find('h1')
        name = name_element.text if name_element else "未知书名"
        
        # 获取作者
        author_name = "未知作者"
        author_name_element = soup.find('div', class_='author-name')
        if author_name_element:
            author_name_span = author_name_element.find('span', class_='author-name-text')
            if author_name_span:
                author_name = author_name_span.text
        
        # 获取简介
        description = "无简介"
        description_element = soup.find('div', class_='page-abstract-content')
        if description_element:
            description_p = description_element.find('p')
            if description_p:
                description = description_p.text
        
        return name, author_name, description
    except Exception as e:
        print(f"获取书籍信息失败: {str(e)}")
        return None, None, None

def extract_chapters(soup):
    """解析章节列表"""
    chapters = []
    chapter_items = soup.select('div.chapter-item')
    
    if not chapter_items:
        print("警告：未找到任何章节！")
        return chapters
    
    for idx, item in enumerate(chapter_items):
        a_tag = item.find('a')
        if not a_tag or not a_tag.get('href'):
            continue
        
        raw_title = a_tag.get_text(strip=True)
        chapter_id = a_tag['href'].split('/')[-1]
        
        if not chapter_id:
            continue
        
        # 特殊章节
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
            "id": chapter_id,
            "title": final_title,
            "url": f"https://fanqienovel.com{a_tag['href']}",
            "index": idx
        })
    
    # 检查章节顺序
    if len(chapters) > 0:
        expected_indices = set(range(len(chapters)))
        actual_indices = set(ch["index"] for ch in chapters)
        if expected_indices != actual_indices:
            print("警告：章节顺序异常，可能未按阿拉伯数字顺序排列！")
            chapters.sort(key=lambda x: x["index"])
    
    return chapters

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

def download_chapter(chapter, headers, save_path, book_name, downloaded):
    """下载单个章节"""
    if chapter["id"] in downloaded:
        return None
    
    content = down_text(chapter["id"], headers)
    if content:
        output_file_path = os.path.join(save_path, f"{book_name}.txt")
        try:
            with open(output_file_path, 'a', encoding='utf-8') as f:
                f.write(f'{chapter["title"]}\n')
                f.write(content + '\n\n')
            downloaded.add(chapter["id"])
            return chapter["index"], content
        except Exception as e:
            print(f"写入文件失败: {str(e)}")
    return None

def Run(book_id, save_path):
    """运行下载"""
    try:
        headers = get_headers(get_cookie())
        
        # 获取书籍信息
        name, author_name, description = get_book_info(book_id, headers)
        if not name:
            print("无法获取书籍信息，请检查小说ID或网络连接。")
            return

        # 检查是否曾经下载过该小说
        status_file = os.path.join(save_path, CONFIG["status_file"])
        downloaded = load_status(save_path)
        
        if downloaded:
            print(f"检测到您曾经下载过小说《{name}》。")
            user_input = input("是否需要再次下载？如果需要请输入1并回车，如果不需要请直接回车即可返回主程序：")
            if user_input != "1":
                print("已取消下载，返回主程序。")
                return

        # 获取章节列表
        url = f'https://fanqienovel.com/page/{book_id}'
        response = requests.get(url, headers=headers, timeout=CONFIG["request_timeout"])
        soup = bs4.BeautifulSoup(response.text, 'lxml')

        chapters = extract_chapters(soup)
        if not chapters:
            print("未找到任何章节，请检查小说ID是否正确。")
            return

        todo_chapters = [ch for ch in chapters if ch["id"] not in downloaded]

        if not todo_chapters:
            print("所有章节已是最新，无需下载")
            return

        print(f"开始下载：《{name}》, 总章节数: {len(chapters)}, 待下载: {len(todo_chapters)}")
        os.makedirs(save_path, exist_ok=True)

        # 写入书籍信息
        output_file_path = os.path.join(save_path, f"{name}.txt")
        if not os.path.exists(output_file_path):
            with open(output_file_path, 'w', encoding='utf-8') as f:
                f.write(f"小说名: {name}\n作者: {author_name}\n内容简介: {description}\n\n")

        # 多线程下载并缓存内容
        content_cache = OrderedDict()
        success_count = 0

        # 顺序下载
        sequential_chapters = todo_chapters[:5]
        for chapter in sequential_chapters:
            time.sleep(random.uniform(1, 3))  # 随机延迟
            result = download_chapter(chapter, headers, save_path, name, downloaded)
            if result:
                index, content = result
                content_cache[index] = (chapter, content)
                success_count += 1

        # 使用多线程下载剩余章节
        remaining_chapters = todo_chapters[5:]
        if remaining_chapters:
            with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
                futures = {
                    executor.submit(
                        download_chapter, 
                        ch, 
                        headers, 
                        save_path, 
                        name, 
                        downloaded
                    ): ch for ch in remaining_chapters
                }
                
                with tqdm(total=len(remaining_chapters), desc="下载进度", unit="章") as pbar:
                    for future in as_completed(futures):
                        chapter = futures[future]
                        try:
                            result = future.result()
                            if result:
                                index, content = result
                                content_cache[index] = (chapter, content)
                                success_count += 1
                        except Exception as e:
                            print(f"章节 [{chapter['title']}] 处理失败: {str(e)}")
                        finally:
                            pbar.update(1)
                            time.sleep(random.uniform(0.5, 1.5))  # 短暂延迟

        # 按顺序写入文件
        if content_cache:
            sorted_chapters = sorted(content_cache.items(), key=lambda x: x[0])
            with open(output_file_path, 'a', encoding='utf-8') as f:
                for index, (chapter, content) in sorted_chapters:
                    f.write(f"{chapter['title']}\n")
                    f.write(content + '\n\n')

        # 保存下载状态
        save_status(save_path, downloaded)

        print(f"下载完成！成功: {success_count}, 失败: {len(todo_chapters)-success_count}")

    except Exception as e:
        print(f"运行过程中发生错误: {str(e)}")

def main():
    print("""欢迎使用番茄小说下载器精简版！
作者：Dlmos（Dlmily）
Github：https://github.com/Dlmily/Tomato-Novel-Downloader-Lite
赞助/了解新产品：https://afdian.com/a/dlbaokanluntanos
------------------------------------------""")
    
    while True:
        book_id = input("请输入小说ID（输入q退出）：").strip()
        if book_id.lower() == 'q':
            break
            
        save_path = input("保存路径（留空为当前目录）：").strip() or os.getcwd()
        
        try:
            Run(book_id, save_path)
        except Exception as e:
            print(f"运行错误: {str(e)}")
        
        print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    main()