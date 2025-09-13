# 番茄小说下载器精简版
>[!IMPORTANT]
>
>1.各位用户，由于盗api事件的存在，为了保障
api开发者和本程序的权益，1.7正式版之后的版本将进入“半开源状态”，也就是除了api和密钥的其他部分开源。
>
>2.如果有番茄API，请在Issues页面里回复我，这我们十分重要！
>
>3.一定一定要把此文档仔细看完！
>
>4.项目开发者有时会玩失踪

如你所见，这个程序只有不到29kb的python文件，但这不影响它的功能！这个程序简单易操作，可以满足你的小说下载需求，需要运行此程序的话，最好是在终端中，以下的所有需要输入的内容都需要在终端中进行，并且在使用此程序之前，您必须先安装python！
## 我该如何使用？
<details>
<summary>手机端(点击展开查看)</summary>

- 1.首先下载Termux ，找到符合您手机配置的apk文件（如果你的手机是在2020年以后购买的，那就选择带有arm64文件名的apk），下载并安装，接着打开应用，然后输入
```bash
termux-setup-storage
```
并回车(也就是换行)。执行后，系统会弹出一个权限请求，请点击“允许”来获取存储权限。

- 2.下载文件2.py，并通过文件管理器获取到这个文件所处的目录位置并复制它备用，在Termux输入： `cd+空格+复制的目录`然后回车。

> 注：文件所处的目录位置就是下载的文件所在的地方，比如我下载了一个文件，这个文件就会显示在文件管理器中，但是该在哪里寻找它呢？要寻找的这个地方就是文件所处的目录，假如我下载的这个文件位置在“/storage/emulated/0/Download/”里，那么这个文件的位置就是这个文件所处的目录。

- 3.在Termux中依次输入安装命令并回车运行：

换源：
```bash
sed -i 's@^\(deb.*stable main\)$@#\1\ndeb https://mirrors.tuna.tsinghua.edu.cn/termux/apt/termux-main stable main@' $PREFIX/etc/apt/sources.list
```
更新软件库：
```
apt update && apt upgrade
```
安装必要库：
```
pkg install python
```

```
pkg install python-pip
```
安装程序依赖：
```
pkg install libxml2 libxslt
```

```
pip install requests beautifulsoup4 urllib3 stem tqdm fake-useragent pycryptodome lxml ebooklib
```

**注：在运行安装命令的时候，您可能会遇到“Do you want to continue? \[Y/n\]”这种情况，这时请输入大写的“Y”并回车来继续下载。**

- 4.全部安装完成且没有显示报错的情况，就继续输入

```bash
python 2.py
```

并回车来启动程序即可
</details>

<details>
<summary>电脑端(点击展开查看)</summary>

### Windows系统

1. **安装Python**

   - 访问Python官网下载最新版本
   - 安装时务必勾选"Add Python to PATH"选项

2. **下载脚本文件**

   - 从GitHub Releases页面下载最新版本的2.py文件
   - 记住文件的保存路径

3. **安装依赖库**

   - 打开命令提示符(CMD)或PowerShell

   - 输入以下命令安装所需库：

     ```bash
     pip install requests beautifulsoup4 urllib3 stem tqdm fake-useragent pycryptodome lxml ebooklib
     ```

4. **运行程序**

   - 在命令提示符中导航到脚本所在目录：

     ```bash
     cd 你的文件路径
     ```

   - 运行程序：

     ```bash
     python 2.py
     ```

### macOS系统

1. **安装Python**

   - 推荐使用Homebrew安装：

     ```bash
     brew install python
     ```

   - 或从Python官网下载安装包

2. **下载脚本文件**

   - 同Windows步骤

3. **安装依赖库**

   - 打开终端(Terminal)

   - 输入以下命令：

     ```bash
     pip3 install requests beautifulsoup4 urllib3 stem tqdm fake-useragent pycryptodome lxml ebooklib
     ```

4. **运行程序**

   - 在终端中导航到脚本目录：

     ```bash
     cd 你的文件路径
     ```

   - 运行程序：

     ```bash
     python3 2.py
     ```

### Linux系统

1. **安装Python**

   - 大多数Linux发行版已预装Python，可通过以下命令检查：

     ```bash
     python3 --version
     ```

   - 如需安装：

     ```bash
     sudo apt-get install python3  # Debian/Ubuntu
     sudo yum install python3      # CentOS/RHEL
     ```

2. **下载脚本文件**

   - 同Windows步骤

3. **安装依赖库**

   - 打开终端

   - 输入以下命令：

     ```bash
     pip3 install requests beautifulsoup4 urllib3 stem tqdm fake-useragent pycryptodome lxml ebooklib
     ```

4. **运行程序**

   - 导航到脚本目录：

     ```bash
     cd 你的文件路径
     ```

   - 运行程序：

     ```bash
     python3 2.py
     ```
 
**注：**
- 如果遇到权限问题，可在命令前加上“sudo”(Linux/macOS可用)或以管理员身份运行CMD(PowerShell)
</details>

## 常见问题
1.`此程序的优势在哪？`

本程序的初衷就是极致简化番茄小说下载器的代码，使程序更加易于操作与运行、更加稳定和快速，并且全平台通用，文档对小白十分友好！

2.`为什么我在安装lxml库的时候始终安装不了？`

按照以下步骤解决：
```bash
apt install clang 
apt install libxml2
apt install libxslt 
pip install cython 
CFLAGS="-O0" pip install lxml
```

3.`小说id是什么？在哪里获取？`

首先你需要找到自己想下载的小说的详情页(例如https://fanqienovel.com/page/7143038691944959011 )，链接中“7143038691944959011”就是小说id
>**注意：详情页链接一定是“https://fanqienovel.com/page/小说id ”这样的。如果不对就继续寻找！或者如果您是手机端，那么就访问主页“https://fanqienovel.com/ ”再在浏览器中寻找并点击“查看桌面网站”选项，然后在主页的右上角找到搜索框，搜索你要下载书籍的名字，搜索完成后找到对应的书籍名并点击它来查看详情页，接着复制当前详情页的网页链接就可以找到小说id了**

4.`我是纯小白，2.py代码在哪里下载啊`

直接点击[此链接](https://github.com/Dlmily/Tomato-Novel-Downloader-Lite/releases/ )，先找到最新版本，然后在最新版本中找到”Assets”并点击来展开内容(如果已展开就不必进行此操作)。在展开的内容中找到2.py代码，点击下载即可。
<details>
<summary>第2种下载方案(点击展开查看)
</summary>

- 第一步
  
![Screenshot_20250608-204406](https://github.com/user-attachments/assets/5357457c-92d8-4b15-af38-6cdad23c57a1)
- 第二步

![Screenshot_20250608-204442](https://github.com/user-attachments/assets/2d349eb3-8b3b-44a6-9188-264b7fbb658f)
- 第三步

![Screenshot_20250608-204456](https://github.com/user-attachments/assets/cd50b5b8-a07b-4f44-b46c-e8f2e8ea8008)
</details>

5.`我无法正常运行代码，有没有可执行文件代替？`

[点击此处跳转到可执行文件安装页面](https://github.com/Dlmily/Tomato-Novel-Downloader-Lite/releases)

然后再重新尝试

6.`怎么中断程序？`

Ctrl+C中断程序（先按Ctrl再按C，多试几次就能中断程序了）

7.`Tor网络怎么使用`

> 适用于旧版本

手机端：
首先下载“Orbot”，安装完成后打开，点击“配置连接方式”，在弹出的页面中点击“询问TOR”，程序会自动为您匹配最适合当前地区的连接方式，然后点击“连接”，等待即可。当然如果你要将TOR网络流量集中的话，连接完成后点击“选择应用”，在弹出的应用列表中勾选“Termux”，然后保存即可。

电脑端(多系统)：
> [!TIP]
> 
> 建议您用Snowflake进行连接，并尝试在Tor浏览器中打开任意网页，查看是否可以使用Tor网络连接，然后再尝试运行2.py
<details>  
<summary>点击展开</summary>  

  (运行2.py脚本前需要关闭系统代理，再使用tor网络下载。注意端口为9050！)
  ![](https://github.com/user-attachments/assets/fb6f1880-09b1-46db-94ce-d3b666bb04ef)
  
### **1. Windows系统使用Tor网络**
#### **方法一：使用Tor浏览器（推荐）**
1. **下载并安装Tor浏览器**  
   - 访问 [Tor官网](https://www.torproject.org/) 下载Tor浏览器（自带Tor代理功能）。
2. **运行Tor浏览器**  
   - 启动后，Tor浏览器会自动连接Tor网络，无需额外配置。
3. **手动配置代理（可选）**  
   - 如果其他应用（如Python脚本）需要通过Tor代理，可设置SOCKS5代理：
     ```python
     proxies = {
         'http': 'socks5h://127.0.0.1:9150',
         'https': 'socks5h://127.0.0.1:9150'
     }
     ```
     （Tor浏览器默认使用端口9150）。

#### **方法二：使用Tor Expert Bundle（高级用户）**
- 适用于需要Tor命令行工具的场景：
  - 下载Tor Expert Bundle（无浏览器）。
  - 编辑`torrc`文件配置SOCKS代理（如`SocksPort 9050`）。

---

### **2. macOS系统使用Tor网络**
#### **方法一：使用Tor浏览器**
1. **下载Tor浏览器**  
   - 从Tor官网获取macOS版本并安装。
2. **连接Tor网络**  
   - 启动后自动连接，支持.onion网站访问。

#### **方法二：命令行安装Tor**
1. **通过Homebrew安装**  
   ```bash
   brew install tor
   ```
2. **启动Tor服务**  
   ```bash
   tor
   ```
   - 默认SOCKS代理端口为9050。

---

### **3. Linux（Unix-like）系统使用Tor网络**
#### **方法一：使用Tor浏览器**
- 与Windows/macOS类似，下载对应版本运行即可。

#### **方法二：通过包管理器安装Tor**
1. **Debian/Ubuntu**  
   ```bash
   sudo apt-get install tor torsocks
   ```
2. **RHEL/CentOS**  
   ```bash
   sudo yum install tor torsocks
   ```
3. **配置与使用**  
   - 编辑`/etc/tor/torrc`配置桥接或代理。
   - 使用`torsocks`运行命令：
     ```bash
     torsocks ssh user@server
     ```
</details>  

## 注意事项（必看）
由于使用的是api，所以未来不知道有哪一天突然失效，如果真的出现了，请立即在“Issues”页面中回复！

如果您在使用本程序的时候出现了下载章节失败的情况，也许并不是api失效了，可能是因为调用api人数过多，导致api暂时关闭，如果遇到了这种情况，请稍后再试，另外，您需要下载的小说api可能会因没有更新所以下载失败。

千万不要想着耍小聪明：“欸，我改一下线程数不就能快速下载了吗？”请打消这种念头！因为这样会加大服务器压力！！！

在v1.6.3.2版本以后已经可以使用vpn或其他网络代理了，之前的版本依旧不能使用！

如果您也没有遇到以上的这种情况，请检查要下载的小说章节数量有多少，不建议大于1000章！如果真的出现了这种情况，可以先中断程序，再运行程序

> [!WARNING]
>
>划重点：切记！不能将此程序用于违法用途，例如将下载到的小说进行转载、给不良人员分享此程序使用等。本开发者严禁不支持这样做！！！并且请不要将api进行转载使用，除非您已经与开发者协商过，否则后果自负！下载到的小说仅供自行阅读，看完之后请立即删除文件，以免造成侵权，如果您还是偷尝禁果，需自行承担由此引发的任何法律责任和风险。程序的作者及项目贡献者不对因使用本程序所造成的任何损失、损害或法律后果负责！

## 赞助/了解新产品
~[DL报刊论坛](https://dlbkltos.s7123.xyz/)

~[小米手环七图像转换工具](https://github.com/Dlmily/ImageToMiBand7)

- [爱发电赞助](https://afdian.com/a/dlbaokanluntanos )

> [!NOTE]
>
>赞助前须知：是否赞助全凭用户您自行选择，不提供退款服务，所以请注意赞助的金额！
## 免责声明
  本程序仅供 Python 网络爬虫技术、网页数据处理及相关研究的学习用途。请勿将其用于任何违反法律法规或侵犯他人权益的活动。
  
  使用本程序的用户需自行承担由此引发的任何法律责任和风险。程序的作者及项目贡献者不对因使用本程序所造成的任何损失、损害或法律后果负责。
  
  在使用本程序之前，请确保您遵守适用的法律法规以及目标网站的使用政策。如有任何疑问或顾虑，请咨询专业法律顾问。

## 感谢
感谢用户选择此程序，如果喜欢可以加star，想关注版本更新的可以加watching，如果有什么对本程序的建议，请在“Issues”页面提出。您的喜欢就是我更新的最大动力❤️
***
感谢来自Github用户@qxqycb的开源项目！

感谢来自QQ用户@终忆的api！

感谢来自Github用户@s7123studio的服务器支持！

感谢来自Github用户@jingluopro的api！

感谢来自Gmail用户@Deivid Zubi的api！

感谢来自Github用户@lnb0X的api！

感谢来自Github用户@zyh6663的cookie！

感谢来自Github用户@hailey07的cookie！

感谢Github用户@Eternity231的代码协助！

感谢来自Github用户@helloplhm-qwq的api！

感谢来自[此项目](https://github.com/POf-L/Fanqie-novel-Downloader) 的api！

感谢来自Github用户@huangchaoabc提供[此项目](https://github.com/duongden/fanqienovel) 的api！

感谢所有赞助此程序的赞助者们！
***
