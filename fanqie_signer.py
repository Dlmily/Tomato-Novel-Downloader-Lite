# -*- coding: utf-8 -*-
"""
fanqie_signer.py
=================
番茄小说 (Fanqie Novel / com.dragon.read.oversea.gp, aid=1967) 请求签名工具。

四个头的生成方式：
    - X-Gorgon / X-Khronos   (class XGorgon)
    - X-Ladon                (class XLadon)
    - X-Argus                (class XArgus)
    - X-SS-Stub              (generate_stub，gorgon 要用它)

并在文件末尾把它们接进设备注册 -> key 注册 -> 批量取章的完整流程
（FanqieDeviceClient / FanqieBatchClient），三处请求都带上这四个头。

────────────────────────────────────────────────────────────────────────
⚠️ 当前验证状态（重要，请先看完再跑）
────────────────────────────────────────────────────────────────────────
  1. XGorgon —— 前缀 "8404"，已用 3 组独立真机抓包核实（老数据 + 两张截图）。
     混合算法（_initialize/_handle/_encryption_table）沿用 tiktok-signer
     开源库的实现，结构上跟抓包长度对得上，但没有做到逐字节验证（缺已知
     明文/密文对），如果服务端还是拒绝，这一块是下一个排查重点。

  2. XArgus / XLadon —— **已用 3 组独立真机抓包逐字节验证过**，是短签
     方案，不是 tiktok-signer 库里那套 protobuf bean + Simon 加密的长签：
         x-argus = base64( struct.pack('<I', khronos) )   # 小端 4 字节
         x-ladon = base64( struct.pack('>I', khronos) )   # 大端 4 字节
     三组抓包 (x-khronos, x-argus, x-ladon) 拿这个公式现算，全部精确匹配。
     置信度目前是四个头里最高的。

  3. 真机抓包里其实还有 x-helios 和 x-medusa 两个头（"四神"已经不够用了），
     这两个目前完全没有实现——x-helios 长度中等，x-medusa 是几百字符的巨大
     base64 blob，看起来是加密后的设备指纹/风控数据。反编译 smali 搜不到
     这两个名字的 Java 层字符串，说明是 native 层（大概率就是你在 unidbg
     里调的 libmetasec_ml.so）算出来再传回 Java 的，静态分析走不通，只能
     等 unidbg 那条线（0x5000001 初始化路径）跑通后直接从模拟器里取真值，
     或者上 Frida hook 实机验证。如果某个接口加了四神还是不通，大概率就是
     卡在这两个头上。
"""

from __future__ import annotations

import re
import time
import uuid
import gzip
import base64
import hashlib
import asyncio
import random
from copy import deepcopy
import struct
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode, parse_qs
from datetime import datetime

import aiohttp
from aiohttp import web
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad, pad as _pad
from Crypto.Cipher.AES import MODE_CBC, block_size, new as aes_new


# ════════════════════════════════════════════════════════════════════
# 0. 通用小工具
# ════════════════════════════════════════════════════════════════════

def random_uuid() -> str:
    p1 = random.randint(0, 0xffff)
    p2 = random.randint(0, 0xffff)
    p3 = random.randint(0, 0xffff)
    p4 = random.randint(0, 0x0fff) | 0x4000
    p5 = random.randint(0, 0x3fff) | 0x8000
    p6 = random.randint(0, 0xffff)
    p7 = random.randint(0, 0xffff)
    p8 = random.randint(0, 0xffff)
    return '%04x%04x-%04x-%04x-%04x-%04x%04x%04x' % (p1, p2, p3, p4, p5, p6, p7, p8)


def r_hex(num: int) -> str:
    """大端 32 字节 hex 反转为小端字节序 hex（key_register 用）。"""
    hex_str = format(num, '032x')
    res = ''
    for i in range(len(hex_str), 0, -2):
        res += hex_str[i - 2:i]
    return res


def generate_stub(data: Optional[Union[Dict, str, bytes]] = None) -> str:
    """x-ss-stub：请求体的 md5（dict/str 会先 gzip 压缩），Gorgon/Argus 计算都要用。"""
    if data is None:
        return hashlib.md5(b"undefined").hexdigest().upper()
    if isinstance(data, dict):
        data_bytes = gzip.compress(urlencode(data).encode(), compresslevel=9, mtime=0)
    elif isinstance(data, str):
        data_bytes = gzip.compress(data.encode(), compresslevel=9, mtime=0)
    elif isinstance(data, bytes):
        data_bytes = data
    else:
        return hashlib.md5(b"undefined").hexdigest().upper()
    return hashlib.md5(data_bytes).hexdigest().upper()


# ════════════════════════════════════════════════════════════════════
# 4. X-Gorgon / X-Khronos  —— 通用，无内置密钥
# ════════════════════════════════════════════════════════════════════

class XGorgon:
    LENGTH = 20
    HEX_DIGITS = [30, 64, 224, 217, 147, 69, 0, 180]

    @classmethod
    def _encryption_table(cls) -> list:
        tmp = ""
        table = list(range(256))
        for i in range(256):
            a = 0 if i == 0 else (tmp if tmp else table[i - 1])
            b = cls.HEX_DIGITS[i % 8]
            if a == 85 and i != 1 and tmp != 85:
                a = 0
            c = (a + i + b) % 256
            tmp = c if c < i else ""
            table[i] = table[c]
        return table

    @classmethod
    def _initialize(cls, data: list, table: list) -> list:
        tmp_add: list = []
        tmp_table = deepcopy(table)
        for i in range(cls.LENGTH):
            a = data[i]
            b = tmp_add[-1] if tmp_add else 0
            c = (table[i + 1] + b) % 256
            tmp_add.append(c)
            d = tmp_table[c]
            tmp_table[i + 1] = d
            e = (d + d) % 256
            f = tmp_table[e]
            data[i] = a ^ f
        return data

    @staticmethod
    def _reverse_nibble(num: int) -> int:
        s = hex(num)[2:].zfill(2)
        return int(s[1:] + s[:1], 16)

    @staticmethod
    def _rbit(num: int) -> int:
        s = bin(num)[2:].zfill(8)
        return int(s[::-1], 2)

    @classmethod
    def _handle(cls, data: list) -> list:
        for i in range(cls.LENGTH):
            a = data[i]
            b = cls._reverse_nibble(a)
            c = data[(i + 1) % cls.LENGTH]
            d = b ^ c
            e = cls._rbit(d)
            f = e ^ cls.LENGTH
            g = ~f
            while g < 0:
                g += 4294967296
            data[i] = int(hex(g)[-2:], 16)
        return data

    @staticmethod
    def _hex2(num: int) -> str:
        return hex(num)[2:].zfill(2)

    @classmethod
    def _calculate(cls, gorgon: list) -> str:
        result = "".join(
            cls._hex2(x) for x in cls._handle(cls._initialize(gorgon, cls._encryption_table()))
        )
        # 前缀已经用真机抓包核实过：番茄小说当前版本是 "8404"，不是
        # "0404"。之前照着 TomatoSigner.java 改成 0404 是错的——那份
        # harness 代码本身可能还没跑通/没验证过，真实抓包才是准的。
        return "8404{}{}{}{}{}".format(
            cls._hex2(cls.HEX_DIGITS[7]),
            cls._hex2(cls.HEX_DIGITS[3]),
            cls._hex2(cls.HEX_DIGITS[1]),
            cls._hex2(cls.HEX_DIGITS[6]),
            result,
        )

    @classmethod
    def calculate(
        cls,
        params: Union[str, Dict],
        headers: Optional[Dict[str, str]] = None,
        cookie: Optional[str] = None,
        unix: Optional[int] = None,
    ) -> Dict[str, str]:
        """生成 x-gorgon / x-khronos。"""
        params_str = urlencode(params) if isinstance(params, dict) else str(params)
        headers = {k.lower(): v for k, v in (headers or {}).items()}
        if cookie is not None:
            headers["cookie"] = cookie

        ts = unix if unix is not None else int(time.time())
        khronos_hex = hex(ts)[2:].zfill(8)

        gorgon: list = []
        url_md5 = hashlib.md5(params_str.encode()).hexdigest()
        gorgon += [int(url_md5[2 * i:2 * i + 2], 16) for i in range(4)]

        if "x-ss-stub" in headers:
            stub = headers["x-ss-stub"]
            gorgon += [int(stub[2 * i:2 * i + 2], 16) for i in range(4)]
        else:
            gorgon += [0] * 4

        if "cookie" in headers:
            cookie_md5 = hashlib.md5(headers["cookie"].encode()).hexdigest()
            gorgon += [int(cookie_md5[2 * i:2 * i + 2], 16) for i in range(4)]
        else:
            gorgon += [0] * 4

        gorgon += [0] * 4
        gorgon += [int(khronos_hex[2 * i:2 * i + 2], 16) for i in range(4)]

        return {"x-gorgon": cls._calculate(gorgon), "x-khronos": str(ts)}


# ════════════════════════════════════════════════════════════════════
# 5. X-Ladon / X-Argus —— 短签方案，已用 3 组独立真机抓包逐字节验证
#
#    之前从 tiktok-signer 库照搬的长签版本（protobuf bean + Simon 分组
#    密码）整个是错的算法——这个 App 版本的 x-argus/x-ladon 短得多
#    （base64 后只有 8 个字符），验证下来就是把 x-khronos 时间戳打包
#    成 4 字节再 base64，两个头分别用小端/大端字节序区分：
#
#        x-argus = base64( struct.pack('<I', khronos) )   # 小端
#        x-ladon = base64( struct.pack('>I', khronos) )   # 大端
#
#    验证方式：拿三组真实抓包的 (x-khronos, x-argus, x-ladon) 现算一遍，
#    三组全部逐字节对上，置信度非常高。
# ════════════════════════════════════════════════════════════════════

class XLadon:
    @classmethod
    def calculate(cls, unix: Optional[int] = None, **_ignored) -> Dict[str, str]:
        """生成 x-ladon（短签版，已抓包验证）。"""
        ts = unix if unix is not None else int(time.time())
        return {"x-ladon": base64.b64encode(struct.pack(">I", ts)).decode()}


class XArgus:
    @classmethod
    def calculate(cls, unix: Optional[int] = None, **_ignored) -> Dict[str, str]:
        """生成 x-argus（短签版，已抓包验证）。"""
        ts = unix if unix is not None else int(time.time())
        return {"x-argus": base64.b64encode(struct.pack("<I", ts)).decode()}


# ════════════════════════════════════════════════════════════════════
# 7. FanqieSigner —— 汇总四个头，默认参数按番茄小说(aid=1967)填
# ════════════════════════════════════════════════════════════════════

class FanqieSigner:
    """番茄小说(Fanqie Novel) 请求头签名。"""

    AID = 1967
    PACKAGE = "com.dragon.read.oversea.gp"
    CHANNEL = "googleplay"
    VERSION_NAME = "6.8.1.32"
    VERSION_CODE = 68132
    SDK_VERSION = "3.7.0-rc.25-fanqie-xiaoshuo-opt"
    # lc_id / sdk_ver_code 目前沿用示例值，如与真机抓包不一致请替换
    LC_ID = 0
    SDK_VER_CODE = 0

    @classmethod
    def generate_headers(
        cls,
        params: Union[str, Dict],
        data: Optional[Union[str, Dict, bytes]] = None,
        device_id: Optional[str] = None,
        cookie: Optional[str] = None,
        unix: Optional[int] = None,
    ) -> Dict[str, str]:
        if unix is None:
            unix = int(time.time())

        headers: Dict[str, str] = {
            "x-ss-req-ticket": str(int(time.time() * 1000)),
        }
        if data is not None:
            headers["x-ss-stub"] = generate_stub(data)

        headers.update(XLadon.calculate(unix=unix))
        headers.update(
            XGorgon.calculate(params=params, headers=headers, cookie=cookie, unix=unix)
        )
        headers.update(XArgus.calculate(unix=unix))
        if cookie is not None:
            headers["cookie"] = cookie
        return headers


# ════════════════════════════════════════════════════════════════════
# 8. 设备注册 / key 注册 —— 移植自 api_fix.py，未改动业务逻辑
# ════════════════════════════════════════════════════════════════════

class ImprovedRandomDeviceGenerator:
    DEVICE_BRANDS = {
        "Xiaomi": ["24031PN0DC", "2304FPN6DC", "23078RKD5C", "MI11", "MI12", "RedmiK40"],
        "HUAWEI": ["ELS-AN00", "TAS-AL00", "ANA-AN00", "P50", "Mate50", "nova9"],
        "OPPO": ["CPH2207", "CPH2371", "FindX5", "Reno8", "Reno9"],
        "vivo": ["V2197A", "V2073A", "X80", "X90", "iQOO9"],
        "OnePlus": ["LE2100", "LE2110", "OnePlus9", "OnePlus10"],
        "Samsung": ["SM-G9980", "SM-G9910", "GalaxyS22", "GalaxyS23"],
    }
    ANDROID_VERSIONS = [
        {"version": "12", "api": 32, "release": "V417IR"},
        {"version": "13", "api": 33, "release": "V433IR"},
        {"version": "11", "api": 30, "release": "V394IR"},
        {"version": "14", "api": 34, "release": "V451IR"},
    ]
    RESOLUTIONS = [
        {"resolution": "2400*1080", "density_dpi": 480, "display_density": "xxhdpi"},
        {"resolution": "2340*1080", "density_dpi": 440, "display_density": "xxhdpi"},
        {"resolution": "1920*1080", "density_dpi": 480, "display_density": "xxhdpi"},
    ]
    CPU_ABIS = ["arm64-v8a", "armeabi-v7a"]
    ROM_VERSIONS = ["1417", "1418", "1419", "1420"]

    @staticmethod
    def md5_encode(text: str) -> str:
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    @classmethod
    def generate_android_id(cls) -> str:
        return "".join(random.choices("0123456789abcdef", k=16))

    @classmethod
    def generate_openudid_real(cls, android_id_str: Optional[str] = None) -> str:
        android_id_str = android_id_str or cls.generate_android_id()
        h = cls.md5_encode(android_id_str)
        return (h + cls.md5_encode(h)[:8]).lower()

    @staticmethod
    def generate_device_id() -> str:
        return str(random.randint(1000000000000000, 9999999999999999))

    @classmethod
    def generate_random_device(cls) -> Dict[str, Any]:
        android_id_str = cls.generate_android_id()
        open_udid = cls.generate_openudid_real(android_id_str)
        brand = random.choice(list(cls.DEVICE_BRANDS.keys()))
        model = random.choice(cls.DEVICE_BRANDS[brand])
        android_info = random.choice(cls.ANDROID_VERSIONS)
        screen = random.choice(cls.RESOLUTIONS)
        now_ms = int(time.time() * 1000)
        return {
            "android_id": android_id_str,
            "device_brand": brand,
            "device_manufacturer": brand,
            "device_model": model,
            "device_type": model,
            "os_version": android_info["version"],
            "os_api": android_info["api"],
            "release_build": android_info["release"] + "_20171120",
            "rom_version": f"{android_info['release']}+release-keys",
            "resolution": screen["resolution"],
            "density_dpi": screen["density_dpi"],
            "display_density": screen["display_density"],
            "cpu_abi": random.choice(cls.CPU_ABIS),
            "host_abi": random.choice(cls.CPU_ABIS),
            "rom": random.choice(cls.ROM_VERSIONS),
            "cdid": str(uuid.uuid4()),
            "sig_hash": "".join(random.choices("0123456789abcdef", k=32)),
            "openudid": open_udid,
            "clientudid": str(uuid.uuid4()),
            "ipv6_address": ":".join(f"{random.randint(0, 65535):04X}" for _ in range(8)),
            "device_id": cls.generate_device_id(),
            "install_id": cls.generate_device_id(),
            "req_id": str(uuid.uuid4()),
            "apk_first_install_time": now_ms - random.randint(86400000, 31536000000),
            "_gen_time": now_ms,
            "_rticket": now_ms,
        }


class FanqieDeviceClient:
    """设备注册 + 阅读内容解密 key 申请，逻辑照搬 api_fix.py。"""

    REGISTER_URL = "https://log5-applog.fqnovel.com/service/2/device_register/"
    KEY_URL_FMT = "https://reading.snssdk.com/reading/crypt/registerkey?aid=1967&device_id={device_id}&iid={install_id}"
    MASTER_KEY_HEX = "ac25c67ddd8f38c1b37a2348828e222e"
    KEY_REGISTER_SECRET_B64 = "rCXGfd2POMGzeiNIgo4iLg=="

    APP_INFO = {
        "display_name": "番茄小说",
        "aid": FanqieSigner.AID,
        "channel": FanqieSigner.CHANNEL,
        "package": FanqieSigner.PACKAGE,
        "app_version": FanqieSigner.VERSION_NAME,
        "version_code": FanqieSigner.VERSION_CODE,
        "update_version_code": FanqieSigner.VERSION_CODE,
        "manifest_version_code": FanqieSigner.VERSION_CODE,
        "app_version_minor": FanqieSigner.VERSION_NAME,
        "sdk_version": FanqieSigner.SDK_VERSION,
        "sdk_target_version": 29,
        "git_hash": "5b6a0d3",
        "sdk_flavor": "china",
        "guest_mode": 0,
        "is_system_app": 0,
        "pre_installed_channel": "",
        "not_request_sender": 0,
    }

    def _build_headers(self, dev: Dict[str, Any]) -> Dict[str, str]:
        return {
            "User-Agent": (
                f"com.dragon.read.oversea.gp/68132 (Linux; U; Android {dev['os_version']}; "
                f"zh_CN; {dev['device_model']}; Build/{dev['rom_version'].split('+')[0]};"
                f"tt-ok/3.12.13.4-tiktok)"
            ),
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json",
            "log-encode-type": "gzip",
            "x-ss-req-ticket": str(dev["_gen_time"]),
            "x-vc-bdturing-sdk-version": "3.7.2.cn",
            "Cookie": f"store-region=cn-zj; store-region-src=did; install_id={dev['install_id']}",
        }

    def _build_params(self, dev: Dict[str, Any]) -> Dict[str, str]:
        return {
            "aid": str(self.APP_INFO["aid"]),
            "version_code": str(self.APP_INFO["version_code"]),
            "channel": self.APP_INFO["channel"],
            "package": self.APP_INFO["package"],
            "_rticket": str(dev["_rticket"]),
            "use_store_region_cookie": "1",
            "okhttp_version": "4.2.137.76-fanqie",
        }

    def _build_payload(self, dev: Dict[str, Any]) -> Dict[str, Any]:
        header = {**self.APP_INFO}
        header.update({
            "os": "Android",
            "os_version": dev["os_version"],
            "os_api": dev["os_api"],
            "device_model": dev["device_model"],
            "device_brand": dev["device_brand"],
            "device_manufacturer": dev["device_manufacturer"],
            "cpu_abi": dev["cpu_abi"],
            "release_build": dev["release_build"],
            "density_dpi": dev["density_dpi"],
            "display_density": dev["display_density"],
            "resolution": dev["resolution"].replace("*", "x"),
            "language": "zh",
            "timezone": 8,
            "access": "wifi",
            "rom": dev["rom"],
            "rom_version": dev["rom_version"].replace("+", " "),
            "cdid": dev["cdid"],
            "sig_hash": dev["sig_hash"],
            "openudid": dev["openudid"],
            "clientudid": dev["clientudid"],
            "ipv6_list": [{"type": "client_anpi", "value": dev["ipv6_address"]}],
            "region": "CN",
            "tz_name": "Asia/Shanghai",
            "tz_offset": 28800,
            "sim_serial_number": [],
            "oaid_may_support": False,
            "req_id": dev["req_id"],
            "device_platform": "android",
            "custom": {"host_bit": 64, "account_region": "cn", "dragon_device_type": "phone"},
            "apk_first_install_time": dev["apk_first_install_time"],
        })
        return {"magic_tag": "ss_app_log", "header": header, "_gen_time": dev["_gen_time"]}

    async def register_device(self, session: aiohttp.ClientSession, dev: Dict[str, Any]) -> Dict[str, Any]:
        import json as _json

        headers = self._build_headers(dev)
        params = self._build_params(dev)
        payload = self._build_payload(dev)

        # 注意：这里故意不 gzip。原始 api_fix.py（还能用的版本）用
        # requests 的 json= 直接发明文 JSON，Content-Type: application/json，
        # "log-encode-type: gzip" 这个头看起来只是抄来的、并没有真的要求
        # body 是 gzip——之前我自作主张真做了 gzip 压缩，是我引入的回归。
        body_str = _json.dumps(payload)
        headers["x-ss-stub"] = hashlib.md5(body_str.encode()).hexdigest().upper()

        query_str = urlencode(params)
        ts = int(time.time())
        headers.update(XGorgon.calculate(params=query_str, headers=headers, unix=ts))
        headers.update(XLadon.calculate(unix=ts))
        headers.update(XArgus.calculate(unix=ts))

        async with session.post(self.REGISTER_URL, params=params, headers=headers, data=body_str) as resp:
            raw_text = await resp.text()
            try:
                data = await resp.json(content_type=None)
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(
                    f"[register_device] 响应不是合法 JSON，status={resp.status}, body={raw_text[:300]!r}"
                ) from e
            if not isinstance(data, dict) or "device_id" not in data:
                raise RuntimeError(
                    f"[register_device] 服务端没有正常下发 device_id，status={resp.status}, "
                    f"body={raw_text[:300]!r}（大概率是缺少必要签名头，比如 x-gorgon，"
                    f"或者 payload/aid 跟真机抓包对不上）"
                )
            dev["device_id"] = str(data["device_id"])
            dev["install_id"] = str(data["install_id"])
            print(
                f"[DEBUG][register_device] status={resp.status} "
                f"device_id={dev['device_id']} install_id={dev['install_id']} "
                f"resp_headers={dict(resp.headers)}"
            )
            return dev

    def _decrypt_key(self, encrypted_key: str) -> Optional[bytes]:
        key = bytes.fromhex(self.MASTER_KEY_HEX)
        raw = base64.b64decode(encrypted_key)
        iv, ct = raw[:16], raw[16:]
        cipher = AES.new(key, AES.MODE_CBC, iv)
        plain = cipher.decrypt(ct)
        pad_len = plain[-1]
        if 1 <= pad_len <= 16:
            plain = plain[:-pad_len]
        return plain

    async def key_register(self, session: aiohttp.ClientSession, device_id: str, install_id: str) -> Optional[str]:
        import json as _json

        secret_key = base64.b64decode(self.KEY_REGISTER_SECRET_B64)
        uid_hex = random_uuid().replace("-", "")
        iv = bytes.fromhex(uid_hex[:32])
        data_bytes = bytes.fromhex(r_hex(int(device_id)))
        cipher = AES.new(secret_key, AES.MODE_CBC, iv)
        encrypted = cipher.encrypt(pad(data_bytes, AES.block_size))
        content_b64 = base64.b64encode(iv + encrypted).decode()

        url = self.KEY_URL_FMT.format(device_id=device_id, install_id=install_id)
        query = f"aid=1967&device_id={device_id}&iid={install_id}"
        payload = {"content": content_b64, "keyver": 1}
        # 注意：body 是 json.dumps() 出来的原始 JSON 字符串，Content-Type 却
        # 写的是 application/x-www-form-urlencoded——这个"头跟实际格式对不上"
        # 是原始能跑通的脚本里的真实行为，不是笔误，别把它"修正"成标准表单编码。
        body_str = _json.dumps(payload)
        headers = {
            "User-Agent": "com.dragon.read.oversea.gp/68132 (Linux; U; Android 12; zh_CN; ANA-AN00; Build/V417IR;tt-ok/3.12.13.4-tiktok)",
            "Content-Type": "application/x-www-form-urlencoded",
            "x-ss-req-ticket": str(int(time.time() * 1000)),
            "x-ss-stub": hashlib.md5(body_str.encode()).hexdigest().upper(),
        }
        # 补齐四神：x-ss-stub + x-gorgon/x-khronos + 短签版 x-argus/x-ladon
        # （已用真机抓包验证过短签算法，见 XArgus/XLadon 的注释）。
        ts = int(time.time())
        headers.update(XGorgon.calculate(params=query, headers=headers, unix=ts))
        headers.update(XLadon.calculate(unix=ts))
        headers.update(XArgus.calculate(unix=ts))
        print(f"[DEBUG][key_register] 请求 URL={url}")
        print(f"[DEBUG][key_register] 请求头={headers}")
        print(f"[DEBUG][key_register] 请求体={body_str}")
        async with session.post(url, headers=headers, data=body_str) as resp:
            raw_text = await resp.text()
            print(
                f"[DEBUG][key_register] status={resp.status} "
                f"resp_headers={dict(resp.headers)} body={raw_text[:300]!r}"
            )
            try:
                resp_json = await resp.json(content_type=None)
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(
                    f"[key_register] 响应不是合法 JSON，status={resp.status}, body={raw_text[:300]!r}"
                ) from e
            if not isinstance(resp_json, dict):
                raise RuntimeError(
                    f"[key_register] 服务端返回了 {resp_json!r}（不是 dict），status={resp.status}, "
                    f"body={raw_text[:300]!r}。大概率是缺少必要的签名头（x-argus 等）或者 "
                    f"device_id/install_id 没有真正注册成功，建议先确认 register_device 是否成功。"
                )
            encrypted_key = resp_json.get("data", {}).get("key")
            if not encrypted_key:
                return None
            decrypted = self._decrypt_key(encrypted_key)
            return decrypted.hex().upper() if decrypted else None


# ════════════════════════════════════════════════════════════════════
# 9. 章节内容解密 + 清洗（原样移植自 api_fix.py）
# ════════════════════════════════════════════════════════════════════

_CONTENT_PATTERNS = [
    r'<p class="pictureDesc" group-id="\d+" idx="\d+">',
    r'</body>|</html>|</div>',
    r'<p class="picture" group-id="\d+">',
    r'<div data-fanqie-type="image" source="user">',
    r'<head>.*?</h1>',
    r'<!DOCTYPE.*?<html>',
    r'<\?xml.*?\?>',
    r'<p idx="\d+">',
    r'<header>|</header>',
    r'<article>|</article>',
    r'<footer>|</footer>',
    r'<tt_keyword.*?keyword_ad>',
    r'<p>',
]


def process_content(text_content: str) -> str:
    for pat in _CONTENT_PATTERNS:
        text_content = re.sub(pat, "", text_content, flags=re.DOTALL)
    text_content = re.sub(r"&amp;x", "&x", text_content)
    text_content = re.sub(r"</p>", "\n", text_content, flags=re.DOTALL)
    return text_content.strip()


def decrypt_chapter(encrypted: str, secret_key_hex: str) -> str:
    key_bytes = bytes.fromhex(secret_key_hex)
    raw = base64.b64decode(encrypted)
    iv, ct = raw[:16], raw[16:]
    cipher = AES.new(key_bytes, AES.MODE_CBC, iv)
    plain = unpad(cipher.decrypt(ct), AES.block_size)
    return plain.decode("utf-8", errors="replace")


# ════════════════════════════════════════════════════════════════════
# 10. 批量取章 —— 在 api_fix.py 原有基础上，把请求头换成完整四签名
# ════════════════════════════════════════════════════════════════════

class FanqieBatchClient:
    BATCH_URL = "https://api5-normal-sinfonlineb.fqnovel.com/reading/reader/batch_full/v"

    def __init__(self) -> None:
        self.device: Dict[str, Any] = {}
        self.content_key: Optional[str] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "FanqieBatchClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session:
            await self._session.close()

    async def setup(self) -> None:
        """注册设备 + 申请内容解密 key，只需要跑一次。"""
        assert self._session is not None
        dev_client = FanqieDeviceClient()
        dev = ImprovedRandomDeviceGenerator.generate_random_device()
        self.device = await dev_client.register_device(self._session, dev)
        self.content_key = await dev_client.key_register(
            self._session, self.device["device_id"], self.device["install_id"]
        )
        if not self.content_key:
            raise RuntimeError("key_register 失败，拿不到内容解密 key")

    async def fetch_chapters(self, item_ids: List[str], book_id: str = "0") -> Dict[str, Dict[str, str]]:
        """批量取章节内容，带上完整的 x-gorgon/x-khronos/x-ladon/x-argus。"""
        assert self._session is not None
        if not self.device or not self.content_key:
            await self.setup()

        params = {
            "aid": str(FanqieSigner.AID),
            "app_name": "novelapp",
            "channel": "0",
            "device_platform": "android",
            "device_id": self.device["device_id"],
            "device_type": "Honor10",
            "os_version": "0",
            "version_code": str(FanqieSigner.VERSION_CODE),
            "book_id": book_id,
            "item_ids": ",".join(item_ids),
            "novel_text_type": "1",
            "req_type": "1",
        }

        base_headers = {
            "User-Agent": (
                "com.dragon.read.oversea.gp/68132 (Linux; U; Android 12; zh_CN; "
                "ANA-AN00; Build/V417IR;tt-ok/3.12.13.4-tiktok)"
            )
        }
        sig_headers = FanqieSigner.generate_headers(
            params=params,
            device_id=self.device["device_id"],
        )
        headers = {**base_headers, **sig_headers}

        async with self._session.get(
            self.BATCH_URL, params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            api_resp = await resp.json()

        result: Dict[str, Dict[str, str]] = {}
        for chap_id in item_ids:
            item = api_resp.get("data", {}).get(chap_id)
            if not item:
                continue
            dec = decrypt_chapter(item["content"], self.content_key)
            result[chap_id] = {
                "title": item.get("title", ""),
                "content": process_content(dec),
            }
        return result


# ════════════════════════════════════════════════════════════════════
# 11. aiohttp 服务 + 命令行 demo
# ════════════════════════════════════════════════════════════════════

_batch_client: Optional[FanqieBatchClient] = None


async def _handle_content(request: web.Request) -> web.Response:
    global _batch_client
    item_ids = request.query["item_ids"].split(",")
    book_id = request.query.get("book_id", "0")
    try:
        data = await _batch_client.fetch_chapters(item_ids, book_id=book_id)
        return web.json_response(data, dumps=lambda x: __import__("json").dumps(x, ensure_ascii=False))
    except Exception as e:  # noqa: BLE001
        return web.json_response({"error": str(e)}, status=500)


async def _init_app() -> web.Application:
    global _batch_client
    _batch_client = FanqieBatchClient()
    await _batch_client.__aenter__()
    await _batch_client.setup()
    print("[INFO] 设备注册 + key 申请完成:", _batch_client.device.get("device_id"))

    app = web.Application()
    app.add_routes([web.get("/content", _handle_content)])
    return app


if __name__ == "__main__":
    # 起一个本地服务：GET /content?item_ids=xxx,yyy&book_id=zzz
    web.run_app(_init_app(), port=8080)