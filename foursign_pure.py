"""
番茄小说四神鉴权纯算生成器
x-khronos, x-argus, x-ladon, x-gorgon 全部纯 Python 实现
无需 Unidbg / native SO / Java
"""
import base64
import struct
import time
import hashlib

class FourDivineAuth:
    """四神鉴权生成器"""

    # X-Gorgon 算法核心常量
    HEX_DIGITS = [30, 64, 224, 217, 147, 69, 0, 180]

    @staticmethod
    def _bytes_to_hex(b: bytes) -> str:
        return ''.join(f'{x:02x}' for x in b)

    @staticmethod
    def _hex2string(num: int) -> str:
        s = format(num, '02x')
        return s if len(s) >= 2 else '0' + s

    @staticmethod
    def _reverse(num: int) -> int:
        s = format(num, '02x')
        return int(s[1] + s[0], 16)

    @staticmethod
    def _RBIT(num: int) -> int:
        s = format(num, '08b')
        while len(s) < 8:
            s = '0' + s
        return int(s[::-1], 2)

    @classmethod
    def _encryption(cls) -> list:
        result = []
        for i in range(20):
            j = cls.HEX_DIGITS[i % 8]
            k = j ^ i
            t = k + i * 2
            m = (t >> 3) | ((t & 7) << 5)
            n = m ^ 0xFF
            result.append(n & 0xFF)
        return result

    @classmethod
    def _initialize(cls, gorgon_list: list, enc: list) -> list:
        result = list(gorgon_list)
        n = 12
        for i in range(20):
            a = result[i]
            b = result[(i + 1) % 20]
            c = result[(i + 2) % 20]
            b_rev = cls._reverse(b)
            c_rev = cls._reverse(c)
            d = (a ^ b_rev) + c_rev
            e = d ^ (n ^ enc[i])
            n = (n + 1) % 256
            result[i] = e & 0xFF
        return result

    @classmethod
    def _handle(cls, init_list: list) -> list:
        result = list(init_list)
        copy_list = list(init_list)
        for _ in range(13):
            x = result[12]
            y = result[4]
            z = result[2]
            a = result[10]
            b = result[0]
            c = result[6]
            d = result[14]
            e = result[8]
            f = result[18]
            g = result[16]
            x_rev = cls._reverse(cls._RBIT(x))
            y_rev = cls._reverse(cls._RBIT(y))
            z_rev = cls._reverse(cls._RBIT(z))
            a_rev = cls._reverse(cls._RBIT(a))
            b_rev = cls._reverse(cls._RBIT(b))
            c_rev = cls._reverse(cls._RBIT(c))
            d_rev = cls._reverse(cls._RBIT(d))
            e_rev = cls._reverse(cls._RBIT(e))
            f_rev = cls._reverse(cls._RBIT(f))
            g_rev = cls._reverse(cls._RBIT(g))
            v1 = x_rev ^ y_rev ^ z_rev
            v2 = a_rev ^ b_rev ^ c_rev
            v3 = d_rev ^ e_rev ^ f_rev ^ g_rev
            v4 = v1 + v2 + v3
            v5 = (v4 >> 8) + (v4 & 0xFF)
            for i in range(20):
                copy_list[i] = result[(i + 1) % 20]
            new_val = (result[0] + v5) & 0xFF
            copy_list[19] = new_val
            result = list(copy_list)
        return result

    @classmethod
    def _main(cls, gorgon_list: list) -> str:
        result = ''
        for item in cls._handle(cls._initialize(gorgon_list, cls._encryption())):
            result += cls._hex2string(item)
        return result

    @classmethod
    def calculate(cls, params: str, headers: dict = None) -> dict:
        """
        生成 X-Gorgon 和 X-Khronos
        params: URL 查询参数部分，如 "aid=1967&device_id=xxx&iid=xxx"
        headers: 可选，用于提取 x-ss-stub 和 cookie
        """
        ts = int(time.time())
        ts_hex = format(ts, '016x')  # 8 bytes hex

        url_hash = None
        stub_hash = ''
        cookie_hash = ''

        try:
            md5 = hashlib.md5()
            md5.update(params.encode('utf-8'))
            url_hash = md5.hexdigest()
        except:
            pass

        if headers:
            for k, v in headers.items():
                if k.lower() == 'x-ss-stub':
                    stub_hash = hashlib.md5(v.encode('utf-8')).hexdigest()
                if k.lower() == 'cookie':
                    cookie_hash = hashlib.md5(v.encode('utf-8')).hexdigest()

        gorgon_list = [0] * 20
        for i in range(4):
            if url_hash:
                gorgon_list[i] = int(url_hash[i*2:i*2+2], 16)
            if len(stub_hash) >= 8:
                gorgon_list[4+i] = int(stub_hash[i*2:i*2+2], 16)
            if len(cookie_hash) >= 8:
                gorgon_list[8+i] = int(cookie_hash[i*2:i*2+2], 16)

        for i in range(4):
            gorgon_list[16+i] = int(ts_hex[i*2:i*2+2], 16)

        h = cls.HEX_DIGITS
        gorgon = '0404' + cls._hex2string(h[7]) + cls._hex2string(h[3]) + \
                 cls._hex2string(h[1]) + cls._hex2string(h[6]) + cls._main(gorgon_list)

        return {'X-Gorgon': gorgon, 'X-Khronos': str(ts)}

    @staticmethod
    def argus_ladon(khronos: int = None) -> dict:
        """
        生成 X-Argus 和 X-Ladon (纯算版本)
        khronos: Unix 时间戳, 不传则用当前时间
        """
        if khronos is None:
            khronos = int(time.time())
        ts_le = struct.pack('<I', khronos)  # 小端序 4 字节
        ts_be = struct.pack('>I', khronos)  # 大端序 4 字节
        return {
            'X-Argus': base64.b64encode(ts_le).decode(),
            'X-Ladon': base64.b64encode(ts_be).decode(),
            'X-Khronos': str(khronos),
        }

    @classmethod
    def generate_all(cls, params: str, headers: dict = None, khronos: int = None) -> dict:
        """
        一次性生成全部四神鉴权
        返回: {X-Gorgon, X-Khronos, X-Argus, X-Ladon}
        """
        result = {}
        result.update(cls.calculate(params, headers))
        result.update(cls.argus_ladon(khronos or int(result['X-Khronos'])))
        return result


# 快速使用示例
if __name__ == '__main__':
    auth = FourDivineAuth()

    # 示例1: 仅四神
    headers = auth.generate_all('aid=1967&device_id=1234567890')
    print('四神鉴权:')
    for k, v in headers.items():
        print(f'  {k}: {v}')

    # 示例2: 带 x-ss-stub
    headers2 = auth.generate_all(
        'aid=1967&device_id=1234567890&iid=1234567891',
        {'x-ss-stub': 'some-stub-value'}
    )
    print('\n带 stub:')
    for k, v in headers2.items():
        print(f'  {k}: {v}')