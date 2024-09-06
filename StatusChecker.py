import requests
import time
import hmac
import hashlib
import base64
import json


class OrderStatusChecker:
    def __init__(self, api_key, api_secret, passphrase):
        self.base_url = "https://www.okx.com"
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase

    def get_order_status(self, inst_id, cl_ord_id):
        """
        查询订单状态，使用 cl_ord_id 查询订单信息

        参数:
        inst_id (str): 产品ID，如 "BTC-USDT"
        cl_ord_id (str): 客户自定义订单ID

        返回:
        dict: 订单的详细状态信息
        """
        url = f"{self.base_url}/api/v5/trade/order"
        headers = self._generate_headers()

        params = {
            "instId": inst_id,
            "clOrdId": cl_ord_id
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()  # 确保 HTTP 请求成功
            data = response.json()

            if data.get("code") == "0":
                order_data = data.get("data", [])
                if order_data:
                    # 假设订单信息在列表中第一个元素
                    return order_data[0].get("state")
                else:
                    print(f"未找到订单 {cl_ord_id} 的状态信息。")
            else:
                print(f"查询失败，错误信息: {data.get('msg')}")
        except requests.RequestException as e:
            print(f"HTTP请求失败，错误信息: {e}")

        return None  # 如果查询失败或没有找到信息，返回 None

    def _generate_headers(self):
        """
        生成请求头，包括API认证所需的headers

        返回:
        dict: 包含认证信息的headers
        """
        timestamp = str(int(time.time()))
        request_path = "/api/v5/trade/order"
        method = "GET"

        body = ""  # GET 请求没有请求体
        sign = self._generate_signature(timestamp, method, request_path, body)

        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }

        return headers

    def _generate_signature(self, timestamp, method, request_path, body):
        """
        生成 HMAC-SHA256 签名

        参数:
        timestamp (str): 当前时间戳
        method (str): 请求方法，如 GET 或 POST
        request_path (str): 请求路径
        body (str): 请求体

        返回:
        str: 生成的签名
        """
        message = f"{timestamp}{method.upper()}{request_path}{body}"
        mac = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode()
