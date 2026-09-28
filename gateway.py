#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股本地数据网关（零第三方依赖，纯 stdlib）
==========================================
用途：把 a-share-agent 前端目前不稳的直接抓免费源，改为经本网关「缓存 + 重试 + 统一出口」，
     解决深网 CORS/限流/ERR 问题。接口设计成 Provider 可插拔：
     —— 现阶段 proxy_fetch 直连免费源（新浪/东财/腾讯）；
     —— 将来你开通 QMT(miniQMT/xtquant) 后，在此页实现 xtquant Provider 替换 data 层，前端不用改。

运行：
    python gateway.py [端口]         默认 0.0.0.0:8126
前端访问：绝对地址如
    http://127.0.0.1:8126/proxy?u=<urlencoded 上游URL>&ttl=8
返回上游原始字节（JSON / JSONP 脚本均可），带 Access-Control-Allow-Origin:*，带 GET 级缓存。
"""
import json, time, sys, threading, urllib.parse, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8126

# 允许转发的上游 host 白名单（防 SSRF，只放本项目用到的源）
ALLOWED_HOSTS = {
    'vip.stock.finance.sina.com.cn',   # 新浪全市场快照 / 资金流
    'quotes.sina.cn',                  # 新浪日K线 (替代被WAF封的腾讯)
    'hq.sinajs.cn',                    # 新浪实时行情
    'push2.eastmoney.com',             # 东财行情(备用)
    'push2ex.eastmoney.com',           # 东财涨停池(JSONP)
    'web.ifzq.gtimg.cn',               # 腾讯K线(已被WAF封，保留作备用)
}

# 上游并发信号量：限制同时发出的上游请求数，防止短时爆量触发各源(WAF/限流)封禁本机IP
UPSTREAM_SEM = threading.BoundedSemaphore(4)

CACHE = {}
CACHE_LOCK = threading.Lock()
CACHE_MAX = 4000  # 缓存条目上限，防内存膨胀

def proxy_fetch(u, ttl=8, timeout=12, retries=2):
    """带缓存 + 重试的上游抓取。u 必须是完整 URL。"""
    p = urllib.parse.urlparse(u)
    if p.scheme not in ('http', 'https') or p.hostname not in ALLOWED_HOSTS:
        raise PermissionError('host not allowed: ' + str(p.hostname))
    h = 'G:' + u
    with CACHE_LOCK:
        c = CACHE.get(h)
        if c and time.time() - c[0] < ttl:
            return c[1]
    last = None
    for i in range(retries + 1):
        with UPSTREAM_SEM:  # 限量并发，避免爆量被封
            try:
                req = urllib.request.Request(u, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                    'Referer': 'https://finance.sina.com.cn/',
                    'Accept': '*/*',
                })
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    body = r.read()
                got = True
            except Exception as e:
                body, got, last = None, False, e
        if got:
            with CACHE_LOCK:
                CACHE[h] = (time.time(), body)
                if len(CACHE) > CACHE_MAX:
                    # 清理最旧的 1/4
                    for k in list(CACHE)[:CACHE_MAX // 4]:
                        CACHE.pop(k, None)
            return body
        last = e
        time.sleep(0.4 * (i + 1))
    raise ConnectionError('upstream fail(%s): %s' % (p.hostname, last))


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def _send(self, code, body, ct='application/json'):
        if isinstance(body, str):
            body = body.encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', ct)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,OPTIONS')
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        try:
            self.wfile.write(body)
        except BrokenPipeError:
            pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_GET(self):
        try:
            p = urllib.parse.urlparse(self.path)
            if p.path == '/health':
                self._send(200, 'ok', 'text/plain'); return
            if p.path == '/proxy':
                q = urllib.parse.parse_qs(p.query)
                u = q.get('u', [''])[0]
                if not u:
                    self._send(400, 'missing u'); return
                try:
                    ttl = int(q.get('ttl', ['8'])[0] or 8)
                except ValueError:
                    ttl = 8
                ttl = max(0, min(ttl, 60))
                body = proxy_fetch(u, ttl)
                # 若上游是 JSONP（URL 含 cb= 回调参数），用 JS 类型返回，否则浏览器跨域 <script> 会因
                # MIME 类型拦截(application/json 非可执行脚本)而拒绝执行。普通 JSON 仍走 application/json。
                ct = 'application/javascript; charset=utf-8' if ('cb=' in u) else 'application/json'
                self._send(200, body, ct); return
            self._send(404, 'not found: ' + p.path, 'text/plain')
        except PermissionError as e:
            self._send(403, str(e), 'text/plain')
        except Exception as e:
            self._send(502, 'err: ' + str(e), 'text/plain')

    def log_message(self, *args):
        pass


def main():
    srv = ThreadingHTTPServer(('0.0.0.0', PORT), Handler)
    print('A股数据网关已启动  http://127.0.0.1:%d' % PORT, flush=True)
    print('允许上游:' , ','.join(sorted(ALLOWED_HOSTS)), flush=True)
    print('测试: curl "http://127.0.0.1:%d/health"' % PORT, flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\n已停止。')


if __name__ == '__main__':
    main()