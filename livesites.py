"""虎牙 / 斗鱼 / 快手 直播间的弹幕抓取。

思路和 douyin.py 一样：连浏览器的 DevTools，注入一段脚本把新弹幕推进 `window.__wtQueue`，
Python 每 0.25 秒拉一次。好处是不用逆向各家的 WebSocket 协议，平台改版只要改选择器。

三个平台只差「URL 规则」和「DOM 选择器」两件事，选择器都是实测出来的稳定钩子：
- 虎牙  #chat-room__list / .msg-normal / .name / .msg
- 斗鱼  #js-barrage-list / .Barrage-listItem / .js-nick / .Barrage-content
- 快手  .chat-history / .comment-cell / .username / .comment

注意各平台带 `--hash` 后缀的类名是 CSS Modules 生成的，会随版本变化，不能用。
"""

import asyncio
import collections
import json
import logging
import os
import re
import subprocess
import time

import aiohttp

from douyin import _Cdp, find_browser, norm

log = logging.getLogger("site")

SITES = {
    "huya": {
        "name": "虎牙",
        "url": "https://www.huya.com/{room}",
        "box": "#chat-room__list",
        "item": ".msg-normal",
        "user": ".name",
        "text": ".msg",
        "room_hint": "房间号，比如 660000；也可以直接粘完整网址",
    },
    "douyu": {
        "name": "斗鱼",
        "url": "https://www.douyu.com/{room}",
        "box": "#js-barrage-list",
        "item": ".Barrage-listItem",
        "user": ".js-nick",
        "text": ".Barrage-content",
        "room_hint": "房间号，比如 9999；也可以直接粘完整网址",
    },
    "kuaishou": {
        "name": "快手",
        "url": "https://live.kuaishou.com/u/{room}",
        "box": ".chat-history",
        "item": ".comment-cell",
        "user": ".username",
        "text": ".comment",
        "room_hint": "快手号（不是纯数字 ID），比如 3xs856itqc5ivgm；也可以直接粘完整网址",
    },
}

_JS = """(() => {
  if (window.__wtHook) return;
  window.__wtHook = 1;
  window.__wtQueue = [];
  window.__wtSeen = new Set();
  const SEL = %s;
  const pick = (el, q) => { if (!q) return (el.textContent || '').trim();
    const n = el.querySelector(q); return n ? (n.textContent || '').trim() : ''; };
  function scan() {
    const box = document.querySelector(SEL.box);
    if (!box) return;
    box.querySelectorAll(SEL.item).forEach(el => {
      const name = pick(el, SEL.user).replace(/[：:]\\s*$/, '');
      const text = pick(el, SEL.text).replace(/\\s+/g, ' ').trim();
      if (!text) return;
      const key = name + '|' + text;
      if (window.__wtSeen.has(key)) return;
      window.__wtSeen.add(key);
      if (window.__wtSeen.size > 5000) window.__wtSeen.clear();
      window.__wtQueue.push({name, text, kind: name ? 'danmaku' : 'system', t: Date.now()});
      if (window.__wtQueue.length > 300) window.__wtQueue.splice(0, window.__wtQueue.length - 300);
    });
  }
  setInterval(scan, 300);
  scan();
  window.__wtHealth = () => ({title: document.title, url: location.href,
    ok: !!document.querySelector(SEL.box)});
})();"""


def site_url(site, room):
    room = (room or "").strip()
    if room.startswith("http://") or room.startswith("https://"):
        return room
    room = room.rstrip("/").split("/")[-1]
    return SITES[site]["url"].format(room=room)


class SiteLive:
    """接口和 DouyinLive 对齐：start/stop/connected/room_id/on_event。"""

    def __init__(self, on_event, on_status):
        self.on_event = on_event
        self.on_status = on_status
        self._stop = asyncio.Event()
        self._cdn = None
        self._cdp = None
        self._port = 9334
        self._cfg = {}
        self._site = "huya"
        self._url = ""
        self._launched = 0.0
        self._task = None
        self.connected = False
        self.room_id = ""
        self.login_name = ""
        self.owner_name = ""
        self.browser_info = {}
        self.raw = collections.deque(maxlen=200)

    # ---------- 浏览器 ----------
    async def _targets(self):
        async with aiohttp.ClientSession(trust_env=False) as s:
            async with s.get(f"http://127.0.0.1:{self._port}/json/list",
                             timeout=aiohttp.ClientTimeout(total=3)) as r:
                return await r.json(content_type=None)

    async def _ensure_browser(self, url):
        try:
            await self._targets()
            return
        except Exception:
            pass
        if time.time() - self._launched < 25:
            raise RuntimeError("浏览器正在启动，稍等…")
        exe = norm(self._cfg.get("browser_exe")) or find_browser()
        if not exe:
            raise RuntimeError("找不到 Edge / Chrome，请在【直播间 / 登录】里手动选择浏览器")
        profile = norm(self._cfg.get("profile_dir")) or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "site_profile")
        os.makedirs(profile, exist_ok=True)
        args = [exe, f"--remote-debugging-port={self._port}", f"--user-data-dir={profile}",
                "--no-first-run", "--no-default-browser-check", "--disable-popup-blocking",
                "--disable-blink-features=AutomationControlled", "--mute-audio", f"--app={url}"]
        if self._cfg.get("headless"):
            args.insert(1, "--headless=new")
        try:
            subprocess.Popen(args, close_fds=True)
            self._launched = time.time()
        except OSError as e:
            raise RuntimeError(f"启动浏览器失败：{e}")
        for _ in range(150):
            if self._stop.is_set():
                return
            await asyncio.sleep(0.2)
            try:
                await self._targets()
                return
            except Exception:
                continue
        raise RuntimeError("浏览器没有在 30 秒内打开调试端口，可能被杀毒软件拦了")

    async def _attach(self, url):
        targets = await self._targets()
        pages = [t for t in targets if t.get("type") == "page" and t.get("webSocketDebuggerUrl")]
        host = url.split("//", 1)[-1].split("/")[0]
        chosen = next((t for t in pages if host and host in (t.get("url") or "")), None)
        if not chosen:
            chosen = next((t for t in pages if "chat" not in (t.get("url") or "")), None)
        s = aiohttp.ClientSession(trust_env=False)
        self._cdn = s
        if not chosen:
            try:
                async with s.put(f"http://127.0.0.1:{self._port}/json/new?{url}") as r:
                    chosen = await r.json(content_type=None)
            except Exception:
                async with s.get(f"http://127.0.0.1:{self._port}/json/new?{url}") as r:
                    chosen = await r.json(content_type=None)
        if not chosen or not chosen.get("webSocketDebuggerUrl"):
            raise RuntimeError("浏览器里找不到可用的页面")
        cdp = _Cdp(chosen["webSocketDebuggerUrl"], s)
        await cdp.connect()
        await cdp.send("Runtime.enable")
        await cdp.send("Page.enable")
        js = _JS % json.dumps(SITES[self._site])
        try:
            await cdp.send("Page.addScriptToEvaluateOnNewDocument", source=js)
        except Exception:
            pass
        if host and host not in (chosen.get("url") or ""):
            await cdp.send("Page.navigate", url=url)
            await asyncio.sleep(2)
        await cdp.js(js)
        self._cdp = cdp
        self.browser_info = {"url": url, "port": self._port}

    # ---------- 生命周期 ----------
    async def start(self, cfg):
        self._cfg = cfg or {}
        self._site = (self._cfg.get("platform") or "huya").lower()
        if self._site not in SITES:
            raise RuntimeError(f"不支持的平台：{self._site}")
        self._port = int(self._cfg.get("port") or 9334)
        room = (self._cfg.get("room") or "").strip()
        if not room:
            raise RuntimeError(f"还没填{SITES[self._site]['name']}房间号")
        self._url = site_url(self._site, room)
        self._stop.clear()
        self.connected = False
        self.on_status(f"正在打开{SITES[self._site]['name']}直播间…")
        await self._ensure_browser(self._url)
        await self._attach(self._url)
        self._task = asyncio.create_task(self._run())
        return True

    async def _run(self):
        try:
            await self._poll()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("直播抓取中断：%s", e)
            self.connected = False
            self.on_status(f"{SITES[self._site]['name']}抓取中断：{e}")
        finally:
            self.connected = False

    async def _poll(self):
        miss = 0
        while not self._stop.is_set():
            await asyncio.sleep(0.25)
            cdp = self._cdp
            if not cdp or cdp.closed:
                raise RuntimeError("浏览器页面已关闭")
            try:
                raw = await cdp.js("JSON.stringify((window.__wtQueue||[]).splice(0,200))")
            except Exception as e:
                miss += 1
                if miss >= 3:
                    raise RuntimeError(str(e) or "抓取脚本失效，可能页面被刷新")
                continue
            miss = 0
            for item in json.loads(raw or "[]"):
                self._handle(item)
            try:
                health = json.loads(await cdp.js("JSON.stringify((window.__wtHealth||(()=>({})))())") or "{}")
                if health:
                    self.browser_info.update(health)
                    if not self.connected and health.get("ok"):
                        self._ready(health)
            except Exception:
                pass

    def _ready(self, health):
        self.connected = True
        self._detect_owner(health.get("title") or "")
        self.room_id = self.owner_name or (self._cfg.get("room") or "")
        self.login_name = "网页抓取"
        where = f"{self.owner_name}的直播间" if self.owner_name else f"{SITES[self._site]['name']}直播间"
        self.on_status(f"已连接{where}（浏览器抓取）")
        if self._site == "kuaishou":
            self.on_status("快手未登录时可能只看到部分弹幕，建议在这个浏览器窗口里登录一次")

    def _detect_owner(self, title):
        if self.owner_name:
            return
        m = re.match(r"^(.+?)(?:的直播间|正在直播|直播中|[-_—|])", (title or "").strip())
        if m:
            name = m.group(1).strip()
            if 1 < len(name) <= 24:
                self.owner_name = name

    def _handle(self, item):
        kind = item.get("kind") or "danmaku"
        name = norm(item.get("name"))[:24]
        text = norm(item.get("text"))[:200]
        if not text:
            return
        self.raw.appendleft({"t": time.strftime("%H:%M:%S"), "text": text,
                             "kind": kind if name else "系统"})
        if kind != "danmaku" or not name:
            return  # 系统通知、进场提示之类不触发回复
        if self.owner_name and name == self.owner_name:
            return  # 主播自己说的不回
        self.on_event({"type": "danmaku", "uid": abs(hash(name)) % 100000000,
                       "name": name, "text": text, "platform": self._site})

    async def stop(self, keep=None):
        self._stop.set()
        self.connected = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None
        if self._cdp:
            try:
                await self._cdp.close()
            except Exception:
                pass
            self._cdp = None
        if self._cdn and not keep:
            try:
                await self._cdn.close()
            except Exception:
                pass
            self._cdn = None
