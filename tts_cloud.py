"""第三方在线语音合成。

统一出口：`synth(cfg_tts, text) -> (pcm int16 ndarray (n, ch), sample_rate)`，
和本地 GsvEngine.synth 的返回格式完全一致，pipeline 不用区分。

支持的供应商按「协议族」分，不按品牌分，这样以后加同类服务只要填 base_url 就行：

- openai       OpenAI /v1/audio/speech 协议，硅基流动、各类中转、vLLM-TTS 都兼容
- azure        Azure 语音服务（SSML）
- minimax      MiniMax t2a_v2（返回 hex 音频）
- elevenlabs   ElevenLabs
- fish         Fish Audio
- custom       自定义：自己填 URL / 请求头 / 带 {text} 的 body 模板
"""

import asyncio
import io
import json
import logging

import aiohttp
import numpy as np

log = logging.getLogger("tts")

TIMEOUT = aiohttp.ClientTimeout(total=60)

PRESETS = {
    "openai": {"base_url": "https://api.openai.com/v1", "model": "tts-1", "voice": "alloy",
               "hint": "OpenAI 官方或任何兼容 /v1/audio/speech 的服务（硅基流动填 https://api.siliconflow.cn/v1）"},
    "azure": {"base_url": "", "model": "", "voice": "zh-CN-XiaoxiaoNeural",
              "hint": "区域填在 base_url，如 https://eastasia.tts.speech.microsoft.com"},
    "minimax": {"base_url": "https://api.minimax.chat/v1", "model": "speech-01-turbo", "voice": "female-shaonv",
                "hint": "GroupId 填在「额外参数」里，格式 {\"group_id\":\"xxx\"}"},
    "elevenlabs": {"base_url": "https://api.elevenlabs.io/v1", "model": "eleven_multilingual_v2", "voice": "",
                   "hint": "voice 填 Voice ID（不是名字）"},
    "fish": {"base_url": "https://api.fish.audio/v1", "model": "s1", "voice": "",
             "hint": "voice 填参考音频的 reference_id"},
    "custom": {"base_url": "", "model": "", "voice": "",
               "hint": "body 模板里用 {text} 占位，例如 {\"text\":\"{text}\"}"},
}


def _extra(c):
    raw = (c.get("extra") or "").strip()
    if not raw:
        return {}
    try:
        v = json.loads(raw)
        return v if isinstance(v, dict) else {}
    except ValueError:
        return {}


def _decode_audio(data):
    """把 mp3/wav/ogg 解成 int16 PCM。云服务基本都返回这几种。"""
    if not data:
        raise RuntimeError("接口返回了空音频")
    import miniaudio
    try:
        d = miniaudio.decode(data, output_format=miniaudio.SampleFormat.SIGNED16)
    except Exception as e:
        raise RuntimeError(f"音频解码失败（可能不是 mp3/wav）：{e}") from e
    pcm = np.frombuffer(d.samples, dtype=np.int16).reshape(-1, d.nchannels)
    if pcm.size == 0:
        raise RuntimeError("解码后没有采样数据")
    return pcm, int(d.sample_rate)


async def _post(session, url, headers=None, json_body=None, data=None, label=""):
    try:
        async with session.post(url, headers=headers, json=json_body, data=data, timeout=TIMEOUT) as r:
            body = await r.read()
            if r.status != 200:
                snippet = body[:300].decode("utf-8", "replace")
                raise RuntimeError(f"{label or '接口'}返回 HTTP {r.status}：{snippet}")
            return body, r.headers.get("Content-Type", "")
    except asyncio.TimeoutError:
        raise RuntimeError(f"{label or '接口'}超时（{int(TIMEOUT.total)} 秒）")
    except aiohttp.ClientError as e:
        raise RuntimeError(f"{label or '接口'}连不上：{e}")


async def _synth_openai(session, c, text):
    base = (c.get("base_url") or PRESETS["openai"]["base_url"]).rstrip("/")
    key = (c.get("api_key") or "").strip()
    if not key:
        raise RuntimeError("还没有填 API Key")
    body = {"model": c.get("model") or "tts-1", "input": text,
            "voice": c.get("voice") or "alloy", "response_format": "mp3",
            "speed": float(c.get("speed") or 1.0)}
    body.update(_extra(c))
    data, _ = await _post(session, f"{base}/audio/speech",
                          {"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                          json_body=body, label="在线语音")
    return data


async def _synth_azure(session, c, text):
    base = (c.get("base_url") or "").rstrip("/")
    key = (c.get("api_key") or "").strip()
    if not base or not key:
        raise RuntimeError("Azure 需要填区域地址和 API Key")
    voice = c.get("voice") or "zh-CN-XiaoxiaoNeural"
    lang = "-".join(voice.split("-")[:2]) if "-" in voice else "zh-CN"
    ssml = (f"<speak version='1.0' xml:lang='{lang}'>"
            f"<voice name='{voice}'><prosody rate='{_rate(c)}'>{_xml(text)}</prosody></voice></speak>")
    data, _ = await _post(session, f"{base}/cognitiveservices/v1",
                          {"Ocp-Apim-Subscription-Key": key,
                           "Content-Type": "application/ssml+xml",
                           "X-Microsoft-OutputFormat": "audio-24khz-48kbitrate-mono-mp3"},
                          data=ssml.encode("utf-8"), label="Azure 语音")
    return data


async def _synth_minimax(session, c, text):
    base = (c.get("base_url") or PRESETS["minimax"]["base_url"]).rstrip("/")
    key = (c.get("api_key") or "").strip()
    if not key:
        raise RuntimeError("还没有填 API Key")
    extra = _extra(c)
    gid = extra.pop("group_id", "") or extra.pop("GroupId", "")
    url = f"{base}/t2a_v2" + (f"?GroupId={gid}" if gid else "")
    body = {"model": c.get("model") or "speech-01-turbo", "text": text,
            "voice_setting": {"voice_id": c.get("voice") or "female-shaonv",
                              "speed": float(c.get("speed") or 1.0)},
            "audio_setting": {"format": "mp3", "sample_rate": 32000}}
    body.update(extra)
    raw, _ = await _post(session, url, {"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                         json_body=body, label="MiniMax 语音")
    try:
        d = json.loads(raw)
    except ValueError:
        raise RuntimeError(f"MiniMax 返回的不是 JSON：{raw[:200]!r}")
    audio = ((d.get("data") or {}).get("audio")) or ""
    if not audio:
        msg = ((d.get("base_resp") or {}).get("status_msg")) or "没有返回音频"
        raise RuntimeError(f"MiniMax 合成失败：{msg}")
    return bytes.fromhex(audio)


async def _synth_elevenlabs(session, c, text):
    base = (c.get("base_url") or PRESETS["elevenlabs"]["base_url"]).rstrip("/")
    key = (c.get("api_key") or "").strip()
    voice = (c.get("voice") or "").strip()
    if not key or not voice:
        raise RuntimeError("ElevenLabs 需要 API Key 和 Voice ID")
    body = {"text": text, "model_id": c.get("model") or "eleven_multilingual_v2",
            "voice_settings": {"stability": 0.5, "similarity_boost": 0.75}}
    body.update(_extra(c))
    data, _ = await _post(session, f"{base}/text-to-speech/{voice}",
                          {"xi-api-key": key, "Content-Type": "application/json"},
                          json_body=body, label="ElevenLabs")
    return data


async def _synth_fish(session, c, text):
    base = (c.get("base_url") or PRESETS["fish"]["base_url"]).rstrip("/")
    key = (c.get("api_key") or "").strip()
    body = {"text": text, "format": "mp3", "mp3_bitrate": 128,
            "reference_id": c.get("voice") or "", "model": c.get("model") or "s1"}
    body.update(_extra(c))
    data, _ = await _post(session, f"{base}/tts",
                          {"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                          json_body=body, label="Fish Audio")
    return data


async def _synth_custom(session, c, text):
    url = (c.get("base_url") or "").strip()
    if not url:
        raise RuntimeError("自定义接口要先填 URL")
    tpl = (c.get("body_tpl") or '{"text":"{text}"}').replace("{text}", json.dumps(text, ensure_ascii=False)[1:-1])
    headers = {"Content-Type": "application/json"}
    key = (c.get("api_key") or "").strip()
    if key:
        headers["Authorization"] = f"Bearer {key}"
    headers.update(_extra(c))
    body = tpl.encode("utf-8")
    data, _ = await _post(session, url, headers, data=body, label="自定义语音接口")
    return data


def _rate(c):
    try:
        v = float(c.get("speed") or 1.0)
    except (TypeError, ValueError):
        v = 1.0
    v = max(0.5, min(2.0, v))
    return f"{'+' if v >= 1 else '-'}{abs(round((v - 1) * 100))}%"


def _xml(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;").replace("'", "&apos;"))


_HANDLERS = {
    "openai": _synth_openai,
    "azure": _synth_azure,
    "minimax": _synth_minimax,
    "elevenlabs": _synth_elevenlabs,
    "fish": _synth_fish,
    "custom": _synth_custom,
}


def vendor_of(cfg):
    return ((cfg.get("cloud") or {}).get("vendor") or "openai").lower()


async def synth(cfg, text):
    c = cfg.get("cloud") or {}
    vendor = vendor_of(cfg)
    fn = _HANDLERS.get(vendor)
    if not fn:
        raise RuntimeError(f"不认识的在线语音供应商：{vendor}")
    if not (text or "").strip():
        raise RuntimeError("要合成的文本是空的")
    async with aiohttp.ClientSession(trust_env=False) as s:
        data = await fn(s, c, text)
    pcm, sr = _decode_audio(data)
    scale = float(cfg.get("speed") or 1.0)
    if abs(scale - 1.0) > 0.02 and vendor in ("openai", "minimax", "elevenlabs", "fish"):
        pass  # 这些供应商已经按 speed 合成了，不再二次变速
    return pcm, sr


async def probe(cfg, text="你好，这是一次语音测试。"):
    """界面上的「测试在线语音」，返回合成耗时和时长。"""
    import time
    t0 = time.time()
    pcm, sr = await synth(cfg, text)
    return {"seconds": round(time.time() - t0, 2), "dur": round(len(pcm) / sr, 2), "sr": sr}
