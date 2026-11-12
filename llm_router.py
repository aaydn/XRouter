# -*- coding: utf-8 -*-
"""
XRouter·川流 (XRouter) v1.0.6 — 本地 LLM 多级分流枢纽
版权：© 2026 XRouter·川流 (XRouter) contributors · MIT License

LLM 多级分流网关：
  L1 免费层（按序尝试，任一失败/超时自动下一个）：
     1) 智谱 glm-4.7-flash      （免费，ZHIPU_FREE_KEY）
     2) 硅基流动 Qwen2.5-7B 等   （9B 以下永久免费，SILICONFLOW_KEY）
  L2 局域网快线：deepseek-v4.1-flash（团队网关）
  L3 局域网旗舰：glm-5.3（团队网关，复杂任务）

策略（以「任务完成速度」为目标）：
  1. 带 tools 的请求一律走 L2 deepseek —— 小模型返工比等待更费时间
  2. 会话超过 40 条消息（≈20 轮，说明任务在打转）→ 升级 L3 glm-5.3，用更强模型一次做对
  3. 极短闲聊（≤400 字符且无 tools）→ 免费池
  4. 带图片 → 直走 L2（唯一视觉模型）
  5. 显式指定真实模型名 → 直通
  API Key 原样透传团队网关；免费层 key 全部走环境变量。

用法：
  python llm_router.py            # 127.0.0.1:3100
  python llm_router.py 0.0.0.0 3100
"""
import asyncio
import hashlib
import io
import json
import os
import re
import socket
import sys
import time
from datetime import date, datetime
from typing import AsyncIterator

# pythonw 启动时没有控制台，sys.stdout 为 None，print 会崩；这里兜底写到文件
if sys.stdout is None or sys.stderr is None:
    _out = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "router-console.log"),
                "a", encoding="utf-8", buffering=1)
    sys.stdout = sys.stdout or _out
    sys.stderr = sys.stderr or _out

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse

# ---------- 品牌 / 版本 ----------
__version__ = "1.0.6"
APP_NAME = "XRouter·川流"
APP_NAME_EN = "XRouter"
APP_AUTHOR = "XRouter·川流 (XRouter) contributors"
COPYRIGHT = f"© 2026 {APP_NAME} (XRouter) contributors · MIT License"
DEV_HOURS = 34.0   # 累计实际开发耗时（小时，支持小数）——人工记账：各开发会话按实际时长累加，合计不足 1 小时按 1 小时计


def _dev_stats() -> tuple:
    """Vibecoding 开发统计：累计实际开发耗时（人工记账）+ 源码行数（实时统计本文件）。"""
    try:
        lines = sum(1 for _ in io.open(os.path.abspath(__file__), "r", encoding="utf-8"))
    except Exception:
        lines = 0
    return DEV_HOURS, lines

# ---------- 配置 ----------
UPSTREAM = "http://127.0.0.1:3000"             # 默认上游（透传模式；首次启动向导/设置页可改）
FAST_MODEL = "deepseek-v4.1-flash"             # L2 简单
SMART_MODEL = "glm-5.3"                        # L3 复杂
FREE_TTFB_TIMEOUT = 0.8                        # 首字节超时（秒）。健康源 TTFT 0.36-0.77s，
                                               # 0.8s 卡在实测上界，慢源立即放弃，不再白等 2s
FREE_CONNECT_TIMEOUT = 1.5                     # 连接超时（公网）
FREE_READ_TIMEOUT = 60.0                       # 首字节后的流读超时（模型可中途停顿）
UPSTREAM_CONNECT_TIMEOUT = 2.0                 # 局域网网关连接超时
FREE_COOLDOWN_SEC = 60                         # 熔断冷却：某源失败/超时后 60s 内不再尝试
FREE_POOL_ENABLED = os.environ.get("FREE_POOL_ENABLED", "1") != "0"  # 0=延迟优先，全走 L2
HEDGE_ENABLED = os.environ.get("HEDGE_ENABLED", "1") != "0"          # 1=免费池与L2并发竞速
TOOLS_FREE_MAX_CHARS = int(os.environ.get("TOOLS_FREE_MAX_CHARS", "32000"))  # 带tools请求可走免费池的上下文上限
# 主分流模式：flash=flash 优先（默认）；glm=GLM-5.3 优先；cache=按有无历史估缓存命中
ROUTE_MODE = os.environ.get("ROUTE_MODE", "glm").strip().lower()
# 网关并发上限：超过就排队，避免把网关数据库打爆（New API 报 Database error）
MAX_CONCURRENT = int(os.environ.get("ROUTER_MAX_CONCURRENT", "8"))

# ---------- config.json（发行版配置，首次启动向导生成） ----------
# 双上游：flash/强模型各自 base_url + api_key + model，key 由 router 保存注入；
# api_key 留空 = 透传客户端 Authorization（团队网关用法，key 在 CodeBuddy 侧配置）。
# base_url 约定填到版本段（如 https://api.deepseek.com/v1），router 自动拼 /chat/completions。
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0]) or "."),
                           "config.json") if not getattr(sys, "frozen", False) \
    else os.path.join(os.path.dirname(sys.executable), "config.json")
HAS_CONFIG = os.path.exists(CONFIG_FILE)
NEED_SETUP = not HAS_CONFIG                                    # 无配置 → 首次启动向导


def _norm_upstream(d: dict, default_base: str, default_model: str) -> dict:
    """归一化单个上游配置，推导 chat completions 完整 URL。"""
    base = (d.get("base_url") or default_base).strip().rstrip("/")
    key = (d.get("api_key") or "").strip()
    model = (d.get("model") or default_model).strip()
    path = "/chat/completions" if re.search(r"/v\d+$", base) else "/v1/chat/completions"
    extra = d.get("extra") if isinstance(d.get("extra"), dict) else {}
    return {"base": base, "key": key, "model": model, "path": path, "extra": extra}


def _apply_extra(body: dict, extra) -> None:
    """把配置的 OpenAI 兼容参数合并进请求体（配置了即覆盖；保留核心字段）。"""
    if not isinstance(extra, dict):
        return
    for k, v in extra.items():
        if k in ("model", "stream", "messages", "stream_options"):
            continue
        if v is None or v == "":
            continue
        try:
            body[k] = v
        except Exception:
            pass


def load_config():
    """读 config.json 覆盖全局配置（保存向导后热生效，无需重启）。"""
    global CFG, FAST_MODEL, SMART_MODEL, ROUTE_MODE, FREE_POOL_ENABLED, HEDGE_ENABLED
    global TOOLS_FREE_MAX_CHARS, MAX_CONCURRENT, HAS_CONFIG, NEED_SETUP
    global STUCK_DEPTH_IMAGE, STUCK_DEPTH_TEXT, TOOL_SIG_REPEAT, PROMPT_TOKEN_THRESHOLD
    global SHORT_MIN_CHARS, SHORT_MAX_TOKENS, FREE_POOL_MAX_CHARS, FAILOVER_CHAIN
    if not os.path.exists(CONFIG_FILE):
        HAS_CONFIG, NEED_SETUP = False, True
        return
    try:
        with io.open(CONFIG_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        log(f"config.json load fail: {e}（沿用当前配置）")
        return
    CFG["flash"] = _norm_upstream(raw.get("flash") or {}, UPSTREAM, FAST_MODEL)
    CFG["smart"] = _norm_upstream(raw.get("smart") or {}, UPSTREAM, SMART_MODEL)
    FAST_MODEL = CFG["flash"]["model"]
    SMART_MODEL = CFG["smart"]["model"]
    if raw.get("route_mode"):
        ROUTE_MODE = str(raw["route_mode"]).strip().lower()
    if "free_pool_enabled" in raw:
        FREE_POOL_ENABLED = bool(raw["free_pool_enabled"])
    # 免费池各平台 key：config 覆盖 env；平台禁用则清空该平台所有源的 key
    provs = (raw.get("free_pool") or {}).get("providers") or {}
    for p in FREE_PROVIDERS:
        pc = provs.get(p["id"]) or {}
        en = pc.get("enabled", True) is not False
        k = (pc.get("key") or "").strip()
        ex = pc.get("extra") if isinstance(pc.get("extra"), dict) else {}
        FREE_PROV_CFG[p["id"]] = {"enabled": en, "key": k, "extra": ex}
        for src in FREE_POOL:
            if src.get("provider") == p["id"]:
                src["key"] = "" if not en else (k or src.get("key_env", ""))
    # 用户自定义免费平台（OpenAI 兼容端点）：以配置为准重建，支持设置页增删
    global CUSTOM_FREE_SOURCES
    CUSTOM_FREE_SOURCES = []
    for pid, pc in provs.items():
        if not str(pid).startswith("custom_") or not isinstance(pc, dict):
            continue
        base = (pc.get("base") or "").strip().rstrip("/")
        models = [m.strip() for m in str(pc.get("models") or "").split(",") if m.strip()]
        if not base or not models:
            continue
        en = pc.get("enabled", True) is not False
        name = (pc.get("name") or pid).strip()
        for i, m in enumerate(models):
            CUSTOM_FREE_SOURCES.append({
                "provider": pid, "name": f"{name}·{m}" if len(models) > 1 else name,
                "base": base, "model": m, "key_env": "",
                "key": "" if not en else (pc.get("key") or "").strip(),
                "tools": pc.get("tools", True) is not False, "custom": True})
    rebuild_free_pool()
    if "hedge_enabled" in raw:
        HEDGE_ENABLED = bool(raw["hedge_enabled"])
    if "tools_free_max_chars" in raw:
        TOOLS_FREE_MAX_CHARS = int(raw["tools_free_max_chars"] or 32000)
    if "router_max_concurrent" in raw:
        MAX_CONCURRENT = int(raw["router_max_concurrent"] or 8)
    # 阈值（升级判定 / 过短回答观察 / 免费池长度），越界自动收敛到安全范围
    th = raw.get("thresholds") or {}

    def _ti(key, cur, lo, hi):
        try:
            return max(lo, min(hi, int(th.get(key, cur))))
        except Exception:
            return cur

    STUCK_DEPTH_IMAGE = _ti("upgrade_depth_image", STUCK_DEPTH_IMAGE, 4, 300)
    STUCK_DEPTH_TEXT = _ti("upgrade_depth_text", STUCK_DEPTH_TEXT, 4, 400)
    TOOL_SIG_REPEAT = _ti("upgrade_tool_repeat", TOOL_SIG_REPEAT, 2, 30)
    PROMPT_TOKEN_THRESHOLD = _ti("upgrade_prompt_chars", PROMPT_TOKEN_THRESHOLD, 10000, 5000000)
    SHORT_MIN_CHARS = _ti("short_min_chars", SHORT_MIN_CHARS, 1000, 1000000)
    SHORT_MAX_TOKENS = _ti("short_max_tokens", SHORT_MAX_TOKENS, 10, 5000)
    FREE_POOL_MAX_CHARS = _ti("free_pool_max_chars", FREE_POOL_MAX_CHARS, 0, 100000)
    # 模型调用顺序（L2/L3 兜底链）：["flash","smart"] 或 ["smart","flash"]
    fc = raw.get("failover_chain")
    if isinstance(fc, list) and len(fc) >= 2 and set(fc) == {"flash", "smart"}:
        FAILOVER_CHAIN = [str(x) for x in fc]
    HAS_CONFIG, NEED_SETUP = True, False
    log(f"config loaded: flash={CFG['flash']['base']}/{CFG['flash']['model']} "
        f"smart={CFG['smart']['base']}/{CFG['smart']['model']} mode={ROUTE_MODE}")


CFG = {  # 默认透传模式；load_config() 读到 config.json 后覆盖
    "flash": {"base": UPSTREAM, "key": "", "model": FAST_MODEL, "path": "/v1/chat/completions"},
    "smart": {"base": UPSTREAM, "key": "", "model": SMART_MODEL, "path": "/v1/chat/completions"},
}

# ---------- 外部直连模型（不在团队网关上） ----------
# 讯飞星火 X2.6：纯文本，走讯飞 MaaS，key 由 SPARK_KEY 环境变量提供
SPARK_MODEL = "spark-x2.6"
SPARK_BASE = "https://maas-api.cn-huabei-1.xf-yun.com/v2"
SPARK_KEY = os.environ.get("SPARK_KEY", "").strip()
SPARK_QUOTA = int(os.environ.get("SPARK_QUOTA", "500000000"))   # 5 亿 token 总额度
SPARK_DEADLINE = date(2026, 11, 27)                             # 该日期之后自动停用
_spark_used = 0                                                 # 已用 token（启动时从日志汇总）


def _spark_available() -> bool:
    """兜底模型是否还能用：有 key、未过期、额度未耗尽。"""
    if not SPARK_KEY:
        return False
    if date.today() > SPARK_DEADLINE:
        return False
    return _spark_used < SPARK_QUOTA


def _upstream_for(model: str, headers: dict):
    """按模型选择上游：返回 (client, headers, 完整URL)。config 模式注入对应 api_key；
    key 为空则透传客户端 Authorization（团队网关用法）。"""
    if model == SPARK_MODEL:
        return spark_client, {**headers, "Authorization": f"Bearer {SPARK_KEY}"}, \
            SPARK_BASE + "/chat/completions"
    cfg = CFG["flash"] if model == FAST_MODEL else CFG["smart"]
    cli = flash_client if model == FAST_MODEL else smart_client
    h = dict(headers)
    if cfg["key"]:
        h["Authorization"] = f"Bearer {cfg['key']}"
    return cli, h, cfg["base"] + cfg["path"]
LISTEN_HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
LISTEN_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 3100

# ---------- 免费池（仅官方定价页明确标注"免费"的对话模型；key 为空则跳过该项） ----------
# 智谱官方定价页（docs.bigmodel.cn/cn/guide/start/pricing）标注"免费"：
#   glm-4.7-flash（免费，高峰期易 429）、glm-4-flash-250414（免费）、glm-z1-flash（免费）
# 硅基流动定价页（siliconflow.cn/pricing）标注"免费"的对话模型仅 2 个：
#   tencent/Hunyuan-MT-7B（翻译模型，不支持 tools）、XingChenAGI/Xing4.0-29B（支持 tools）
# 硅基其余免费模型（bge 向量/PaddleOCR/Kolors 生图/ASR 语音）均非对话类，无法进分流池。
# tools 标志：请求带 tools 时，自动跳过不支持工具的源，避免无效跳转。
FREE_PROVIDERS = [   # 免费池平台元信息（设置页展示/申请入口；不含任何 key）
    {"id": "zhipu", "name": "智谱 GLM（免费版）", "homepage": "https://open.bigmodel.cn"},
    {"id": "siliconflow", "name": "硅基流动（免费模型）", "homepage": "https://cloud.siliconflow.cn"},
]
FREE_PROV_CFG = {p["id"]: {"enabled": True, "key": ""} for p in FREE_PROVIDERS}  # config.json 覆盖

FREE_POOL = [
    {
        "provider": "zhipu",
        "name": "zhipu-glm-4-flash",
        "base": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4-flash-250414",
        "key_env": os.environ.get("ZHIPU_FREE_KEY", "").strip(),
        "key": os.environ.get("ZHIPU_FREE_KEY", "").strip(),
        "tools": True,
    },
    {
        "provider": "siliconflow",
        "name": "sf-xing4.0-29b",
        "base": "https://api.siliconflow.cn/v1",
        "model": "XingChenAGI/Xing4.0-29B",
        "key_env": os.environ.get("SILICONFLOW_KEY", "").strip(),
        "key": os.environ.get("SILICONFLOW_KEY", "").strip(),
        "tools": True,
    },
    {
        "provider": "zhipu",
        "name": "zhipu-glm-4.7-flash",
        "base": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4.7-flash",
        "key_env": os.environ.get("ZHIPU_FREE_KEY", "").strip(),
        "key": os.environ.get("ZHIPU_FREE_KEY", "").strip(),
        "tools": True,
    },
    {
        "provider": "zhipu",
        "name": "zhipu-glm-z1-flash",
        "base": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-z1-flash",
        "key_env": os.environ.get("ZHIPU_FREE_KEY", "").strip(),
        "key": os.environ.get("ZHIPU_FREE_KEY", "").strip(),
        "tools": False,
    },
    {
        "provider": "siliconflow",
        "name": "sf-hunyuan-mt-7b",
        "base": "https://api.siliconflow.cn/v1",
        "model": "tencent/Hunyuan-MT-7B",
        "key_env": os.environ.get("SILICONFLOW_KEY", "").strip(),
        "key": os.environ.get("SILICONFLOW_KEY", "").strip(),
        "tools": False,
    },
]

# ---------- 分流模式：DeepSeek 极限模式 ----------
# 目标：除极短闲聊外，所有请求都交给 deepseek-v4.1-flash（1M 上下文 / 384K 输出 / 支持 tools 与视觉），
#        glm-5.3 只在显式指定时才用（单价是 deepseek 的 4 倍，缓存命中价差 50 倍）。
# 回滚到"激进升级 L3"：把下面 4 个阈值改回 1500 / 8 / 3，并恢复 SMART_KEYWORDS 列表。
PROMPT_TOKEN_THRESHOLD = 800_000      # 字符数（≈0.8M token，deepseek 上限 1M 的安全线）
MESSAGE_COUNT_THRESHOLD = 40          # 会话内消息数超 40（约 20 轮）→ 判定任务在打转，升级 L3
TOOL_THRESHOLD = 60                   # 工具数
SMART_KEYWORDS = []                   # 不靠关键词升级
FREE_POOL_MAX_CHARS = 400             # 免费池只接极短闲聊，超出直接走 deepseek
CUSTOM_FREE_SOURCES = []              # 用户自定义免费平台源（config.json 提供，设置页可增删）
FAILOVER_CHAIN = ["flash", "smart"]   # L2/L3 兜底顺序（高级设置可调）


def rebuild_free_pool():
    """把自定义平台源并入 FREE_POOL（以配置为准重建，避免重启残留）。"""
    global FREE_POOL
    FREE_POOL = [s for s in FREE_POOL if not s.get("custom")] + CUSTOM_FREE_SOURCES
# 过短回答观察项（仅统计提示，不参与分流）：大会话却回得很短，疑似模型偷懒/放弃
SHORT_MIN_CHARS = 20000               # 会话字符数超过此值才算"大会话"
SHORT_MAX_TOKENS = 150                # 输出 token 低于此值算"过短"

app = FastAPI()
_UP_TIMEOUT = httpx.Timeout(600.0, connect=UPSTREAM_CONNECT_TIMEOUT, pool=UPSTREAM_CONNECT_TIMEOUT)
flash_client = httpx.AsyncClient(timeout=_UP_TIMEOUT)   # config 模式下请求用绝对 URL
smart_client = httpx.AsyncClient(timeout=_UP_TIMEOUT)
client = flash_client                                   # 兼容旧直用点（识图/hedge 探测，均为 flash 上游）
free_client = httpx.AsyncClient(timeout=httpx.Timeout(
    FREE_READ_TIMEOUT, connect=FREE_CONNECT_TIMEOUT, write=10.0, pool=FREE_CONNECT_TIMEOUT))
spark_client = httpx.AsyncClient(base_url=SPARK_BASE, timeout=httpx.Timeout(
    600.0, connect=3.0, pool=3.0), trust_env=False, verify=False)  # verify=False：本机证书链不全，讯飞证书校验失败

# ---------- 熔断器：避免每次都去撞已知失效的免费源 ----------
_circuit: dict[str, float] = {}          # name -> 冷却截止时间戳
_upstream_sem = asyncio.Semaphore(MAX_CONCURRENT)   # 网关并发闸门


async def _acquire_slot(trace: list):
    """等网关并发名额；排队超过 50ms 记进 path，便于排查。"""
    if _upstream_sem.locked():
        t0 = time.monotonic()
        await _upstream_sem.acquire()
        wait = int((time.monotonic() - t0) * 1000)
        if wait >= 50:
            trace.append(f"queued:{wait}ms")
    else:
        await _upstream_sem.acquire()


async def _close_and_release(resp):
    try:
        await resp.aclose()
    finally:
        _upstream_sem.release()


def _circuit_open(name: str) -> bool:
    until = _circuit.get(name, 0)
    if until and time.time() < until:
        return True
    _circuit.pop(name, None)
    return False


def _trip(name: str, sec: float = FREE_COOLDOWN_SEC):
    _circuit[name] = time.time() + sec
    log(f"L1[{name}] circuit open {sec:.0f}s")


def estimate_tokens(messages: list) -> int:
    total = 0
    for m in messages:
        c = m.get("content")
        if isinstance(c, str):
            total += len(c)
        elif isinstance(c, list):
            total += len(json.dumps(c, ensure_ascii=False))
    return total


def has_image(body: dict) -> bool:
    """只看最后一条 user 消息（当前轮）是否带图。
    历史里的图片不再影响路由——否则一个会话发过一张图，后续所有轮次都会永远走 deepseek。"""
    for m in reversed(body.get("messages") or []):
        if m.get("role") != "user":
            continue
        c = m.get("content")
        if isinstance(c, list):
            return any(isinstance(p, dict) and p.get("type") in ("image_url", "image", "input_image")
                       for p in c)
        return False
    return False


def has_image_anywhere(body: dict) -> bool:
    """任意消息里是否有图片片段（含历史）。用于历史图片的描述补齐。"""
    for m in body.get("messages") or []:
        c = m.get("content")
        if isinstance(c, list):
            for p in c:
                if isinstance(p, dict) and p.get("type") in ("image_url", "image", "input_image"):
                    return True
    return False


def strip_images(body: dict) -> bool:
    """把历史消息里的图片片段替换成文字占位，供不支持视觉的模型使用。
    返回是否发生了替换。"""
    changed = False
    for m in body.get("messages") or []:
        c = m.get("content")
        if not isinstance(c, list):
            continue
        new = []
        for p in c:
            if isinstance(p, dict) and p.get("type") in ("image_url", "image", "input_image"):
                new.append({"type": "text", "text": "[图片内容已省略：该模型不支持视觉，图片未发送]"})
                changed = True
            else:
                new.append(p)
        if new and all(isinstance(p, dict) and p.get("type") in ("text", "input_text") for p in new):
            m["content"] = "\n".join(p.get("text", "") for p in new)
        else:
            m["content"] = new
    return changed


# ---------- 图片描述缓存：deepseek 识图 → glm 主导推理 ----------
_img_desc_cache: dict[str, str] = {}      # 图片 URL/base64 前缀 → 描述
_IMG_DESC_MAX = 200                        # 缓存条目上限，防内存膨胀
_DESC_PROMPT = (
    "请用中文详细描述这张图片的全部内容，用于转述给一个看不到图片的助手。"
    "要求：1) 如果是截图/报错，逐字转录所有可见文字（报错信息、代码、路径、数字尤其要精确）；"
    "2) 如果是照片/图表，描述主体、布局、颜色、数据；3) 不要添加猜测和评论，只描述可见内容。"
)

# 识图请求的 token 预算。deepseek 是推理模型，思考过程与正文共享 max_tokens：
# 1024 时思考就吃掉 2800-3600 token（finish=length），content 被截断甚至为空，
# describe_images 静默跳过 → 下游看到 [图片内容已省略]。2026-11-05 实测截图
# 思考 2832-3575 token，4096 留给正文约 500-1200 token，仍偏紧但基本够用。
_DESC_MAX_TOKENS = 4096

# ---------- vision-flash 会话状态：flash 直通作答，跑不通再升 glm ----------
# 2026-11-11 策略（用户确认）：
#   带图轮默认 flash 直通原图（flash 本身是视觉模型，无需先识图）；
#   无图轮维持 glm 优先（历史有图走识图→glm，即原 vision-glm-history）。
#   "跑不通"判定（任一触发 → 该会话带图轮改走识图→glm）：
#     a) flash 空响应（content 空且无 tool_calls）1 次即升——思考吃光预算的已知风险
#     b) finish_reason=length 截断累计 2 次——截断≠跑不通，容忍一次偶发
#     c) 指令执行深度超阈值（最后一条 user 之后的消息数）——2026-11-11 修正：原用会话
#        总消息数，但 CodeBuddy 每次调用全量携带页面历史，长会话会被永久误判为打转；
#        改为只数"当前指令跑了几轮"，新指令天然从 0 计数，页面历史不计入
#     d) flash 直通轮上游 400——当轮立即换 glm（glm 前先识图注入）
#     e) 同一工具调用签名在最近 40 条里重复 ≥3 次——打转铁证
#   用户新发图（图 key 会话内未见过）→ 重置 a/b 计数与升级标志，重新给 flash 机会。
#   不做时间粘性：升级状态只由"新图重置"，会话指纹变化（新会话/上下文重写）自然重置。
VISION_EMPTY_LIMIT = 1        # 空响应几次升级
VISION_LENGTH_LIMIT = 2       # 截断几次升级
STUCK_DEPTH_IMAGE = 24        # 带图指令执行深度阈值（最后一条 user 后 24 条 ≈ 12 轮）
STUCK_DEPTH_TEXT = 40         # 纯文本指令执行深度阈值（编码任务链更长，容忍 20 轮）
VISION_SEEN_IMGS = 20         # 每会话记住的图 key 上限
_SESSION_TTL = 7200           # 会话状态保留时长（仅内存管理，非粘性）
TOOL_SIG_WINDOW = 40          # 工具签名滑动窗口（只看最近 N 次 tool_call）
TOOL_SIG_REPEAT = 3           # 窗口内同签名（工具名+归一化参数）出现 ≥3 次 = 打转
_sessions: dict[str, dict] = {}   # 指纹 -> {"empty","length","upgraded","imgs","ts"}


def _session_key(body: dict) -> str:
    """会话指纹：system + 首条 user 前 512 字符。压缩上下文/新会话时指纹变化即自然重置。"""
    msgs = body.get("messages") or []
    parts = []
    for m in msgs[:2]:
        c = m.get("content")
        s = c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
        parts.append((m.get("role", "") + ":" + s)[:512])
    return hashlib.md5("\x00".join(parts).encode("utf-8", "ignore")).hexdigest()


def _sess(skey: str) -> dict:
    """取（或建）会话状态，刷新时间戳并做容量清理。"""
    s = _sessions.get(skey)
    if not s or time.time() - s["ts"] > _SESSION_TTL:
        s = {"empty": 0, "length": 0, "upgraded": False, "imgs": [], "ts": time.time()}
        _sessions[skey] = s
    else:
        s["ts"] = time.time()
    if len(_sessions) > 500:
        for k in sorted(_sessions, key=lambda k: _sessions[k]["ts"])[:100]:
            _sessions.pop(k, None)
    return s


def _current_image_keys(body: dict) -> list:
    """当前轮（最后一条 user 消息）里的图 key 列表。"""
    for m in reversed(body.get("messages") or []):
        if m.get("role") != "user":
            continue
        c = m.get("content")
        if isinstance(c, list):
            return [_img_key(p) for p in c
                    if isinstance(p, dict) and p.get("type") in ("image_url", "image", "input_image")]
        return []
    return []


def _all_image_keys(body: dict) -> list:
    """全部消息里的图 key 列表（历史图轮入账用——否则后续带图轮会误判为新图，
    错误重置升级状态）。"""
    keys = []
    for m in body.get("messages") or []:
        c = m.get("content")
        if not isinstance(c, list):
            continue
        for p in c:
            if isinstance(p, dict) and p.get("type") in ("image_url", "image", "input_image"):
                keys.append(_img_key(p))
    return keys


def _note_flash_fail(skey: str, kind: str):
    """flash 直通轮响应后记账；达到阈值置 upgraded。"""
    s = _sessions.get(skey)
    if not s:
        return
    if kind == "empty":
        s["empty"] += 1
        if s["empty"] >= VISION_EMPTY_LIMIT:
            s["upgraded"] = True
    elif kind == "length":
        s["length"] += 1
        if s["length"] >= VISION_LENGTH_LIMIT:
            s["upgraded"] = True


def _mark_upgraded(skey: str):
    s = _sessions.get(skey)
    if s:
        s["upgraded"] = True


def _tool_sig_loop(body: dict) -> int:
    """网关版 LoopGuard（72Technologies 生产实践）：扫描消息历史里的 tool_calls，
    签名 = 工具名 + 排序键归一化参数的哈希；返回滑动窗口内最大同签名重复次数。
    CodeBuddy 每轮发送完整消息历史，网关可离线判定，无需记账（历史即证据，天然粘性）。"""
    sigs = []
    for m in body.get("messages") or []:
        if m.get("role") != "assistant":
            continue
        for tc in m.get("tool_calls") or []:
            if not isinstance(tc, dict):
                continue
            fn = tc.get("function") or {}
            args = fn.get("arguments") or ""
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {"_raw": args}
            try:
                norm = json.dumps(args, sort_keys=True, ensure_ascii=False)
            except Exception:
                norm = str(args)
            sigs.append(hashlib.md5(((fn.get("name") or "") + "\x00" + norm)
                                    .encode("utf-8", "ignore")).hexdigest()[:12])
    sigs = sigs[-TOOL_SIG_WINDOW:]
    best = 0
    for s in set(sigs):
        c = sigs.count(s)
        if c > best:
            best = c
    return best


def _instruction_depth(body: dict) -> int:
    """当前指令的执行深度：最后一条 user 消息之后的消息数（agent loop 每
    推进一轮 +2：assistant + tool 结果）。页面历史不计入——CodeBuddy 每次调用
    全量携带历史，总消息数会把长会话永久误判为打转。指令边界即天然重置点。"""
    msgs = body.get("messages") or []
    for i in range(len(msgs) - 1, -1, -1):
        if (msgs[i] or {}).get("role") == "user":
            return len(msgs) - 1 - i
    return len(msgs)


def _flash_stream_verdict(tail: bytes) -> str | None:
    """从流式 tail（末 16KB）判断 flash 作答是否空响应/截断。
    空响应 = finish_reason=stop 且 tail 内无非空 content delta（空响应总量小，tail 必覆盖全程）。"""
    finish = None
    has_content = False
    for line in tail.split(b"\n"):
        line = line.strip()
        if not line.startswith(b"data:"):
            continue
        d = line[5:].strip()
        if d in (b"[DONE]", b""):
            continue
        try:
            j = json.loads(d)
        except Exception:
            continue
        for ch in j.get("choices") or []:
            delta = ch.get("delta") or {}
            if delta.get("content"):
                has_content = True
            if ch.get("finish_reason"):
                finish = ch["finish_reason"]
    if finish == "length":
        return "length"
    if finish == "stop" and not has_content:
        return "empty"
    return None


def _flash_payload_verdict(payload: dict) -> str | None:
    """非流式：判空响应/截断。"""
    ch = (payload.get("choices") or [{}])[0]
    msg = ch.get("message") or {}
    if ch.get("finish_reason") == "length":
        return "length"
    if ch.get("finish_reason") == "stop" and not (msg.get("content") or "").strip() \
            and not msg.get("tool_calls"):
        return "empty"
    return None


def _img_key(p: dict) -> str:
    """图片片段的缓存键：URL 或 base64 前 256 字符。"""
    url = (p.get("image_url") or {}).get("url") or p.get("source", {}).get("data") or ""
    return url[:256]


async def describe_images(body: dict, headers: dict) -> int:
    """用 deepseek 非流式识别消息里的图片，把描述写回缓存。
    返回处理的图片数。"""
    msgs = body.get("messages") or []
    tasks = []          # (消息, 片段, key)
    for m in msgs:
        c = m.get("content")
        if not isinstance(c, list):
            continue
        for p in c:
            if isinstance(p, dict) and p.get("type") in ("image_url", "image", "input_image"):
                k = _img_key(p)
                if k in _img_desc_cache:
                    continue
                tasks.append((m, p, k))
    if not tasks:
        return 0

    # 逐张识别（并发会撞网关闸门，且图片请求本身少见）
    done = 0
    for m, p, k in tasks:
        # 最多两轮：第一轮常规；若 content 为空或被截断（finish=length），
        # 第二轮关掉思考重试，保证拿到完整正文描述
        for attempt in range(2):
            ask = {"model": FAST_MODEL, "max_tokens": _DESC_MAX_TOKENS, "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": _DESC_PROMPT},
                    p,
                ]}]}
            if attempt:
                ask["chat_template_kwargs"] = {"thinking": False}
            try:
                await _acquire_slot([])
                try:
                    d_cli, d_headers, d_url = _upstream_for(FAST_MODEL, headers)
                    r = await d_cli.post(d_url, json=ask, headers=d_headers)
                finally:
                    _upstream_sem.release()
                if r.status_code != 200:
                    log(f"describe fail HTTP{r.status_code}")
                    break
                ch = (r.json().get("choices") or [{}])[0]
                desc = (ch.get("message") or {}).get("content", "").strip()
                if desc and ch.get("finish_reason") != "length":
                    _img_desc_cache[k] = desc
                    if len(_img_desc_cache) > _IMG_DESC_MAX:
                        _img_desc_cache.pop(next(iter(_img_desc_cache)))
                    done += 1
                    break
                if attempt:
                    # 重试后仍为空/截断：把残缺描述也存进去，总比占位符强
                    if desc:
                        _img_desc_cache[k] = desc
                        if len(_img_desc_cache) > _IMG_DESC_MAX:
                            _img_desc_cache.pop(next(iter(_img_desc_cache)))
                        done += 1
                    else:
                        log("describe empty after retry (thinking ate budget)")
                    break
                # attempt 0 且为空/截断 → 关思考重试
            except Exception as e:
                log(f"describe error: {type(e).__name__}")
                break
    return done


def inject_image_descs(body: dict) -> bool:
    """把消息里的图片片段替换成缓存的描述（无缓存则占位）。返回是否改动。"""
    changed = False
    for m in body.get("messages") or []:
        c = m.get("content")
        if not isinstance(c, list):
            continue
        new = []
        for p in c:
            if isinstance(p, dict) and p.get("type") in ("image_url", "image", "input_image"):
                k = _img_key(p)
                desc = _img_desc_cache.get(k)
                new.append({"type": "text",
                            "text": f"[图片内容：{desc}]" if desc else
                            "[图片内容已省略：识图调用失败，请重发图片或稍后重试]"})
                changed = True
            else:
                new.append(p)
        if new and all(isinstance(p, dict) and p.get("type") in ("text", "input_text") for p in new):
            m["content"] = "\n".join(p.get("text", "") for p in new)
        else:
            m["content"] = new
    return changed


# 简单指令识别（2026-11-12）：提交/继续/重试类短指令 flash 完全胜任。
# 只作用于"新指令"（agent loop 未启动，depth<=1）——loop 中途轮的 user 消息
# 仍是原始长指令，不能拿来判断。且不做复杂度豁免：上下文超限仍走 glm。
SIMPLE_CMD_MAX_CHARS = 60
_SIMPLE_VERBS = ("提交", "commit", "push", "pull", "拉取", "合并", "merge",
                 "继续", "好的", "可以", "确定", "是", "对", "ok", "好",
                 "重新", "重试", "再试", "保存", "运行", "执行", "刷新",
                 "同步", "查看", "打开", "关闭", "继续执行", "continue", "next")


def _last_user_text(body: dict) -> str:
    """最后一条 user 消息的文本（非文本内容序列化）。无 user 消息返回空串。"""
    msgs = body.get("messages") or []
    for m in reversed(msgs):
        if (m.get("role") or "") == "user":
            c = m.get("content")
            return c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
    return ""


def is_simple_command(body: dict, depth: int) -> bool:
    """本轮是否为简短操作指令：新指令（depth<=1）+ 短文本 + 命令式动词。
    用于突破粘性升级——上一个指令曾升级 glm，不代表新的"提交git"也需要 glm。"""
    if depth > 1:
        return False
    last_user = _last_user_text(body)
    if not last_user or len(last_user) > SIMPLE_CMD_MAX_CHARS:
        return False
    low = last_user.strip().lower()
    if "```" in low or "http" in low:   # 带代码块/链接的不算简单指令
        return False
    return any(k in low for k in _SIMPLE_VERBS)


def is_complex(body: dict) -> bool:
    """是否升级到 L3（glm-5.3）：只看上下文体量与工具数的硬保护。
    打转判定不再放这里——2026-11-11 起"消息数 ≥40"口径已废弃（长会话历史会
    误判），统一改用 _instruction_depth 的指令执行深度（flash 模式分支内判定）。"""
    messages = body.get("messages") or []
    tools = body.get("tools") or []
    if estimate_tokens(messages) >= PROMPT_TOKEN_THRESHOLD:
        return True
    if len(tools) >= TOOL_THRESHOLD:
        return True
    last_user = ""
    for m in reversed(messages):
        if m.get("role") == "user":
            c = m.get("content")
            last_user = c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
            break
    return any(k in last_user for k in SMART_KEYWORDS)


def log(msg: str):
    print(f"[router] {msg}", flush=True)


# 账户级错误（欠费/额度异常/账号受限）：同一请求换模型大概率仍失败，
# 但应触发链路下一个模型回退，而非把底层错误直接透传给客户端。
# 智谱返回 {"error":{"type":"Arrearage","message":"Access denied, please make sure
# your account is in good standing..."}}；其余厂商类似文案一并覆盖。
_ACCOUNT_ERR_RE = re.compile(
    r"arrearage|good standing|access denied"
    r"|insufficient[_ ]?(balance|quota)|quota[_ ]?(exceed|exhaust)"
    r"|余额不足|欠费|账户.*(异常|冻结|欠)|balance.*not.*enough",
    re.I)


def _is_account_error(raw: bytes) -> bool:
    """判断上游 400 是否为账户级错误（欠费/额度）。是则触发链路回退而非透传。"""
    # 中文文案无法写进 bytes 字面量（SyntaxError），统一按 UTF-8 解码后用 str 正则匹配
    return bool(_ACCOUNT_ERR_RE.search((raw or b"").decode("utf-8", "ignore")))


# ---------- 请求日志 ----------
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "router.log")
LOG_ENABLED = os.environ.get("ROUTER_LOG", "1") != "0"   # ROUTER_LOG=0 可关闭
LOG_MAX_BYTES = int(os.environ.get("ROUTER_LOG_MAX_MB", "20")) * 1024 * 1024  # 超限轮转为 router.log.1
_log_writes = 0


def _rotate_log():
    """日志超过上限时轮转一份 router.log.1（保留最近一份，磁盘占用封顶约 2×上限）。"""
    global _log_writes
    _log_writes += 1
    if _log_writes % 200:
        return
    try:
        if os.path.getsize(LOG_FILE) > LOG_MAX_BYTES:
            bak = LOG_FILE + ".1"
            if os.path.exists(bak):
                os.remove(bak)
            os.replace(LOG_FILE, bak)
    except OSError:
        pass


def log_request(record: dict):
    """追加一行 JSON 到 router.log，并同步打印摘要。"""
    global _spark_used
    if record.get("final_model") == SPARK_MODEL:
        _spark_used += int(record.get("total_tokens") or 0)
    if not LOG_ENABLED:
        return
    record["ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"[router] log write fail: {e}", flush=True)
    _rotate_log()
    # 控制台摘要
    path = " -> ".join(record.get("path", []))
    print(f"[LOG] {record['ts']} {record.get('verdict','?'):8s} {path} "
          f"({record.get('final_model','?')}, {record.get('total_ms',0)}ms)", flush=True)


def first_user_snippet(body: dict, n=40) -> str:
    for m in reversed(body.get("messages") or []):
        if m.get("role") == "user":
            c = m.get("content")
            s = c if isinstance(c, str) else json.dumps(c, ensure_ascii=False)
            return s[:n].replace("\n", " ")
    return ""


def first_user_text(body: dict, n=4000) -> str:
    """最后一条用户消息的明文（多模态只取 text 片段），截断到 n 字符。"""
    for m in reversed(body.get("messages") or []):
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, str):
                return c[:n]
            if isinstance(c, list):
                parts = [p.get("text", "") for p in c
                         if isinstance(p, dict) and p.get("type") in ("text", "input_text")]
                return "\n".join(parts)[:n]
    return ""


USAGE_RE = re.compile(rb'"usage"\s*:\s*\{[^}]*\}')


def _usage_fields(u: dict) -> dict:
    """把上游 usage 归一化：prompt/completion/total/cached。"""
    if not u:
        return {}
    details = u.get("prompt_tokens_details") or {}
    cached = details.get("cached_tokens")
    if cached is None:
        cached = u.get("prompt_cache_hit_tokens")
    return {
        "prompt_tokens": int(u.get("prompt_tokens") or 0),
        "completion_tokens": int(u.get("completion_tokens") or 0),
        "total_tokens": int(u.get("total_tokens") or 0),
        "cached_tokens": int(cached or 0),
    }


def _mirror_usage(u):
    """把 glm/deepseek 式缓存字段（prompt_cache_hit_tokens）镜像写入 OpenAI 标准
    字段（prompt_tokens_details.cached_tokens）。上游真实命中与计费不变，仅补齐
    口径——CodeBuddy 的用量面板只认标准字段，读不到私有字段就显示 0%。"""
    if not isinstance(u, dict):
        return
    hit = u.get("prompt_cache_hit_tokens")
    if hit is None:
        hit = (u.get("prompt_tokens_details") or {}).get("cached_tokens")
    if hit:
        det = u.get("prompt_tokens_details")
        if not isinstance(det, dict):
            det = {}
        det["cached_tokens"] = hit
        u["prompt_tokens_details"] = det


def _mirror_sse_chunk(chunk: bytes) -> bytes:
    """流式透传时改写含 usage 的 SSE 块：缓存字段镜像到标准口径。
    只重写能完整解析的 data 行；任何解析异常（chunk 切断半行等）整块原样返回，
    绝不影响响应本身。非 usage 块零开销直通。"""
    if b'"usage"' not in chunk:
        return chunk
    out, changed = [], False
    for line in chunk.split(b"\n"):
        s = line.strip()
        if b'"usage"' in s and s.startswith(b"data:") and b"[DONE]" not in s:
            try:
                d = json.loads(s[5:].strip().decode("utf-8", "ignore"))
                u = d.get("usage")
                if isinstance(u, dict):
                    _mirror_usage(u)
                    out.append(b"data: " + json.dumps(d, ensure_ascii=False).encode("utf-8"))
                    changed = True
                    continue
            except Exception:
                pass
        out.append(line)
    return b"\n".join(out) if changed else chunk


def _extract_usage(raw: bytes):
    """从响应体或 SSE 尾部提取 usage（逐行解析，兼容嵌套的 *_tokens_details）。"""
    if not raw:
        return None
    for line in reversed(raw.split(b"\n")):
        line = line.strip()
        if b'"usage"' not in line:
            continue
        if line.startswith(b"data:"):
            line = line[5:].strip()
        if line in (b"[DONE]", b""):
            continue
        try:
            d = json.loads(line.decode("utf-8", "ignore"))
        except json.JSONDecodeError:
            continue
        u = d.get("usage") or {}
        if u.get("total_tokens") or u.get("prompt_tokens"):
            return _usage_fields(u)
    return None


def _tap(it, rec: dict, t_start: float, closer=None, skey: str = ""):
    """透传流式响应，结束后补上 token 用量再写日志。
    vision-flash 轮顺带从 tail 判空响应/截断，记账到会话状态。"""
    async def gen():
        tail = b""
        try:
            async for chunk in it:
                tail = (tail + chunk)[-16384:]
                yield _mirror_sse_chunk(chunk)
        finally:
            if closer is not None:
                try:
                    await closer()
                except Exception:
                    pass
            if skey and rec.get("final_model") == FAST_MODEL and rec.get("verdict") != "explicit":
                v = _flash_stream_verdict(tail)
                if v:
                    _note_flash_fail(skey, v)
                    rec["path"].append(f"flashfail:{v}")
            u = _extract_usage(tail)
            if u:
                rec.update(u)
            rec["total_ms"] = int((time.monotonic() - t_start) * 1000)
            log_request(rec)
    return gen()


async def try_free_stream(body: dict, headers: dict, trace: list):
    """依次尝试免费池。返回 (stream_iterator, status, media_type, source_name) 或 None。
    判定标准：{FREE_TTFB_TIMEOUT}s 内收到首个数据块且 HTTP 200；否则视为排队/拥塞，试下一个。
    请求带 tools 时自动跳过不支持工具调用的源。trace 记录分流路径。"""
    has_tools = bool(body.get("tools"))
    size = estimate_tokens(body.get("messages") or [])
    if not FREE_POOL_ENABLED:
        trace.append("skip:free-pool(disabled)")
        return None
    if has_tools and size > TOOLS_FREE_MAX_CHARS:
        log(f"L1 skipped (tools + size={size} > {TOOLS_FREE_MAX_CHARS}) -> L2")
        trace.append(f"skip:free-pool(tools,{size}chars)")
        return None
    if not has_tools and size > FREE_POOL_MAX_CHARS:
        log(f"L1 skipped (size={size} > {FREE_POOL_MAX_CHARS}) -> L2")
        trace.append(f"skip:free-pool({size}chars)")
        return None
    for src in FREE_POOL:
        if not src["key"]:
            continue
        if _circuit_open(src["name"]):
            log(f"L1[{src['name']}] skipped (circuit open)")
            trace.append(f"skip:{src['name']}(circuit)")
            continue
        if has_tools and not src.get("tools"):
            log(f"L1[{src['name']}] skipped (no tool-call support)")
            trace.append(f"skip:{src['name']}(no-tools)")
            continue
        req_body = dict(body)
        req_body["model"] = src["model"]
        if req_body.get("stream") and "stream_options" not in req_body:
            req_body["stream_options"] = {"include_usage": True}
        _apply_extra(req_body, (FREE_PROV_CFG.get(src["provider"]) or {}).get("extra"))
        auth = {"Authorization": f"Bearer {src['key']}", "Content-Type": "application/json"}
        try:
            req = free_client.build_request("POST", f"{src['base']}/chat/completions", json=req_body, headers=auth)
            resp = await free_client.send(req, stream=True)
        except httpx.HTTPError as e:
            log(f"L1[{src['name']}] connect fail: {type(e).__name__} -> next")
            trace.append(f"fail:{src['name']}({type(e).__name__})")
            _trip(src["name"])
            continue
        if resp.status_code != 200:
            detail = (await resp.aread())[:200]
            await resp.aclose()
            log(f"L1[{src['name']}] HTTP {resp.status_code}: {detail!r} -> next")
            trace.append(f"fail:{src['name']}(HTTP{resp.status_code})")
            # 欠费/额度类错误：长冷却，避免每 60s 反复撞死账户、拖慢回退
            _trip(src["name"], sec=3600 if _is_account_error(detail) else FREE_COOLDOWN_SEC)
            continue
        # 首块超时判定
        t0 = time.monotonic()
        it = resp.aiter_raw()
        try:
            first = await asyncio.wait_for(it.__anext__(), timeout=FREE_TTFB_TIMEOUT)
        except (StopAsyncIteration, TimeoutError, httpx.HTTPError) as e:
            await resp.aclose()
            log(f"L1[{src['name']}] TTFB>{FREE_TTFB_TIMEOUT}s or stream error ({type(e).__name__}) -> next")
            trace.append(f"fail:{src['name']}(ttfb>{FREE_TTFB_TIMEOUT}s)")
            _trip(src["name"])
            continue
        dt = time.monotonic() - t0
        log(f"L1[{src['name']}] hit (ttfb {dt:.1f}s)")
        trace.append(f"hit:{src['name']}(ttfb {dt:.2f}s)")

        async def chain() -> AsyncIterator[bytes]:
            yield first
            async for chunk in it:
                yield chunk
            await resp.aclose()

        return chain(), resp.status_code, resp.headers.get("content-type", "text/event-stream"), src["name"]
    log("L1 free pool exhausted -> fallback L2")
    trace.append("pool-exhausted")
    return None


async def _l2_first(body: dict, headers: dict, target: str):
    """发起 L2 请求并读到首块，返回结构与 try_free_stream 一致。"""
    b = dict(body)
    b["model"] = target
    if b.get("stream") and "stream_options" not in b:
        b["stream_options"] = {"include_usage": True}
    h_cli, h_headers, h_url = _upstream_for(target, headers)
    req = h_cli.build_request("POST", h_url, json=b, headers=h_headers)
    resp = await h_cli.send(req, stream=True)
    it = resp.aiter_raw()
    try:
        first = await it.__anext__()
    except BaseException:
        await resp.aclose()
        raise

    async def chain() -> AsyncIterator[bytes]:
        try:
            yield first
            async for c in it:
                yield c
        finally:
            await resp.aclose()

    return chain(), resp.status_code, resp.headers.get("content-type", "text/event-stream"), target


async def hedge_stream(body: dict, headers: dict, target: str, trace: list):
    """免费池与 L2 并发竞速：谁先吐首块用谁，另一个立刻取消。"""
    ft = asyncio.create_task(try_free_stream(body, headers, []))
    lt = asyncio.create_task(_l2_first(body, headers, target))
    pending = {ft, lt}
    try:
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    res = t.result()
                except Exception as e:
                    log(f"hedge side error: {type(e).__name__}")
                    res = None
                if res:
                    for p in pending:
                        p.cancel()
                    tag = "free" if t is ft else "l2"
                    trace.append(f"hedge-win:{tag}")
                    log(f"hedge winner: {tag} ({res[3]})")
                    return res
        return None
    finally:
        for t in (ft, lt):
            if not t.done():
                t.cancel()


@app.on_event("startup")
def load_spark_usage():
    """从历史日志汇总 spark 已用 token（进程启动时调用一次）。"""
    global _spark_used
    _spark_used = 0
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if SPARK_MODEL not in line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if d.get("final_model") == SPARK_MODEL:
                    _spark_used += int(d.get("total_tokens") or 0)
    except OSError:
        pass


async def warmup():
    """预热连接。"""
    log(f"spark 已用额度: {_spark_used:,} / {SPARK_QUOTA:,} tokens，有效期至 {SPARK_DEADLINE}")

    async def hit(coro):
        try:
            await asyncio.wait_for(coro, 4)
        except Exception:
            pass
    await asyncio.gather(
        hit(client.get("/v1/models")),
        hit(free_client.get("https://open.bigmodel.cn/api/paas/v4/models")),
        hit(free_client.get("https://api.siliconflow.cn/v1/models")),
        return_exceptions=True,
    )
    log("warmup done")


@app.get("/v1/models")
async def list_models():
    # spark-x2.6 已不参与任何自动兜底（2026-11-05 起），仅可手动显式调用，
    # 故不在模型列表里暴露，避免客户端把它当常规模型选
    ids = ["auto", SMART_MODEL, FAST_MODEL]
    return {"object": "list",
            "data": [{"id": i, "object": "model", "owned_by": "xrouter"} for i in ids]}


LOG_PAGE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>XRouter·川流 分流日志</title>
<style>
  :root { color-scheme: light; --brand: #0e7490; --brand-mid: #22b8cf; --brand-light: #a5e8f5;
    --bg: #0b111c; --card: #fff; --card2: #fff; --bd: #e6ebf0; --bd2: #f0f1f3;
    --tx: #1f2328; --tx2: #57606a; --tx3: #6a737d; --head: #f0f1f3; --hover: #e8f6fc;
    --free: #1a7f37; --paid: #cf222e; --link: #0969da; --warn: #b35900; --accent: #378add;
    --onbg: #a9bdd4; --onbg2: #e8f0fa; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: "Segoe UI", "Microsoft YaHei", sans-serif; position: relative; background-color: #eef8ff; background-image: linear-gradient(180deg, #0b1530 0%, #14264c 10%, #26538e 22%, #4a7fb5 36%, #8fc2e4 50%, #cfe9f7 64%, #eef8ff 80%); color: var(--tx); padding: 20px; overflow-x: hidden; }
  body > * { position: relative; z-index: 1; }
  /* 星空层：星点平铺 + 银河光斑，仅覆盖页面顶部并向晴空渐隐 */
  body::before { content: ""; position: absolute; top: 0; left: 0; right: 0; height: 1000px; z-index: 0; pointer-events: none;
    background-image:
      radial-gradient(2.2px 2.2px at 40px 60px, rgba(255,255,255,.95), transparent),
      radial-gradient(1.4px 1.4px at 150px 130px, rgba(205,235,255,.85), transparent),
      radial-gradient(1.2px 1.2px at 260px 45px, rgba(255,255,255,.7), transparent),
      radial-gradient(1px 1px at 90px 210px, rgba(255,255,255,.55), transparent),
      radial-gradient(1.6px 1.6px at 330px 180px, rgba(190,230,255,.75), transparent),
      radial-gradient(1px 1px at 210px 280px, rgba(255,255,255,.45), transparent),
      radial-gradient(3px 3px at 310px 70px, rgba(230,248,255,.5), rgba(230,248,255,0) 55%),
      radial-gradient(1.3px 1.3px at 60px 300px, rgba(255,255,255,.8), transparent),
      radial-gradient(1px 1px at 300px 250px, rgba(200,228,255,.6), transparent),
      radial-gradient(1.5px 1.5px at 175px 320px, rgba(255,255,255,.65), transparent),
      radial-gradient(2.6px 2.6px at 25px 150px, rgba(214,240,255,.45), rgba(214,240,255,0) 55%),
      radial-gradient(1px 1px at 250px 170px, rgba(255,255,255,.5), transparent),
      radial-gradient(2.8px 2.8px at 352px 42px, rgba(224,244,255,.5), rgba(224,244,255,0) 55%),
      radial-gradient(1.2px 1.2px at 22px 30px, rgba(255,255,255,.85), transparent),
      radial-gradient(1px 1px at 108px 62px, rgba(205,232,255,.6), transparent),
      radial-gradient(1.4px 1.4px at 292px 96px, rgba(255,255,255,.7), transparent);
    background-size: 380px 340px, 380px 340px, 380px 340px, 380px 340px, 380px 340px, 380px 340px,
                     380px 340px, 300px 270px, 300px 270px, 300px 270px, 420px 380px, 420px 380px,
                     460px 400px, 460px 400px, 460px 400px, 460px 400px;
    -webkit-mask-image: linear-gradient(180deg, #000 0%, #000 45%, transparent 92%);
    mask-image: linear-gradient(180deg, #000 0%, #000 45%, transparent 92%);
    animation: twinkle 2.6s ease-in-out infinite; }
  @keyframes twinkle { 0%, 100% { opacity: 1; } 50% { opacity: .3; } }
  /* 呼吸层：星云与天光缓慢起伏，与星点闪烁双周期干涉，形成"夜空呼吸" */
  body::after { content: ""; position: absolute; top: 0; left: 0; right: 0; height: 1100px; z-index: 0; pointer-events: none;
    background-image:
      radial-gradient(520px 300px at 78% 140px, rgba(34,184,207,.22), transparent 70%),
      radial-gradient(640px 360px at 16% 300px, rgba(99,102,241,.18), transparent 70%),
      radial-gradient(1100px 520px at 50% 0%, rgba(125,232,250,.15), transparent 72%);
    transform-origin: 50% 0;
    animation: breathe 4.5s ease-in-out infinite; }
  @keyframes breathe { 0%, 100% { opacity: .12; transform: scale(1); } 50% { opacity: 1; transform: scale(1.13); } }
  .brand { display: flex; align-items: center; gap: 12px; width: 88%; margin: 0 auto 6px; }
  .brand .bn { font-size: 24px; font-weight: 700; letter-spacing: -.5px; line-height: 1.1; color: var(--onbg2); }
  .brand .bn .bcn { margin-left: 8px; font-size: 17px; font-weight: 600; background: linear-gradient(90deg, var(--brand), var(--brand-mid)); -webkit-background-clip: text; background-clip: text; color: transparent; }
  .brand .bsub { font-size: 12px; color: var(--onbg); margin-top: 2px; }
  .ft { margin-top: 28px; padding-top: 12px; border-top: 1px solid rgba(31,58,82,.18); color: #5b7186; font-size: 12px; text-align: center; }
  .ft .ftbtn { display: inline-block; margin-right: 14px; padding: 3px 16px; border: 1px solid rgba(31,58,82,.28); border-radius: 14px; color: #3d566e; text-decoration: none; font-size: 12px; transition: color .2s ease, border-color .2s ease, background .2s ease; }
  .ft .ftbtn:hover { color: #0e7490; border-color: var(--brand-mid); background: rgba(34,184,207,.10); }
  .brand svg .fl { stroke-dasharray: 11 5; animation: xflow 1.1s linear infinite; }
  @keyframes xflow { to { stroke-dashoffset: -16; } }
  .sub { color: var(--onbg); font-size: 12px; width: 88%; margin: 0 auto 16px; }
  .stats { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
  .stat { position: relative; background: var(--card); border: 1px solid var(--bd); border-radius: 10px; padding: 8px 14px; flex: 1 1 130px; min-width: 126px; box-shadow: 0 1px 2px rgba(0,0,0,.25), 0 4px 14px rgba(0,0,0,.22); transition: box-shadow .25s ease, border-color .25s ease; }
  .stat:hover { z-index: 5; border-color: #9ad0e2; box-shadow: 0 0 0 1px rgba(34,184,207,.30), 0 6px 18px rgba(14,116,144,.20); }
  .stat .num { font-size: 22px; font-weight: 600; line-height: 1.25; transition: font-size .25s ease; color: var(--tx); }
  .stat .lbl { font-size: 13px; color: var(--tx2); line-height: 1.35; margin-top: 2px; transition: font-size .25s ease; }
  .stat:hover .num { font-size: 14px; }
  .stat:hover .lbl { font-size: 11px; }
  .stat .lbl .pn { display: inline-block; white-space: nowrap; }
  .stat .num.topnum { font-size: 12px !important; }
  .stat:hover .num.topnum { font-size: 11px !important; }
  .stat .num { font-size: 22px; font-weight: 600; line-height: 1.25; }
  .stat .lbl { font-size: 13px; color: #6a737d; line-height: 1.35; margin-top: 2px; }
  .stat .lbl .exp { display: block; font-size: 11px; color: var(--tx2); line-height: 1.55; max-height: 0; overflow: hidden; opacity: 0; margin-top: 0; transition: max-height .3s ease, opacity .2s ease, margin-top .2s ease; }
  .stat:hover .lbl .exp { max-height: 180px; opacity: 1; margin-top: 4px; transition-delay: .6s; }
  button.mini { padding: 3px 10px; font-size: 12px; }
  button.mini.on { background: #0e7490; color: #fff; border-color: #0e7490; }
  .sechd, .dechd { width: 82%; margin: 18px auto 8px; padding: 0 10px; box-sizing: border-box; display: flex; align-items: center; gap: 12px; }
  .sechd h2.sec, .dechd h2.sec { margin: 0; }
  .tablewrap.det { width: 82%; margin: 0 auto 16px; }
  table.det tr.srow td { background: #e6f2f8; font-weight: 600; cursor: pointer; user-select: none; border-top: 1px solid #cfe0ea; border-bottom: 1px solid #cfe0ea; }
  table.det tr.srow:hover td { background: #dcedf6; }
  table.det tr.srow .caret { display: inline-block; width: 14px; color: var(--accent); }
  table.det tr.crow td { background: var(--card); }
  .stat.free .num { color: var(--free); }
  .stat.paid .num { color: var(--paid); }
  table { width: 100%; border-collapse: collapse; background: var(--card); border: 1px solid var(--bd); border-radius: 10px; overflow: hidden; font-size: 13px; box-shadow: 0 1px 2px rgba(0,0,0,.25), 0 4px 14px rgba(0,0,0,.22); }
  table.det, table.daytbl { min-width: 760px; }
  .ptab { width: auto; min-width: 460px; border-collapse: collapse; font-size: 13px; }
  .ptab td, .ptab th { padding: 3px 18px !important; line-height: 1.5; }
  .tablewrap { overflow-x: auto; margin-bottom: 16px; }
  .toprow { width: 82%; margin: 0 auto 16px; display: flex; gap: 12px; align-items: stretch; flex-wrap: wrap; }
  .toprow .stats { flex: 1 1 0%; min-width: 300px; margin-bottom: 0; }
  #pricecard { flex: 0 1 auto; min-width: 0; display: flex; }
  .pcard { padding: 8px 18px; width: 100%; }
  th { background: var(--head); text-align: left; padding: 8px 10px; font-weight: 600; border-bottom: 1px solid var(--bd); white-space: nowrap; color: var(--tx2); }
  td { padding: 7px 10px; border-bottom: 1px solid var(--bd2); vertical-align: top; color: var(--tx); }
  tr:hover td, table.det tr.crow:hover td, table.daytbl tr.dayrow:hover td, table.daytbl tr.mrow:hover td { background: var(--hover); }
  .tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 11px; font-weight: 500; white-space: nowrap; }
  .tag.simple { background: #dafbe1; color: #116329; }
  .tag.complex { background: #ffebe9; color: #82071e; }
  .tag.explicit { background: #ddf4ff; color: #0969da; }
  .tag.vision { background: #f3e8ff; color: #8b5cf6; }
  .m-free { color: var(--free); font-weight: 600; }
  .m-paid { color: var(--paid); font-weight: 600; }
  .path { color: var(--tx2); font-size: 12px; line-height: 1.6; }
  .path .hit { color: var(--free); }
  .path .fail { color: var(--warn); }
  .path .skip { color: var(--tx3); }
  .snippet { color: #24292f; max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; cursor: pointer; }
  .snippet:hover { color: var(--link); text-decoration: underline; }
  #modal { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.45); z-index: 99; }
  #modal .box { max-width: 780px; margin: 8vh auto; background: var(--card); border: 1px solid var(--bd); border-radius: 10px; padding: 16px 20px; max-height: 72vh; display: flex; flex-direction: column; }
  #modal .hd { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
  #modal pre { white-space: pre-wrap; word-break: break-word; font-family: inherit; font-size: 13px; line-height: 1.65; overflow: auto; margin: 0; }
  .ms { font-variant-numeric: tabular-nums; white-space: nowrap; }
  .toolbar { width: 82%; margin: 0 auto 10px; padding: 0 10px; box-sizing: border-box; display: flex; gap: 8px; row-gap: 6px; align-items: center; flex-wrap: wrap; }
  .toolbar > * { flex: 0 0 auto; }
  .toolbar button, .toolbar select, .toolbar label { white-space: nowrap; }
  #customBox { display: inline-flex; gap: 6px; align-items: center; flex-wrap: wrap; }
  select, button { padding: 4px 10px; border: 1px solid #d0d7de; border-radius: 6px; background: var(--card2); color: var(--tx); font-size: 13px; cursor: pointer; }
  input[type="date"] { padding: 3px 6px; border: 1px solid #d0d7de; border-radius: 6px; background: var(--card2); color: var(--tx); font-size: 12px; font-family: inherit; }
  button:hover { background: #f0f1f3; }
  @media (max-width: 1240px) {
    .stat { flex: 1 1 112px; min-width: 104px; padding: 7px 11px; }
    .stat .num { font-size: 19px; }
    .stat .lbl { font-size: 12px; }
    .ptab td, .ptab th { padding: 3px 10px !important; }
  }
  @media (max-width: 1240px) {
    .brand, .sub, .toolbar, .sechd, .dechd, .tablewrap, .toprow { width: 96%; }
    #pricecard { flex: 1 1 100%; }
    .pcard { width: 100%; padding: 8px 12px; }
    .ptab { width: 100%; min-width: 0; }
    .ptab td, .ptab th { padding: 3px 6px !important; }
  }
  @media (max-width: 900px) {
    .toolbar select, .toolbar button, .toolbar label { font-size: 12px; padding: 3px 8px; }
    .brand .bn { font-size: 20px; }
    .brand .bn .bcn { font-size: 15px; }
    .brand .bsub { font-size: 11px; }
    .sub { font-size: 11px; }
    .stats { gap: 8px; }
    .stat { flex: 1 1 132px; }
    .snippet { max-width: 42vw; }
    #modal .box { width: calc(100% - 24px); max-width: none; margin: 6vh auto; padding: 12px 14px; }
    table.det, table.daytbl { min-width: 680px; }
  }
  @media (max-width: 620px) {
    .brand, .sub, .toolbar, .sechd, .dechd, .tablewrap, .toprow { width: 98%; padding-left: 4px; padding-right: 4px; }
    .stat { flex: 1 1 46%; min-width: 0; }
    .stat .num { font-size: 17px; }
    .brand .bsub { display: none; }
    table.det, table.daytbl { min-width: 560px; }
    th, td { padding: 6px 7px; }
  }
  .empty { padding: 30px; text-align: center; color: var(--tx2); }
  h2.sec { font-size: 14px; font-weight: 700; margin: 18px 0 8px; color: #1f3a52; }
  .sechd h2.sec, .dechd h2.sec { margin: 0; background: rgba(255,255,255,.62); backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px); padding: 5px 16px; border-radius: 16px; box-shadow: 0 1px 6px rgba(20,50,90,.12), inset 0 0 0 1px rgba(255,255,255,.5); }
  .toolbar { background: rgba(255,255,255,.55); backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px); border-radius: 12px; padding-top: 6px; padding-bottom: 6px; box-shadow: 0 1px 6px rgba(20,50,90,.10), inset 0 0 0 1px rgba(255,255,255,.5); }
  .toolbar label { color: #33475e; }
  table.daytbl { margin-bottom: 18px; }
  table.daytbl td.n { font-variant-numeric: tabular-nums; text-align: right; }
  table.daytbl th.n { text-align: right; }
  table.daytbl tr.dayrow td { background: #cfe6f2; font-weight: 600; border-top: 1px solid #a9cfe0; border-bottom: 1px solid #a9cfe0; }
  table.daytbl tr.dayrow { cursor: pointer; user-select: none; }
  table.daytbl tr.dayrow .caret { display: inline-block; width: 12px; color: var(--accent); }
  table.daytbl tr.dayrow td:first-child { border-left: 3px solid var(--accent); }
  table.daytbl tr.mrow td { background: var(--card); }
  table.daytbl tr.mrow td.model { padding-left: 26px; color: var(--tx2); }
  table.daytbl tr.mrow td.model::before { content: "└ "; color: var(--tx3); }
  .unit { color: var(--tx2); font-size: 11px; }
  .badge { display: inline-block; padding: 0 6px; border-radius: 8px; font-size: 11px; }
  .badge.real { background: #dafbe1; color: #116329; }
  .badge.assume { background: #f0f1f3; color: #6a737d; }
</style>
</head>
<body>
<header class="brand">
  <svg viewBox="0 0 47 30" width="42" height="26" aria-hidden="true" style="filter:drop-shadow(0 0 4px rgba(34,184,207,.45))">
    <defs>
      <linearGradient id="xrg" gradientUnits="userSpaceOnUse" x1="17" y1="15" x2="44" y2="15">
        <stop offset="0" stop-color="#0e7490"/>
        <stop offset="1" stop-color="#7de8fa"/>
      </linearGradient>
    </defs>
    <circle cx="3.2" cy="15" r="2.3" fill="#0e7490"/>
    <path d="M3.2 15h14.3" stroke="#0e7490" stroke-width="3.2" stroke-linecap="round"/>
    <circle cx="17.5" cy="15" r="3.4" fill="#0e7490"/>
    <path d="M17.5 15c7 0 9-9.5 26.5-9.8" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
    <path d="M17.5 15h26.5" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
    <path d="M17.5 15c7 0 9 9.5 26.5 9.8" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
    <circle cx="44" cy="5.2" r="2.3" fill="#22b8cf"/>
    <circle cx="44" cy="15" r="2.3" fill="#22b8cf"/>
    <circle cx="44" cy="24.8" r="2.3" fill="#22b8cf"/>
  </svg>
  <div>
    <div class="bn">XRouter<span class="bcn">川流</span></div>
    <div class="bsub">LLM 分流枢纽 · 分流日志｜三条波线对应三级分流：免费池 → Flash → 强模型</div>
  </div>
</header>
<div class="sub">Flash 优先：默认→deepseek-v4.1-flash，极短闲聊→免费池，简短指令（提交/继续/重试类）突破粘性升级回 flash｜跑不通（空响应/截断/工具签名重复/指令打转≥12轮/400/超大上下文）自动升 glm-5.3｜数据源 router.log</div>
<div class="toprow">
  <div class="stats" id="stats"></div>
  <div id="pricecard"></div>
</div>
<div class="sechd"><h2 class="sec">按天统计</h2><button class="mini" onclick="toggleAllDays(this)">展开全部</button></div>
<div class="tablewrap" style="width:82%;margin:0 auto 18px">
<table class="daytbl">
  <thead><tr><th>日期 / 模型</th><th class="n">请求</th><th class="n">输入 tokens</th><th class="n">输出 tokens</th><th class="n">合计 tokens</th><th class="n">缓存率</th><th class="n">估算费用</th><th class="n">耗时</th></tr></thead>
  <tbody id="dayrows"></tbody>
</table>
</div>
<div class="toolbar">
  <select id="range">
    <option value="week" selected>本周（周一起）</option>
    <option value="2w">最近 2 周</option>
    <option value="1m">最近 1 月</option>
    <option value="custom">自定义日期</option>
    <option value="all">全部记录</option>
  </select>
  <span id="customBox" style="display:none">
    <input type="date" id="dfrom"> ~ <input type="date" id="dto">
    <label style="font-size:12px;color:#33475e"><input type="checkbox" id="dall"> 全部记录</label>
  </span>
  <select id="fmodel"><option value="">全部模型</option></select>
  <span id="hitBox" style="display:inline-flex;gap:6px;align-items:center">
    <label for="hit" style="font-size:12px;color:#33475e;white-space:nowrap" title="日志里没有记录缓存命中的历史请求，费用按此比例估算；新请求用上游真实返回值">估算命中率 ⓘ</label>
    <select id="hit" onchange="load()">
      <option value="0.5">50%</option>
      <option value="0.7" selected>70%</option>
      <option value="0.9">90%</option>
      <option value="0.95">95%</option>
    </select>
  </span>
  <button onclick="load()">刷新</button>
  <button onclick="exportReport()">导出 Word 报告</button>
  <button onclick="location.href='/settings'">设置</button>
  <span style="font-size:12px;color:var(--tx3)">导出当前筛选范围（时间+模型）的汇总报告，每 10 秒自动刷新</span>
</div>
<div class="dechd"><h2 class="sec">请求明细（点击分组行展开/收起）</h2>
  <button class="mini" id="mTime" onclick="setDetMode('time')">按时间（小时折叠）</button>
  <button class="mini on" id="mSess" onclick="setDetMode('sess')">按会话（会话折叠）</button>
  <button class="mini" onclick="toggleAllSess(this)">展开全部</button>
</div>
<div class="tablewrap det">
<table class="det">
  <thead><tr>
    <th>时间</th><th>判定</th><th>入口</th><th>实际模型</th><th>分流路径</th><th>用户消息</th><th>输入</th><th>输入单价</th><th>输出</th><th>输出单价</th><th>缓存</th><th>估算</th><th>耗时</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table>
</div>
<script>
const FREE = ["zhipu-glm-4-flash","sf-xing4.0-29b","zhipu-glm-4.7-flash","zhipu-glm-z1-flash","sf-hunyuan-mt-7b"];
const VERDICT = {simple:"默认", complex:"升级", explicit:"直通", vision:"视觉", "glm-first":"GLM优先", "cache-hit":"高命中", "cache-miss":"低命中", free:"免费池", "vision-glm":"识图→GLM", "vision-glm-history":"识图→GLM", "vision-flash":"flash直通", "flash-first":"flash优先"};
// 估算单价（元/百万 tokens，高峰价）。非高峰按 50% 计。
// GLM-5.3 由 GLM Coding 团队标准席位反算：¥598/月 ÷ (66,000 积分/周 × 52/12) = ¥0.00209/积分
//   积分 = (输入×6.9 + 缓存命中×1.7 + 输出×24)/10000 → 输入 ¥1.44 / 缓存 ¥0.356 / 输出 ¥5.02
// deepseek-v4.1-flash 取官方价：未命中 2 / 命中 0.04 / 输出 8（空闲时段半价）
const HIT = { rate: 0.7 };
// 价格来源：platform.qianwenai.com/pricing/api（2026-11-11 抓取），单位 CNY / M tokens
// peak = 忙时小时区间（忙时全价，区间外闲时按平台口径减半）；null = 无闲时优惠恒全价
// 千问平台闲忙时段无周末区分（deepseek 系：闲时 22:00~次日 8:00）
const PRICE = {
  "glm-5.3":             { in: 8,    hit: 2,    out: 28,  peak: null },      // ZHIPU/GLM-5.3
  "deepseek-v4.1-flash": { in: 2,    hit: 0.04, out: 8,   peak: [[8, 22]] }, // 闲时 ¥1/¥4/¥0.02
  "deepseek-v4.1-flash-1017": { in: 2, hit: 0.04, out: 8,   peak: [[8, 22]] },
  "deepseek-v4-pro-0813":{ in: 9,    hit: 0.9,  out: 27,  peak: [[8, 22]] },
  "ZHIPU/GLM-5.3-Flash": { in: 0.8,  hit: 0.23, out: 2.8, peak: null },
  "ZHIPU/GLM-5.3-FlashX":{ in: 2,    hit: 0.57, out: 7,   peak: null },
  "zhipu-glm-4-flash":   { in: 0, hit: 0, out: 0 },
  "sf-xing4.0-29b":      { in: 0, hit: 0, out: 0 },
  "zhipu-glm-4.7-flash": { in: 0, hit: 0, out: 0 },
  "zhipu-glm-z1-flash":  { in: 0, hit: 0, out: 0 },
  "sf-hunyuan-mt-7b":    { in: 0, hit: 0, out: 0 }
};
function isPeak(ts, ranges){
  if(!ts || !ranges) return true;
  const d = new Date(ts.replace(" ", "T"));
  const h = d.getHours() + d.getMinutes() / 60;
  return ranges.some(r => h >= r[0] && h < r[1]);
}
function hitInfo(l){
  const pin = l.prompt_tokens || 0;
  if(l.cached_tokens != null && pin > 0) return { cached: l.cached_tokens, rate: l.cached_tokens / pin, real: true };
  return { cached: pin * HIT.rate, rate: HIT.rate, real: false };
}
function estCost(l){
  const p = PRICE[l.final_model];
  if(!p) return null;
  const pin = l.prompt_tokens || 0, pout = l.completion_tokens || 0;
  if(!pin && !pout) return 0;
  const h = hitInfo(l);
  const miss = pin - h.cached;
  const c = (miss * p.in + h.cached * p.hit + pout * p.out) / 1e6;
  return isPeak(l.ts, p.peak) ? c : c / 2;
}
function priceIn(l){
  const p = PRICE[l.final_model];
  if(!p) return "—";
  if(!p.in && !p.hit) return "免费";
  return `¥${p.in} · 命中¥${p.hit}`;
}
function priceOut(l){
  const p = PRICE[l.final_model];
  if(!p) return "—";
  if(!p.out) return "免费";
  return `¥${p.out}`;
}
function cacheBadge(l){
  if(!l.prompt_tokens) return `<span class="unit">—</span>`;
  const h = hitInfo(l);
  const pct = (h.rate * 100).toFixed(0);
  return h.real
    ? `<span class="badge real">命中 ${pct}%</span>`
    : `<span class="badge assume">假设 ${pct}%</span>`;
}
function money(v){
  if(v == null) return "—";
  if(v === 0) return "0";
  return "¥" + (v < 0.01 ? v.toFixed(4) : v.toFixed(2));
}
let inited = false;
let CUR = [];
let RANGE = { since: "", until: "", label: "", model: "" };
let THRESH = { short_min_chars: 20000, short_max_tokens: 150 };
let MODELS = { fast: "", smart: "" };   // 当前配置的 Flash / 强模型名（用于区分过短回答来源）
const DAY_OPEN = new Set();   // 已展开的日期（刷新后保留）
const SESS_OPEN = new Set();  // 已展开的会话
let LAST_DAYS = [], LAST_SESS = [];
let DET_MODE = "sess";   // 明细分组：sess=按会话 time=按小时
let FREE_PRICE_OPEN = false;  // 单价表免费模型折叠状态（刷新后保留）
const VERDICT_NOTE = {
  "flash-first": "flash 优先：默认分流至 deepseek-v4.1-flash",
  "vision-flash": "flash 直通：带图轮/历史图轮原图直发 flash，免识图延迟",
  "vision-glm": "识图→glm：带图轮因打转/升级改走 glm-5.3",
  "vision-glm-history": "识图→glm：历史有图的续轮走 glm-5.3",
  "glm-first": "glm 优先：升级后粘住或 glm 模式默认",
  "complex": "超大任务：上下文或工具数超硬阈值直升 glm-5.3",
  "explicit": "直通：客户端显式指定真实模型",
  "free": "免费池：极短闲聊由免费模型承接，零消耗",
  "simple": "两级分流默认（旧模式遗留）",
  "cache-hit": "缓存模式·高命中走 flash（旧模式遗留）",
  "cache-miss": "缓存模式·低命中走 glm（旧模式遗留）"
};
async function exportReport(){
  HIT.rate = parseFloat(document.getElementById("hit").value) || 0.7;
  const r = await fetch("/logs" + buildQuery());
  const d = await r.json();
  let logs = d.logs || [];
  if (RANGE.model) logs = logs.filter(l => l.final_model === RANGE.model || l.req_model === RANGE.model);
  if (!logs.length) { alert("所选时间范围内没有日志"); return; }
  const free = logs.filter(l => isFree(l.final_model)).length;
  const tokIn = logs.reduce((s,l)=>s+(l.prompt_tokens||0),0);
  const tokOut = logs.reduce((s,l)=>s+(l.completion_tokens||0),0);
  const cost = logs.reduce((s,l)=>s+(estCost(l)||0),0);
  const avg = logs.length ? Math.round(logs.reduce((s,l)=>s+(l.total_ms||0),0)/logs.length) : 0;
  const realHit = logs.filter(l => l.cached_tokens != null && l.prompt_tokens > 0);
  const hitRate = realHit.length ? (realHit.reduce((s,l)=>s+l.cached_tokens/l.prompt_tokens,0)/realHit.length*100).toFixed(1)+"%" : "—";
  const casc = logs.filter(l => !isFree(l.final_model) && l.verdict !== "explicit");
  const upg = casc.filter(l => (l.path||[]).some(p => p.startsWith("upgrade:"))).length;
  const upRate = casc.length ? (upg/casc.length*100).toFixed(1)+"%" : "—";
  const withTok = logs.filter(l => (l.completion_tokens||0) > 0);
  const shorts = withTok.filter(l => (l.prompt_chars||0) > THRESH.short_min_chars
                                     && (l.completion_tokens||0) < THRESH.short_max_tokens).length;
  const byVerdict = {}, byModel = {}, byDay = {};
  logs.forEach(l => {
    const v = l.verdict || "?"; byVerdict[v] = (byVerdict[v]||0)+1;
    const mk = l.final_model || "-";
    const mo = byModel[mk] || (byModel[mk] = {n:0,i:0,o:0,c:0});
    mo.n++; mo.i += l.prompt_tokens||0; mo.o += l.completion_tokens||0; mo.c += estCost(l)||0;
    const day = (l.ts||"").slice(0,10); if(!day) return;
    const dd = byDay[day] || (byDay[day] = {n:0,i:0,o:0,ms:0,c:0,hit:0});
    dd.n++; dd.i += l.prompt_tokens||0; dd.o += l.completion_tokens||0;
    dd.ms += l.total_ms||0; dd.c += estCost(l)||0; dd.hit += hitInfo(l).cached;
  });
  const table = (head, rows) => `<table border="1" cellspacing="0" style="border-collapse:collapse;font-size:10.5pt">` +
    `<tr style="background:#eef1f5;font-weight:bold">${head.map(h=>`<th style="padding:4px 10px;text-align:left">${h}</th>`).join("")}</tr>` +
    rows.map(cells => `<tr>${cells.map(c=>`<td style="padding:4px 10px">${c}</td>`).join("")}</tr>`).join("") + `</table>`;
  const sumRows = [
    ["总请求数", logs.length + " 条"],
    ["免费池命中", free + " 条（" + (logs.length?(free/logs.length*100).toFixed(1):0) + "%，零消耗）"],
    ["团队网关额度", (logs.length-free) + " 条"],
    ["输入 tokens", tokIn.toLocaleString()],
    ["输出 tokens", tokOut.toLocaleString()],
    ["缓存命中率（上游真实返回）", hitRate],
    ["升级率（Flash 跑不动自动换强模型的占比）", upRate + "（" + upg + "/" + casc.length + "）"],
    ["过短回答（大会话输出不足 " + THRESH.short_max_tokens + " tokens，观察项）", shorts + " 条"],
    ["平均响应耗时", avg + " ms"],
    ["估算费用（按命中率 " + (HIT.rate*100).toFixed(0) + "% 口径）", money(cost)],
  ];
  const verdictRows = Object.entries(byVerdict).sort((a,b)=>b[1]-a[1])
    .map(([v,c]) => [ (VERDICT[v]||v), esc(v), c + "（" + (c/logs.length*100).toFixed(1) + "%）", esc(VERDICT_NOTE[v]||"") ]);
  const modelRows = Object.entries(byModel).sort((a,b)=>b[1].c-a[1].c)
    .map(([mk,x]) => [esc(mk), x.n, x.i.toLocaleString(), x.o.toLocaleString(), money(x.c), (x.c/cost*100).toFixed(1)+"%"]);
  const dayRows = Object.keys(byDay).sort().reverse().map(day => {
    const v = byDay[day];
    return [esc(day), v.n, v.i.toLocaleString(), v.o.toLocaleString(),
            v.i?(v.hit/v.i*100).toFixed(0)+"%":"—", money(v.c), (v.ms/1000).toFixed(0)+"s"];
  });
  const period = RANGE.since ? `${RANGE.since} ~ ${RANGE.until}` : "全部记录";
  const html = `<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40"><head><meta charset="utf-8"><title>XRouter·川流 使用报告</title>
<!--[if gte mso 9]><xml><w:WordDocument><w:View>Print</w:View><w:Zoom>100</w:Zoom></w:WordDocument></xml><![endif]-->
<style>body{font-family:"微软雅黑",sans-serif;color:#1f2328} h1{font-size:18pt} h2{font-size:13pt;border-bottom:2px solid #d0d7de;padding-bottom:4px} .sub{color:#57606a;font-size:10pt} td,th{font-size:10.5pt}</style></head><body>
<h1>XRouter·川流 使用报告</h1>
<p class="sub">统计区间：${esc(period)}（${esc(RANGE.label||"自定义")}）｜生成时间：${new Date().toLocaleString("zh-CN")}｜数据源：XRouter·川流 分流日志（${logs.length} 条${RANGE.model ? "，模型筛选：" + esc(RANGE.model) : ""}）</p>
<h2>一、总量汇总</h2>
${table(["指标","数值"], sumRows)}
<h2>二、分流策略分布</h2>
${table(["策略","标识","请求数","说明"], verdictRows)}
<h2>三、模型分布（按费用排序）</h2>
${table(["模型","请求数","输入 tokens","输出 tokens","估算费用","费用占比"], modelRows)}
<h2>四、按天统计</h2>
${table(["日期","请求","输入 tokens","输出 tokens","缓存率","估算费用","耗时"], dayRows)}
<p class="sub">注：费用按千问平台公示单价（platform.qianwenai.com/pricing/api）与缓存命中率估算，闲时（deepseek 系 22:00~次日8:00）减半，非实际账单；免费池命中不计费。</p>
<p class="sub">© 2026 XRouter·川流 (XRouter) contributors · MIT License</p>
</body></html>`;
  const blob = new Blob(["\ufeff" + html], {type: "application/msword"});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `XRouter使用报告_${RANGE.since||"全部"}${RANGE.until ? "_" + RANGE.until : ""}.doc`;
  document.body.appendChild(a); a.click(); a.remove();
  URL.revokeObjectURL(a.href);
}
function toggleDay(day){
  if (DAY_OPEN.has(day)) DAY_OPEN.delete(day); else DAY_OPEN.add(day);
  const open = DAY_OPEN.has(day);
  document.querySelectorAll('tr.mrow[data-day="'+day+'"]').forEach(tr => { tr.style.display = open ? "table-row" : "none"; });
  const head = document.querySelector('tr.dayrow[data-day="'+day+'"]');
  if (head) head.querySelector(".caret").textContent = open ? "▾" : "▸";
}
function toggleAllDays(btn){
  const all = LAST_DAYS.length && LAST_DAYS.every(d => DAY_OPEN.has(d));
  if (all) LAST_DAYS.forEach(d => DAY_OPEN.delete(d)); else LAST_DAYS.forEach(d => DAY_OPEN.add(d));
  document.querySelectorAll('tr.mrow').forEach(tr => { tr.style.display = all ? "none" : "table-row"; });
  document.querySelectorAll('tr.dayrow .caret').forEach(c => { c.textContent = all ? "▸" : "▾"; });
  btn.textContent = all ? "展开全部" : "收起全部";
}
function toggleSess(key){
  if (SESS_OPEN.has(key)) SESS_OPEN.delete(key); else SESS_OPEN.add(key);
  const open = SESS_OPEN.has(key);
  document.querySelectorAll('tr.crow[data-sess="'+key+'"]').forEach(tr => { tr.style.display = open ? "table-row" : "none"; });
  const head = document.querySelector('tr.srow[data-sess="'+key+'"]');
  if (head) head.querySelector(".caret").textContent = open ? "▾" : "▸";
}
function setDetMode(m){
  if (DET_MODE === m) return;
  DET_MODE = m;
  document.getElementById("mTime").className = "mini" + (m === "time" ? " on" : "");
  document.getElementById("mSess").className = "mini" + (m === "sess" ? " on" : "");
  load();
}
function toggleAllSess(btn){
  const all = LAST_SESS.length && LAST_SESS.every(k => SESS_OPEN.has(k));
  if (all) LAST_SESS.forEach(k => SESS_OPEN.delete(k)); else LAST_SESS.forEach(k => SESS_OPEN.add(k));
  document.querySelectorAll('tr.crow').forEach(tr => { tr.style.display = all ? "none" : "table-row"; });
  document.querySelectorAll('tr.srow .caret').forEach(c => { c.textContent = all ? "▸" : "▾"; });
  btn.textContent = all ? "展开全部" : "收起全部";
}
function showMsg(idx){
  const l = CUR[idx]; if(!l) return;
  document.getElementById("modalBody").textContent = l.user_text || l.snippet || "(无内容)";
  document.getElementById("modal").style.display = "block";
}
function closeModal(){ document.getElementById("modal").style.display = "none"; }
function copyMsg(){
  const t = document.getElementById("modalBody").textContent;
  navigator.clipboard?.writeText(t).then(()=>{ document.querySelector("#modal .hd b").textContent = "已复制"; setTimeout(()=>{document.querySelector("#modal .hd b").textContent = "用户消息";}, 1200); });
}
function isFree(m){ return FREE.includes(m); }
function esc(s){ return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;"); }
function pathCn(step){
  // 分流路径标记 → 中文（原英文放 title，hover 可见）
  let m;
  if ((m = step.match(/^fallback:(.+)$/))) return `兜底切换→${m[1]}`;
  if ((m = step.match(/^retry5xx:(\d+)$/))) return `上游${m[1]}错误·重试`;
  if ((m = step.match(/^giveup:(\d+)$/))) return `重试后放弃(${m[1]})`;
  if (step === "retry:strip-images") return "图片被拒·剥图重试";
  if ((m = step.match(/^acctfail:(.+)$/))) return `${m[1]}·账户异常跳过`;
  if ((m = step.match(/^upgrade:http400\((.+)\)$/))) return `上游400·当轮升级换${m[1]}`;
  if (step === "inject-desc") return "注入图片描述";
  if (step === "inject-desc(fallback)") return "兜底前注入图片描述";
  if ((m = step.match(/^describe:(\d+)$/))) return `识图${m[1]}张`;
  if (step === "new-img:flash") return "新图·flash直通";
  if ((m = step.match(/^upgrade:toolloop\((\d+)\)$/))) return `升级·工具调用重复${m[1]}次`;
  if ((m = step.match(/^upgrade:depth\((\d+)\)$/))) return `升级·当前指令${m[1]}条未收敛`;
  if ((m = step.match(/^upgrade:msgcount\((\d+)\)$/))) return `升级·消息数${m[1]}（旧口径）`;
  if (step === "upgrade:sticky") return "升级粘住·本会话后续走glm";
  if (step === "upgrade:complex") return "升级·超大上下文或工具过多";
  if ((m = step.match(/^flashfail:empty$/))) return "flash空响应·已记账";
  if ((m = step.match(/^flashfail:(\w+)$/))) return `flash截断·已记账`;
  if (step === "flash-mode:glm") return "历史图轮·flash模式走glm";
  return null;
}
function pathHtml(p){
  return p.map(step => {
    let cls = "skip", txt = step;
    if (step.startsWith("hit:")) { cls="hit"; }
    else if (step.startsWith("fail:")) { cls="fail"; }
    else if (step.startsWith("forward:")) { cls="hit"; }
    const cn = pathCn(step);
    return `<span class="${cls}" ${cn ? `title="${esc(step)}"` : ""}>${esc(cn || step)}</span>`;
  }).join(" → ");
}
function fmtSize(l){
  let parts = [];
  if (l.prompt_chars) parts.push(l.prompt_chars + " 字");
  if (l.messages > 1) parts.push(l.messages + " 轮");
  if (l.tools) parts.push(l.tools + " 工具");
  return parts.join(" · ") || "-";
}
function fmtDate(d){
  return d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" + String(d.getDate()).padStart(2, "0");
}
function buildQuery(){
  const v = document.getElementById("range").value;
  const today = new Date();
  let since = "", until = "";
  if(v === "week"){
    const dow = (today.getDay() + 6) % 7;          // 0 = 周一
    const mon = new Date(today); mon.setDate(today.getDate() - dow);
    since = fmtDate(mon); until = fmtDate(today);
  } else if(v === "2w"){
    const s = new Date(today); s.setDate(today.getDate() - 13);
    since = fmtDate(s); until = fmtDate(today);
  } else if(v === "1m"){
    const s = new Date(today); s.setDate(today.getDate() - 29);
    since = fmtDate(s); until = fmtDate(today);
  } else if(v === "custom"){
    if(!document.getElementById("dall").checked){
      since = document.getElementById("dfrom").value;
      until = document.getElementById("dto").value;
    }
  }
  let q = "?limit=0&format=json";
  if(since) q += "&since=" + since;
  if(until) q += "&until=" + until;
  const sel = document.getElementById("range");
  RANGE = { since, until,
            label: (sel.options[sel.selectedIndex]||{}).text || "",
            model: document.getElementById("fmodel").value };
  return q;
}
async function load(){
  HIT.rate = parseFloat(document.getElementById("hit").value) || 0.7;
  const r = await fetch("/logs" + buildQuery());
  const d = await r.json();
  let logs = d.logs;
  if (!inited) {
    const sel = document.getElementById("fmodel"), seen = new Set();
    d.logs.forEach(l => { if (l.req_model) seen.add(l.req_model); if (l.final_model) seen.add(l.final_model); });
    [...seen].sort().forEach(m => { const o = document.createElement("option"); o.value = m; o.textContent = m; sel.appendChild(o); });
    sel.onchange = load; inited = true;
  }
  document.getElementById("hit").onchange = load;
  const f = document.getElementById("fmodel").value;
  if (f) logs = logs.filter(l => l.final_model === f || l.req_model === f);
  // 统计
  const free = logs.filter(l => isFree(l.final_model)).length;
  const paid = logs.length - free;
  const avg = logs.length ? Math.round(logs.reduce((s,l)=>s+(l.total_ms||0),0)/logs.length) : 0;
  const tokIn = logs.reduce((s,l)=>s+(l.prompt_tokens||0),0);
  const tokOut = logs.reduce((s,l)=>s+(l.completion_tokens||0),0);
  const costTotal = logs.reduce((s,l)=>s+(estCost(l)||0),0);
  const byModel = {};
  logs.forEach(l => byModel[l.final_model] = (byModel[l.final_model]||0)+1);
  const topArr = Object.entries(byModel).sort((a,b)=>b[1]-a[1]).slice(0,3);
  const top = topArr.map(([m,c]) => `${esc(m)}×${c}`).join("，");
  // 升级率：非直通非免费池的级联流量中，因升级条件（path 含 upgrade:）走了 glm 的占比
  const casc = logs.filter(l => !isFree(l.final_model) && l.verdict !== "explicit");
  const upg = casc.filter(l => (l.path||[]).some(p => p.startsWith("upgrade:"))).length;
  const upRate = casc.length ? (upg/casc.length*100).toFixed(1)+"%" : "—";
  // 过短回答（L2 观察项）拆两个口径：全部过短 vs 疑似偷懒。
  // 全部过短 = 大会话（>20k 字符）+ 输出 <150 tokens；实测 90%+ 来自强模型的 Agent
  // 快速确认（"已完成"类回复），属正常。疑似偷懒 = 过短且最终由 Flash/免费池回答
  // （这些没有强模型兜底），才是需要抽查的部分。
  const withTok = logs.filter(l => (l.completion_tokens||0) > 0);
  const shortsArr = withTok.filter(l => (l.prompt_chars||0) > THRESH.short_min_chars
                                        && (l.completion_tokens||0) < THRESH.short_max_tokens);
  const shorts = shortsArr.length;
  const shortsSus = shortsArr.filter(l => MODELS.fast
      && (l.final_model === MODELS.fast || isFree(l.final_model))).length;
  const shortRate = withTok.length ? (shortsSus/withTok.length*100).toFixed(1)+"%" : "—";
  document.getElementById("stats").innerHTML = `
    <div class="stat"><div class="num">${logs.length}</div><div class="lbl">总请求
      <span class="exp">当前筛选范围内、经本机分流器转发的全部请求数（含免费池、团队网关两条链路）。</span></div></div>
    <div class="stat free"><div class="num">${free}</div><div class="lbl">免费命中
      <span class="exp">被 L1 免费模型接住的极短闲聊条数。这部分请求不消耗任何额度、不产生费用。</span></div></div>
    <div class="stat paid"><div class="num">${paid}</div><div class="lbl">走团队额度
      <span class="exp">交给团队网关（Flash / 强模型）处理的请求数，会消耗团队额度并计入费用估算。</span></div></div>
    <div class="stat"><div class="num">${avg}ms</div><div class="lbl">平均耗时
      <span class="exp">每条请求从发出到接收完毕的平均毫秒数（含流式输出的全过程），越低越流畅。</span></div></div>
    <div class="stat"><div class="num">${tokIn.toLocaleString()}</div><div class="lbl">输入 tokens
      <span class="exp">所有请求输入 token 的合计（含被缓存命中的部分）；输入越大上下文越长。</span></div></div>
    <div class="stat"><div class="num">${tokOut.toLocaleString()}</div><div class="lbl">输出 tokens
      <span class="exp">模型生成内容的 token 合计。输出 tokens 通常比输入贵数倍，是费用的主要来源。</span></div></div>
    <div class="stat"><div class="num">${money(costTotal)}</div><div class="lbl">估算费用<span class="pn">（命中 ${(HIT.rate*100).toFixed(0)}%）</span>
      <span class="exp">按公开单价 × 缓存命中率估算的费用，非实际账单。日志已记录真实命中信息的请求按真实值计算；未记录的历史请求按上方工具栏选定的比例估算（越高估得越低）。</span></div></div>
    <div class="stat"><div class="num topnum" style="line-height:1.35">${
        topArr.map(([m,c]) => `<div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(m)}<span style="color:var(--tx3)"> ×${c}</span></div>`).join("") || "-"
      }</div><div class="lbl">模型分布 Top3
      <span class="exp">当前范围内调用次数最多的前三个模型及次数，一行一个。可用工具栏「全部模型」筛选单个模型细看。</span></div></div>
    <div class="stat"><div class="num">${upRate}</div>
      <div class="lbl">升级率<span class="pn">（自动换强模型）</span>
        <span class="exp">便宜的 Flash 遇到复杂任务、工具打转或上下文过大时，自动切到强模型的请求占比（${upg}/${casc.length}）。健康 &lt;10%；偏高说明 Flash 扛不住，可在设置页调高升级阈值来降低升级率。</span></div></div>
    <div class="stat"><div class="num">${shortRate}</div>
      <div class="lbl">疑似偷懒<span class="pn">（过短且走低价模型）</span>
        <span class="exp">大会话（超过 ${Number(THRESH.short_min_chars).toLocaleString()} 字符）输出不足 ${THRESH.short_max_tokens} tokens 的过短回答共 ${shorts} 条，其中最终由 Flash/免费池回答的 ${shortsSus} 条才计入本指标——这些没有强模型兜底，需要抽查。强模型给出的过短回复多为 Agent 快速确认（"已完成"类），属正常，不计入。仅作观察提示，不影响分流。</span></div></div>`;
  // 按天统计（含模型明细）
  const byDay = {};
  logs.forEach(l => {
    const day = (l.ts || "").slice(0, 10);
    if (!day) return;
    const d = byDay[day] || (byDay[day] = { n: 0, i: 0, o: 0, ms: 0, c: 0, hit: 0, models: {} });
    const pi = l.prompt_tokens || 0, co = l.completion_tokens || 0, cst = estCost(l) || 0;
    const h = hitInfo(l);
    d.n++; d.i += pi; d.o += co; d.ms += l.total_ms || 0; d.c += cst; d.hit += h.cached;
    const mk = l.final_model || "-";
    const m = d.models[mk] || (d.models[mk] = { n: 0, i: 0, o: 0, ms: 0, c: 0, hit: 0 });
    m.n++; m.i += pi; m.o += co; m.ms += l.total_ms || 0; m.c += cst; m.hit += h.cached;
  });
  LAST_DAYS = Object.keys(byDay).sort().reverse();
  const dayRows = LAST_DAYS.map(day => {
    const v = byDay[day];
    const vh = v.i ? (v.hit / v.i * 100).toFixed(0) + "%" : "—";
    const open = DAY_OPEN.has(day);
    const show = open ? "table-row" : "none";
    let html = `<tr class="dayrow" data-day="${esc(day)}" onclick="toggleDay('${esc(day)}')" title="点击展开/收起当日模型明细">` +
      `<td class="ms"><span class="caret">${open ? "▾" : "▸"}</span><b>${esc(day)}</b></td><td class="n"><b>${v.n}</b></td>` +
      `<td class="n"><b>${v.i.toLocaleString()}</b></td><td class="n"><b>${v.o.toLocaleString()}</b></td>` +
      `<td class="n"><b>${(v.i + v.o).toLocaleString()}</b></td><td class="n"><b>${vh}</b></td>` +
      `<td class="n"><b>${money(v.c)}</b></td><td class="n">${(v.ms / 1000).toFixed(0)}s</td></tr>`;
    html += Object.entries(v.models).sort((a, b) => (b[1].i + b[1].o) - (a[1].i + a[1].o)).map(([mk, x]) => {
      const mh = x.i ? (x.hit / x.i * 100).toFixed(0) + "%" : "—";
      return `<tr class="mrow" data-day="${esc(day)}" style="display:${show}"><td class="ms model">${esc(mk)}</td><td class="n">${x.n}</td>` +
        `<td class="n">${x.i.toLocaleString()}</td><td class="n">${x.o.toLocaleString()}</td>` +
        `<td class="n">${(x.i + x.o).toLocaleString()}</td><td class="n">${mh}</td>` +
        `<td class="n">${money(x.c)}</td><td class="n">${(x.ms / 1000).toFixed(1)}s</td></tr>`;
    }).join("");
    return html;
  }).join("");
  document.getElementById("dayrows").innerHTML = dayRows || `<tr><td colspan="8" class="empty">暂无数据</td></tr>`;
  // 行
  const rowsHtml = (() => {
    const groups = [], gmap = {};
    (CUR = logs.slice().reverse()).forEach((l, idx) => {
      const hk = "h:" + ((l.ts || "").slice(0, 13) || "unknown");
      const k = DET_MODE === "time" ? hk : (l.sess || hk);
      let g = gmap[k];
      if (!g) { g = gmap[k] = { key: k, rows: [] }; groups.push(g); }
      g.rows.push([l, idx]);
    });
    LAST_SESS = groups.map(g => g.key);
    return groups.map(g => {
      const open = SESS_OPEN.has(g.key);
      const first = g.rows[0][0];
      const head = DET_MODE === "time"
        ? `<b>${esc(String(g.key).slice(2))}</b> 时段`
        : esc(String(first.user_text || first.snippet || "(无内容)").slice(0, 70));
      const models = {};
      let cost = 0;
      g.rows.forEach(([l]) => { models[l.final_model || "-"] = (models[l.final_model || "-"] || 0) + 1; cost += estCost(l) || 0; });
      const mm = Object.entries(models).sort((a, b) => b[1] - a[1]).map(([m, c]) => esc(m) + (c > 1 ? "×" + c : "")).join("，");
      let h = `<tr class="srow" data-sess="${esc(g.key)}" onclick="toggleSess('${esc(g.key)}')" title="点击展开/收起该会话">` +
        `<td colspan="6"><span class="caret">${open ? "▾" : "▸"}</span>${head}</td>` +
        `<td colspan="7" style="text-align:right">${g.rows.length} 条｜${mm}｜${money(cost)}</td></tr>`;
      h += g.rows.map(([l, idx]) => `
    <tr class="crow" data-sess="${esc(g.key)}" style="display:${open ? "table-row" : "none"}">
      <td class="ms">${esc(l.ts||"")}</td>
      <td><span class="tag ${esc(l.verdict||"")}">${esc(VERDICT[l.verdict]||l.verdict||"?")}</span></td>
      <td>${esc(l.req_model||"-")}</td>
      <td class="${isFree(l.final_model)?"m-free":"m-paid"}">${esc(l.final_model||"?")}</td>
      <td><div class="path">${pathHtml(l.path||[])}</div></td>
      <td class="snippet" title="点击查看全文" onclick="showMsg(${idx})">${esc(l.snippet||"-")}</td>
      <td class="ms">${l.prompt_tokens!=null?l.prompt_tokens.toLocaleString():"-"}</td>
      <td class="ms unit">${priceIn(l)}</td>
      <td class="ms">${l.completion_tokens!=null?l.completion_tokens.toLocaleString():"-"}</td>
      <td class="ms unit">${priceOut(l)}</td>
      <td>${cacheBadge(l)}</td>
      <td class="ms">${money(estCost(l))}</td>
      <td class="ms">${l.total_ms||"?"}ms</td>
    </tr>`).join("");
      return h;
    }).join("");
  })();
  document.getElementById("rows").innerHTML = rowsHtml || `<tr><td colspan="13" class="empty">暂无日志</td></tr>`;
  document.getElementById("pricecard").innerHTML = priceCardHtml();
}
function priceCardHtml(){
  // 单价卡：右侧独立栏（与统计卡左右并列），load() 每次重渲染；免费模型默认折叠
  const paid = [], free = [];
  Object.entries(PRICE).forEach(([mk,p]) => {
    if (!p.in && !p.out) { free.push(mk); return; }
    const idle = p.peak
      ? `闲时(${p.peak.map(r=>r[0]+"~"+r[1]+"点").join("、")}外)减半 ¥${p.in/2}/¥${p.out/2}/¥${p.hit/2}`
      : "无闲时优惠";
    paid.push(`<tr><td>${esc(mk)}</td><td>¥${p.in}</td><td>¥${p.out}</td><td>¥${p.hit}</td><td style="font-size:12px">${idle}</td></tr>`);
  });
  return `<div class="stat pcard">
    <div class="lbl" style="font-weight:600;color:var(--tx);margin-bottom:4px">主要模型单价（CNY/M tokens，千问平台 2026-11-11 抓取）</div>
    <table class="ptab">
      <tr style="color:var(--tx2);text-align:left"><th style="font-weight:500;padding:1px 6px">模型</th><th style="font-weight:500;padding:1px 6px">输入</th><th style="font-weight:500;padding:1px 6px">输出</th><th style="font-weight:500;padding:1px 6px">缓存命中</th><th style="font-weight:500;padding:1px 6px;font-size:12px">闲时</th></tr>
      ${paid.join("")}
      <tr id="freePriceToggle" onclick="toggleFreePrice()" style="cursor:pointer;user-select:none"><td colspan="5" style="color:var(--free);font-size:12px;padding:3px 6px"><span id="freePriceCaret">${FREE_PRICE_OPEN ? "▾" : "▸"}</span> 免费模型 ${free.length} 个（不计费，点击展开）</td></tr>
      <tbody id="freePriceRows" style="display:${FREE_PRICE_OPEN ? "table-row-group" : "none"}">
        ${free.map(mk=>`<tr><td style="color:var(--free)">${esc(mk)}</td><td colspan="4" style="color:var(--tx2)">免费池，不计费</td></tr>`).join("")}
      </tbody>
    </table>
    <div class="lbl" style="margin-top:5px;font-size:12px">费用 = 未命中输入×输入价 + 命中×缓存价 + 输出×输出价，闲忙按请求时间判定，历史同样重算｜<a href="https://platform.qianwenai.com/pricing/api" target="_blank" style="color:var(--link)">来源</a>（调价需同步）</div>
  </div>`;
}
function toggleFreePrice(){
  FREE_PRICE_OPEN = !FREE_PRICE_OPEN;
  document.getElementById("freePriceRows").style.display = FREE_PRICE_OPEN ? "table-row-group" : "none";
  document.getElementById("freePriceCaret").textContent = FREE_PRICE_OPEN ? "▾" : "▸";
}
(function initRange(){
  const today = new Date();
  const dow = (today.getDay() + 6) % 7;
  const mon = new Date(today); mon.setDate(today.getDate() - dow);
  document.getElementById("dfrom").value = fmtDate(mon);
  document.getElementById("dto").value = fmtDate(today);
  const sel = document.getElementById("range");
  sel.onchange = () => {
    document.getElementById("customBox").style.display = (sel.value === "custom") ? "inline" : "none";
    load();
  };
  ["dfrom", "dto", "dall"].forEach(id => document.getElementById(id).onchange = load);
})();
fetch("/api/config").then(r => r.json()).then(d => { if (d.thresholds) THRESH = Object.assign(THRESH, d.thresholds); if (d.flash) MODELS = { fast: d.flash.model || "", smart: d.smart.model || "" }; })
  .catch(() => {}).finally(() => { load(); setInterval(load, 10000); });
</script>
<div id="modal" onclick="if(event.target===this)closeModal()">
  <div class="box">
    <div class="hd">
      <b style="font-size:14px">用户消息</b>
      <span><button onclick="copyMsg()">复制</button> <button onclick="closeModal()">关闭</button></span>
    </div>
    <pre id="modalBody"></pre>
  </div>
</div>
<div class="ft"><a class="ftbtn" href="/settings">⚙ 设置</a>© 2026 XRouter·川流 (XRouter) contributors · MIT License</div>
</body>
</html>"""


# ---------- 首次启动安装向导 ----------
SETUP_PAGE = r"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8"><title>XRouter·川流 安装向导</title>
<style>
body{font-family:"Segoe UI","微软雅黑",sans-serif;background:#f6f8fa;color:#1f2328;margin:0}
:root{--brand:#0e7490;--brand-mid:#22b8cf}
.wrap{max-width:760px;margin:40px auto;padding:0 16px}
.brand{display:flex;align-items:center;gap:12px;margin-bottom:6px}
.brand .bn{font-size:24px;font-weight:700;letter-spacing:-.5px;line-height:1.1}
.brand .bn .bcn{margin-left:8px;font-size:17px;font-weight:600;background:linear-gradient(90deg,var(--brand),var(--brand-mid));-webkit-background-clip:text;background-clip:text;color:transparent}
.brand .bsub{font-size:12px;color:#6a737d;margin-top:2px}
.ft{margin-top:28px;padding-top:12px;border-top:1px solid #e1e4e8;color:#9aa2ac;font-size:12px;text-align:center}
.brand svg .fl{stroke-dasharray:11 5;animation:xflow 1.1s linear infinite}@keyframes xflow{to{stroke-dashoffset:-16}}
p.sub{color:#57606a;font-size:13px;margin-top:4px}
.card{background:#fff;border:1px solid #e1e4e8;border-radius:8px;padding:16px 20px;margin:14px 0}
.card h2{font-size:14px;margin:0 0 10px}
label{display:block;font-size:12px;color:#57606a;margin:8px 0 3px}
input{width:100%;box-sizing:border-box;padding:7px 10px;border:1px solid #d0d7de;border-radius:6px;font-size:13px}
input:focus{outline:2px solid #0969da33;border-color:#0969da}
.row{display:flex;gap:12px}.row>div{flex:1}
button{padding:8px 18px;border:1px solid #d0d7de;border-radius:6px;background:#fff;cursor:pointer;font-size:13px}
button.primary{background:var(--brand);color:#fff;border-color:var(--brand)}
button:disabled{opacity:.5;cursor:default}
details{margin-top:10px}.details{background:#f6f8fa;border:1px solid #e1e4e8;border-radius:8px;padding:10px 16px;margin:14px 0}
summary{cursor:pointer;font-size:13px;color:#57606a}
select{padding:6px 8px;border:1px solid #d0d7de;border-radius:6px;font-size:13px}
.msg{font-size:12px;margin-left:10px;display:inline-block}
.ok{color:#1a7f37}.err{color:#cf222e}
.hint{font-size:11px;color:#8b949e;margin-top:2px}
@media (max-width:720px){
  .wrap{padding:0 10px;margin:18px auto}
  .brand{flex-wrap:wrap;gap:6px}
  .brand .bn{font-size:20px}
  .card{padding:12px 14px}
  .row{flex-wrap:wrap}
  .row>div{flex:1 1 100%}
}
</style></head><body><div class="wrap">
<header class="brand">
  <svg viewBox="0 0 47 30" width="42" height="26" aria-hidden="true" style="filter:drop-shadow(0 0 4px rgba(34,184,207,.45))">
    <defs>
      <linearGradient id="xrg" gradientUnits="userSpaceOnUse" x1="17" y1="15" x2="44" y2="15">
        <stop offset="0" stop-color="#0e7490"/>
        <stop offset="1" stop-color="#7de8fa"/>
      </linearGradient>
    </defs>
    <circle cx="3.2" cy="15" r="2.3" fill="#0e7490"/>
    <path d="M3.2 15h14.3" stroke="#0e7490" stroke-width="3.2" stroke-linecap="round"/>
    <circle cx="17.5" cy="15" r="3.4" fill="#0e7490"/>
    <path d="M17.5 15c7 0 9-9.5 26.5-9.8" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
    <path d="M17.5 15h26.5" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
    <path d="M17.5 15c7 0 9 9.5 26.5 9.8" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
    <circle cx="44" cy="5.2" r="2.3" fill="#22b8cf"/>
    <circle cx="44" cy="15" r="2.3" fill="#22b8cf"/>
    <circle cx="44" cy="24.8" r="2.3" fill="#22b8cf"/>
  </svg>
  <div>
    <div class="bn">XRouter<span class="bcn">川流</span></div>
    <div class="bsub">安装向导 · 约 1 分钟完成配置</div>
  </div>
</header>
<p class="sub">配置两个上游 API：廉价快模型（日常默认）与强力模型（复杂任务/兜底）。兼容任何 OpenAI 兼容接口。</p>
<div class="card"><h2>① Flash 模型 API（廉价快模型，日常默认）</h2>
  <label>接口地址 base_url</label><input id="f_base" placeholder="如 https://api.deepseek.com/v1">
  <p class="hint">填到版本路径为止，router 自动拼接 /chat/completions；也可留空沿用内置团队网关</p>
  <div class="row"><div><label>API Key</label><input id="f_key" type="password" placeholder="sk-...（团队网关透传模式可留空）"></div>
  <div><label>模型名</label><input id="f_model" placeholder="deepseek-v4.1-flash"></div></div>
  <div style="margin-top:10px"><button onclick="testUp('f')">测试连接</button><span class="msg" id="f_msg"></span></div>
</div>
<div class="card"><h2>② 强模型 API（复杂任务 / 兜底）</h2>
  <label>接口地址 base_url</label><input id="s_base" placeholder="如 https://open.bigmodel.cn/api/paas/v4">
  <p class="hint">填到版本路径为止；也可留空沿用内置团队网关</p>
  <div class="row"><div><label>API Key</label><input id="s_key" type="password" placeholder="（团队网关透传模式可留空）"></div>
  <div><label>模型名</label><input id="s_model" placeholder="glm-5.3"></div></div>
  <div style="margin-top:10px"><button onclick="testUp('s')">测试连接</button><span class="msg" id="s_msg"></span></div>
</div>
<details class="details"><summary>高级设置（可全部留默认）</summary>
  <div class="row" style="margin-top:10px">
    <div><label>路由模式</label><select id="route_mode"><option value="flash" selected>flash 优先（推荐）</option><option value="glm">glm 优先</option><option value="cache">按缓存命中分流</option></select></div>
    <div><label>免费池（极短闲聊零消耗）</label><select id="free_pool"><option value="1" selected>启用</option><option value="0">停用</option></select></div>
    <div><label>上游并发上限</label><input id="conc" type="number" value="8" min="1" max="32"></div>
  </div>
  <p class="hint" style="margin-top:12px">免费模型（可选，各平台官方标注免费，有速率/额度限制）：
    智谱 <code>glm-4-flash-250414</code>、<code>glm-4.7-flash</code>、<code>glm-z1-flash</code>；
    硅基流动 <code>XingChenAGI/Xing4.0-29B</code>、<code>tencent/Hunyuan-MT-7B</code>。
    不填 key 则该平台免费模型自动跳过，不影响正常使用。</p>
  <div class="row">
    <div><label>智谱免费 key（可选）</label><input id="fp_zhipu" type="password" placeholder="不填则跳过智谱免费模型"></div>
    <div><label>硅基流动免费 key（可选）</label><input id="fp_sf" type="password" placeholder="不填则跳过硅基免费模型"></div>
  </div>
</details>
<div style="margin:18px 0">
  <button class="primary" id="btnSave" onclick="save()">保存并开始使用</button>
  <span class="msg" id="save_msg"></span>
</div>
</div>
<script>
const FIELDS = ["f","s"];
async function testUp(g){
  const msg = document.getElementById(g+"_msg");
  msg.className = "msg"; msg.textContent = "测试中...";
  const r = await fetch("/api/test-upstream", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({base_url: document.getElementById(g+"_base").value,
      api_key: document.getElementById(g+"_key").value,
      model: document.getElementById(g+"_model").value})});
  const d = await r.json();
  msg.className = "msg " + (d.ok ? "ok" : "err");
  msg.textContent = d.ok ? "连接成功，模型可用" : "失败：" + (d.detail||("HTTP "+d.status));
}
async function save(){
  const btn = document.getElementById("btnSave"); btn.disabled = true;
  const msg = document.getElementById("save_msg");
  msg.className="msg"; msg.textContent = "保存中...";
  const body = {
    flash: {base_url: document.getElementById("f_base").value,
            api_key: document.getElementById("f_key").value,
            model: document.getElementById("f_model").value},
    smart: {base_url: document.getElementById("s_base").value,
            api_key: document.getElementById("s_key").value,
            model: document.getElementById("s_model").value},
    route_mode: document.getElementById("route_mode").value,
    free_pool_enabled: document.getElementById("free_pool").value === "1",
    router_max_concurrent: parseInt(document.getElementById("conc").value) || 8,
    free_pool: {providers: {
      zhipu: {key: document.getElementById("fp_zhipu").value, enabled: true},
      siliconflow: {key: document.getElementById("fp_sf").value, enabled: true}}}};
  const r = await fetch("/api/setup", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body)});
  const d = await r.json();
  if (d.ok) { msg.className="msg ok"; msg.textContent="已保存，正在跳转..."; setTimeout(()=>location.href="/logs", 800); }
  else { msg.className="msg err"; msg.textContent = "保存失败：" + (d.detail||"未知错误"); btn.disabled = false; }
}
</script>
<div class="ft">© 2026 XRouter·川流 (XRouter) contributors · MIT License</div>
</body></html>"""


@app.get("/setup")
async def setup_page():
    """首次启动向导（已有配置则回日志页）。"""
    if HAS_CONFIG:
        return RedirectResponse("/logs", status_code=302)
    return HTMLResponse(SETUP_PAGE)


@app.post("/api/test-upstream")
async def api_test_upstream(request: Request):
    """向导页连通性测试：用填入的 base/key/model 发一个 1-token 请求。"""
    d = await request.json()
    base = (d.get("base_url") or "").strip().rstrip("/")
    key = (d.get("api_key") or "").strip()
    model = (d.get("model") or "").strip()
    if not base or not model:
        return {"ok": False, "detail": "base_url 和模型名不能为空"}
    url = base + ("/chat/completions" if re.search(r"/v\d+$", base) else "/v1/chat/completions")
    h = {"Content-Type": "application/json"}
    if key:
        h["Authorization"] = f"Bearer {key}"
    try:
        r = await flash_client.post(url, json={"model": model, "max_tokens": 1,
            "messages": [{"role": "user", "content": "hi"}]}, headers=h, timeout=20.0)
        ok = r.status_code == 200
        detail = "" if ok else (await r.aread())[:220].decode("utf-8", "ignore")
        return {"ok": ok, "status": r.status_code, "detail": detail}
    except Exception as e:
        return {"ok": False, "detail": f"{type(e).__name__}: {e}"[:220]}


SETTINGS_PAGE = r"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8"><title>XRouter·川流 设置</title>
<style>
body{font-family:"Segoe UI","微软雅黑",sans-serif;background:#f6f8fa;color:#1f2328;margin:0}
:root{--brand:#0e7490;--brand-mid:#22b8cf}
.wrap{max-width:820px;margin:32px auto;padding:0 16px}
.brand{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}
.brand-l{display:flex;align-items:center;gap:12px}
.brand .bn{font-size:24px;font-weight:700;letter-spacing:-.5px;line-height:1.1}
.brand .bn .bcn{margin-left:8px;font-size:17px;font-weight:600;background:linear-gradient(90deg,var(--brand),var(--brand-mid));-webkit-background-clip:text;background-clip:text;color:transparent}
.brand .bsub{font-size:12px;color:#6a737d;margin-top:2px}
.ft{margin-top:28px;padding-top:12px;border-top:1px solid #e1e4e8;color:#9aa2ac;font-size:12px;text-align:center}
.brand svg .fl{stroke-dasharray:11 5;animation:xflow 1.1s linear infinite}@keyframes xflow{to{stroke-dashoffset:-16}}
a{color:#0969da;font-size:13px;text-decoration:none}
.card{background:#fff;border:1px solid #e1e4e8;border-radius:8px;padding:16px 20px;margin:14px 0}
.card h2{font-size:14px;margin:0 0 10px}
label{display:block;font-size:12px;color:#57606a;margin:8px 0 3px}
input{width:100%;box-sizing:border-box;padding:7px 10px;border:1px solid #d0d7de;border-radius:6px;font-size:13px}
input:focus{outline:2px solid #0969da33;border-color:#0969da}
input:disabled{background:#f6f8fa;color:#57606a}
.row{display:flex;gap:12px}.row>div{flex:1}
button{padding:8px 18px;border:1px solid #d0d7de;border-radius:6px;background:#fff;cursor:pointer;font-size:13px}
button.primary{background:var(--brand);color:#fff;border-color:var(--brand)}
button.mini{padding:4px 10px;font-size:12px}
details{margin-top:10px}.details{background:#f6f8fa;border:1px solid #e1e4e8;border-radius:8px;padding:10px 16px;margin:14px 0}
summary{cursor:pointer;font-size:13px;color:#57606a}
select{padding:6px 8px;border:1px solid #d0d7de;border-radius:6px;font-size:13px}
.msg{font-size:12px;margin-left:10px;display:inline-block}
.ok{color:#1a7f37}.err{color:#cf222e}
.hint{font-size:11px;color:#8b949e;margin-top:2px}
@media (max-width:720px){
  .wrap{padding:0 10px;margin:18px auto}
  .brand{flex-wrap:wrap;gap:6px}
  .brand .bn{font-size:20px}
  .brand .bsub{font-size:11px}
  .card{padding:12px 14px}
  .row{flex-wrap:wrap}
  .row>div{flex:1 1 100%}
  .vt td:first-child{width:auto;white-space:normal}
  .flow{gap:6px}
  .lv{font-size:11px;padding:3px 10px}
}
.vt{width:100%;border-collapse:collapse;font-size:13px}
.vt td{padding:5px 6px;border-bottom:1px solid #eef1f4;vertical-align:top}
.vt td:first-child{color:#57606a;white-space:nowrap;width:110px}
code{background:#f0f3f6;padding:2px 6px;border-radius:4px;font-size:12px;font-family:Consolas,monospace}
.intro{background:linear-gradient(135deg,#f2fbfe,#eef6fc);border-color:#cfe6f2}
.intro h2{margin:4px 0 6px}
.intro p{font-size:13px;line-height:1.75;margin:6px 0}
.intro ul{margin:4px 0 6px;padding-left:20px}
.intro li{font-size:13px;line-height:1.75;margin:4px 0}
.flow{display:flex;gap:10px;align-items:center;margin:10px 0 4px;flex-wrap:wrap}
.lv{padding:3px 14px;border-radius:14px;font-size:12px;font-weight:600;white-space:nowrap}
.lv.l1{background:#dafbe1;color:#116329}
.lv.l2{background:#ddf4ff;color:#0969da}
.lv.l3{background:#ffebe9;color:#82071e}
.flow .ar{color:#8b949e;font-weight:600}
</style></head><body><div class="wrap">
<header class="brand">
  <div class="brand-l">
    <svg viewBox="0 0 47 30" width="42" height="26" aria-hidden="true" style="filter:drop-shadow(0 0 4px rgba(34,184,207,.45))">
      <defs>
        <linearGradient id="xrg" gradientUnits="userSpaceOnUse" x1="17" y1="15" x2="44" y2="15">
          <stop offset="0" stop-color="#0e7490"/>
          <stop offset="1" stop-color="#7de8fa"/>
        </linearGradient>
      </defs>
      <circle cx="3.2" cy="15" r="2.3" fill="#0e7490"/>
    <path d="M3.2 15h14.3" stroke="#0e7490" stroke-width="3.2" stroke-linecap="round"/>
      <circle cx="17.5" cy="15" r="3.4" fill="#0e7490"/>
      <path d="M17.5 15c7 0 9-9.5 26.5-9.8" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
      <path d="M17.5 15h26.5" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
      <path d="M17.5 15c7 0 9 9.5 26.5 9.8" fill="none" class="fl" stroke="url(#xrg)" stroke-width="2.8" stroke-linecap="round"/>
      <circle cx="44" cy="5.2" r="2.3" fill="#22b8cf"/>
      <circle cx="44" cy="15" r="2.3" fill="#22b8cf"/>
      <circle cx="44" cy="24.8" r="2.3" fill="#22b8cf"/>
    </svg>
    <div>
      <div class="bn">XRouter<span class="bcn">川流</span></div>
      <div class="bsub">设置中心 · 改动保存即热生效</div>
    </div>
  </div>
  <a href="/logs">← 返回日志页</a>
</header>
<div class="card intro">
  <h2>这是什么</h2>
  <p>XRouter·川流 是一个<b>本机 OpenAI 兼容分流器</b>：客户端（CodeBuddy / 任意 OpenAI SDK）把 Base URL 指向下方"虚拟 API"地址即可，请求先进入分流器，按任务特征自动选择最合适、最便宜的上游模型再原样转发——模型切换、key 注入、失败重试对客户端完全透明，每一次分流路径都可在日志页查看。</p>
  <h2>分流策略（三级水闸，从省到强）</h2>
  <div class="flow"><span class="lv l1">L1 免费池 · 零成本</span><span class="ar">→</span><span class="lv l2">L2 Flash · 快且省</span><span class="ar">→</span><span class="lv l3">L3 强模型 · 兜底</span></div>
  <ul>
    <li><b>L1 免费池</b>——≤400 字符、无工具调用、≤2 条消息的极短闲聊，先尝试智谱 / 硅基流动 / 自定义平台的免费模型；超时或报错自动回落 L2，熔断的源自动跳过冷却。</li>
    <li><b>L2 Flash</b>——日常默认水闸，绝大多数请求走廉价快模型，兼顾速度与成本。</li>
    <li><b>L3 强模型</b>——带图/纯文本会话连续多轮未收敛、同一工具反复调用打转、上下文超长时自动升级接管；Flash 上游故障时也由它兜底，全链路失败才返回错误。</li>
  </ul>
  <p class="hint">路由模式、升级阈值、L2↔L3 调用顺序、免费池开关与自定义平台，均在下方各卡片和"高级设置"中调整，保存即热生效。</p>
</div>
<div class="card"><h2>分流器虚拟 API（客户端里配置这个地址）</h2>
  <table class="vt" id="vt"><tr><td>加载中…</td><td></td></tr></table>
  <p class="hint">在 CodeBuddy / 任意 OpenAI 兼容客户端里把 Base URL 指向上面地址；API Key 随便填一个非空值（如 sk-local），真实 key 由分流器注入。</p>
</div>
<div class="card"><h2>① Flash 模型 API（廉价快模型，日常默认）</h2>
  <label>接口地址 base_url</label><input id="f_base" placeholder="如 https://api.deepseek.com/v1">
  <p class="hint">填到版本路径为止，自动拼接 /chat/completions</p>
  <div class="row"><div><label>API Key</label><input id="f_key" type="password" placeholder="留空保持不变"></div>
  <div><label>模型名</label><input id="f_model"></div></div>
  <details class="details" style="margin-top:8px"><summary>高级参数（OpenAI 兼容，留空 = 不覆盖客户端参数）</summary>
    <div class="row" style="margin-top:8px">
      <div><label>temperature</label><input id="f_temperature" type="number" step="0.1" min="0" max="2" placeholder="0 ~ 2"></div>
      <div><label>top_p</label><input id="f_top_p" type="number" step="0.05" min="0" max="1" placeholder="0 ~ 1"></div>
      <div><label>max_tokens</label><input id="f_max_tokens" type="number" min="1" placeholder="如 8192"></div>
    </div>
    <div class="row">
      <div><label>frequency_penalty</label><input id="f_frequency_penalty" type="number" step="0.1" min="-2" max="2" placeholder="-2 ~ 2"></div>
      <div><label>presence_penalty</label><input id="f_presence_penalty" type="number" step="0.1" min="-2" max="2" placeholder="-2 ~ 2"></div>
    </div>
    <label>扩展参数 JSON（任意 OpenAI 请求体字段，如 {"seed":42}）</label>
    <input id="f_extjson" placeholder='{"seed": 42}'>
    <p class="hint">配置了的字段会覆盖客户端同名参数；留空的字段完全透传。model / stream / messages 不可覆盖。</p>
  </details>
  <div style="margin-top:10px"><button class="mini" onclick="testUp('f')">测试连接</button><span class="msg" id="f_msg"></span></div>
</div>
<div class="card"><h2>② 强模型 API（复杂任务 / 兜底）</h2>
  <label>接口地址 base_url</label><input id="s_base" placeholder="如 https://open.bigmodel.cn/api/paas/v4">
  <p class="hint">填到版本路径为止，自动拼接 /chat/completions</p>
  <div class="row"><div><label>API Key</label><input id="s_key" type="password" placeholder="留空保持不变"></div>
  <div><label>模型名</label><input id="s_model"></div></div>
  <details class="details" style="margin-top:8px"><summary>高级参数（OpenAI 兼容，留空 = 不覆盖客户端参数）</summary>
    <div class="row" style="margin-top:8px">
      <div><label>temperature</label><input id="s_temperature" type="number" step="0.1" min="0" max="2" placeholder="0 ~ 2"></div>
      <div><label>top_p</label><input id="s_top_p" type="number" step="0.05" min="0" max="1" placeholder="0 ~ 1"></div>
      <div><label>max_tokens</label><input id="s_max_tokens" type="number" min="1" placeholder="如 16384"></div>
    </div>
    <div class="row">
      <div><label>frequency_penalty</label><input id="s_frequency_penalty" type="number" step="0.1" min="-2" max="2" placeholder="-2 ~ 2"></div>
      <div><label>presence_penalty</label><input id="s_presence_penalty" type="number" step="0.1" min="-2" max="2" placeholder="-2 ~ 2"></div>
    </div>
    <label>扩展参数 JSON（任意 OpenAI 请求体字段）</label>
    <input id="s_extjson" placeholder='{"seed": 42}'>
    <p class="hint">同 Flash 上游；强模型可给更高的 max_tokens 以承载长输出任务。</p>
  </details>
  <div style="margin-top:10px"><button class="mini" onclick="testUp('s')">测试连接</button><span class="msg" id="s_msg"></span></div>
</div>
<div class="card"><h2>③ 免费模型 API（L1，可选——极短闲聊零消耗）</h2>
  <p class="hint">不填也能正常使用（直接走 Flash）；填了之后 ≤400 字符的闲聊会先尝试免费模型。key 只存本地 config.json，不随安装包分发。</p>
  <p class="hint">免费模型 = 平台官方定价页标注“免费”的模型（有速率/额度限制，由平台决定）：
    智谱 <code>glm-4-flash-250414</code>、<code>glm-4.7-flash</code>、<code>glm-z1-flash</code>；
    硅基流动 <code>XingChenAGI/Xing4.0-29B</code>、<code>tencent/Hunyuan-MT-7B</code>。
    未填 key 的平台自动跳过，其余请求不受影响。</p>
  <div id="freebox">加载中…</div>
  <div style="margin-top:10px"><button class="mini" onclick="addCustomProv()">＋ 添加自定义平台</button>
    <span class="hint" style="margin-left:8px">任意 OpenAI 兼容端点都能挂进免费池（填 base_url + 模型名 + key），保存后立即生效</span></div>
</div>
<details class="details"><summary>高级设置</summary>
  <div class="row" style="margin-top:10px">
    <div><label>路由模式</label><select id="route_mode"><option value="flash">flash 优先（推荐）</option><option value="glm">glm 优先</option><option value="cache">按缓存命中分流</option></select></div>
    <div><label>免费池（极短闲聊零消耗）</label><select id="free_pool"><option value="1">启用</option><option value="0">停用</option></select></div>
    <div><label>上游并发上限</label><input id="conc" type="number" value="8" min="1" max="32"></div>
  </div>
  <p class="hint" style="margin-top:14px"><b>模型调用顺序</b>——L2/L3 请求按此顺序尝试：第 1 优先失败（超时/报错）时自动切到第 2 优先。路由模式决定初始进哪个模型，这里决定兜底方向。</p>
  <div class="row">
    <div><label>第 1 优先</label><select id="fo_1" onchange="foSync(1)"><option value="flash">Flash（快、便宜）</option><option value="smart">强模型（稳、贵）</option></select></div>
    <div><label>第 2 优先（兜底）</label><select id="fo_2" onchange="foSync(2)"><option value="smart">强模型（稳、贵）</option><option value="flash">Flash（快、便宜）</option></select></div>
  </div>
  <p class="hint" style="margin-top:14px"><b>升级判定阈值</b>——决定什么时候把任务从便宜的 Flash 交给强模型。调高 = 更少升级、更省钱，但复杂任务可能被 Flash 拖着做不完；调低 = 更早升级、更稳但更贵。改完保存即生效（无需重启）。</p>
  <div class="row">
    <div><label>带图指令深度</label><input id="th_depth_img" type="number" min="4" max="300">
      <p class="hint">带图会话连续执行多少条指令仍未收敛就升级（默认 24 ≈ 12 轮）</p></div>
    <div><label>纯文本指令深度</label><input id="th_depth_txt" type="number" min="4" max="400">
      <p class="hint">纯文本/编码任务同口径（默认 40，编码链更长）</p></div>
  </div>
  <div class="row">
    <div><label>工具重复次数</label><input id="th_tool_rep" type="number" min="2" max="30">
      <p class="hint">同一工具+同一参数重复几次判定为打转并升级（默认 3）</p></div>
    <div><label>上下文上限（字符）</label><input id="th_ctx" type="number" min="10000" step="10000">
      <p class="hint">上下文超过该字符数直接交强模型（默认 800000，接近 Flash 上限）</p></div>
  </div>
  <p class="hint" style="margin-top:14px"><b>过短回答监测</b>——只影响日志页的统计提示（"疑似偷懒"占比），不改变分流行为。</p>
  <div class="row">
    <div><label>会话字符下限</label><input id="th_short_min" type="number" min="1000" step="1000">
      <p class="hint">会话超过该字符数才计入观察（默认 20000）。调低 = 小会话也计入，统计更敏感</p></div>
    <div><label>输出 token 上限</label><input id="th_short_tok" type="number" min="10" step="10">
      <p class="hint">输出少于该 token 数视为过短（默认 150）。调高 = 更多回答被标记</p></div>
  </div>
</details>
<div style="margin:18px 0">
  <button class="primary" id="btnSave" onclick="save()">保存</button>
  <a href="/logs" style="margin-left:12px">返回日志页</a>
  <span class="msg" id="save_msg"></span>
</div>
</div>
<script>
function esc(s){return String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]))}
function copyBtn(txt){return '<button class="mini" onclick="navigator.clipboard.writeText(\''+txt+'\');this.textContent=\'已复制\'">复制</button>'}
async function loadCfg(){
  const r = await fetch("/api/config"); const d = await r.json();
  document.getElementById("f_base").value = d.flash.base_url || "";
  document.getElementById("s_base").value = d.smart.base_url || "";
  document.getElementById("f_model").value = d.flash.model || "";
  document.getElementById("s_model").value = d.smart.model || "";
  document.getElementById("f_key").placeholder = d.flash.has_key ? "已配置（留空保持不变）" : "未配置（团队网关透传模式可留空）";
  document.getElementById("s_key").placeholder = d.smart.has_key ? "已配置（留空保持不变）" : "未配置（团队网关透传模式可留空）";
  document.getElementById("route_mode").value = d.route_mode || "flash";
  document.getElementById("free_pool").value = d.free_pool_enabled ? "1" : "0";
  document.getElementById("conc").value = d.router_max_concurrent || 8;
  const th = TH = d.thresholds || {};
  document.getElementById("th_depth_img").value = th.upgrade_depth_image != null ? th.upgrade_depth_image : 24;
  document.getElementById("th_depth_txt").value = th.upgrade_depth_text != null ? th.upgrade_depth_text : 40;
  document.getElementById("th_tool_rep").value = th.upgrade_tool_repeat != null ? th.upgrade_tool_repeat : 3;
  document.getElementById("th_ctx").value = th.upgrade_prompt_chars != null ? th.upgrade_prompt_chars : 800000;
  document.getElementById("th_short_min").value = th.short_min_chars != null ? th.short_min_chars : 20000;
  document.getElementById("th_short_tok").value = th.short_max_tokens != null ? th.short_max_tokens : 150;
  // 免费池平台卡片
  FPP = (d.free_pool && d.free_pool.providers) || [];
  // L2/L3 高级参数回填
  fillExtra("f_", d.flash.extra || {});
  fillExtra("s_", d.smart.extra || {});
  const fch = d.failover_chain || ["flash", "smart"];
  document.getElementById("fo_1").value = fch[0] || "flash";
  document.getElementById("fo_2").value = fch[1] || "smart";
  renderFree();
  const v = d.virtual || {};
  const mode = (u) => u.auth_mode === "inject"
    ? "<b style='color:#1a7f37'>注入模式</b>（router 保存 key 并注入请求）"
    : "<b style='color:#9a6700'>透传模式</b>（router 不存 key，转发客户端的 Authorization——团队网关用法，属正常状态）";
  const rows = [
    ["Base URL（本机）", "<code>"+esc(v.local)+"</code> " + copyBtn(v.local)],
    ["Base URL（局域网）", v.lan ? "<code>"+esc(v.lan)+"</code> " + copyBtn(v.lan) : "<span class='hint'>未探测到局域网地址</span>"],
    ["端点", "<code>POST " + esc(v.endpoint) + "</code>（OpenAI 兼容）"],
    ["可用模型名", (v.models||[]).map(m=>"<code>"+esc(m)+"</code>").join(" ")],
    ["Flash 认证", mode(d.flash)],
    ["强模型认证", mode(d.smart)],
    ["API Key", "<span class='hint'>任意非空字符串（如 sk-local）——真实上游 key 由分流器注入/透传</span>"]
  ];
  document.getElementById("vt").innerHTML = rows.map(r=>"<tr><td>"+r[0]+"</td><td>"+r[1]+"</td></tr>").join("");
  const app = d.app || {};
  document.getElementById("devstat").innerHTML =
    "本工具采用 <b>Vibecoding</b> 方式开发（AI 生成 + 人工把控的人机结对模式）· 累计开发 <b>" +
    (app.dev_hours || "-") + "</b> 小时 · 代码规模 <b>" + (app.code_lines || "-") +
    "</b> 行（随开发动态更新）";
}
let FPP = [], FREEPROV = {}, TH = {};
function renderFree(){
  document.getElementById("freebox").innerHTML = FPP.map(p => {
    FREEPROV[p.id] = {base: p.base, model: (p.models||[])[0]};
    const ph = p.has_key ? (p.key_from_env ? "环境变量已配置（留空保持不变）" : "已配置（留空保持不变）") : "未配置（不填则跳过该平台）";
    const customEdit = p.custom ? `
      <div class="row" style="margin-top:6px">
        <div><label>平台名称</label><input id="fp_${p.id}_name" value="${esc(p.name||"")}" placeholder="如 我的网关"></div>
        <div style="flex:2"><label>base_url（到版本路径为止）</label><input id="fp_${p.id}_base" value="${esc(p.base||"")}" placeholder="如 https://api.xx.com/v1"></div>
      </div>
      <div class="row">
        <div style="flex:2"><label>模型名（多个用英文逗号分隔）</label><input id="fp_${p.id}_models" value="${esc((p.models||[]).join(","))}" placeholder="model-a,model-b"></div>
        <div style="flex:0 0 130px"><label>支持 tools</label><select id="fp_${p.id}_tools"><option value="1">支持</option><option value="0">不支持</option></select></div>
      </div>` : `
      <div style="margin-top:6px;font-size:11px;color:#8b949e">免费模型：${(p.models||[]).map(m=>"<code>"+esc(m)+"</code>").join(" ")} · <a href="${esc(p.homepage)}" target="_blank">申请 key</a></div>`;
    const delBtn = p.custom ? `<button class="mini" onclick="delProv('${p.id}')" style="color:#cf222e">删除</button>` : "";
    const ex = p.extra && Object.keys(p.extra).length ? esc(JSON.stringify(p.extra)) : "";
    return `<div style="border-top:1px solid #eef1f4;padding:10px 0">
      <div class="row"><div style="flex:2"><label>${esc(p.name)}${p.custom ? "（自定义）" : ""}</label>
        <input id="fp_${p.id}_key" type="password" placeholder="${ph}"></div>
        <div style="flex:0 0 130px"><label>启用</label><select id="fp_${p.id}_en"><option value="1">启用</option><option value="0">停用</option></select></div>
      </div>
      ${customEdit}
      <label style="margin-top:6px">参数覆盖 JSON（OpenAI 兼容，可选）</label>
      <input id="fp_${p.id}_json" value="${ex}" placeholder='如 {"temperature":0.7,"max_tokens":512}'>
      <div style="margin-top:4px;font-size:11px">${(p.has_key && p.enabled)
        ? "<span class='ok'>✓ 该平台免费模型已生效（仅 ≤400 字符闲聊走免费池）</span>"
        : "<span class='hint'>未配置 key 或已停用——该平台免费模型不参与分流</span>"}</div>
      <div style="margin-top:6px;display:flex;gap:8px;align-items:center"><button class="mini" onclick="testFree('${p.id}')">测试</button>${delBtn}<span class="msg" id="fp_${p.id}_msg"></span></div>
    </div>`;
  }).join("") || "<div class='hint'>无免费池平台</div>";
  FPP.forEach(p => {
    document.getElementById("fp_"+p.id+"_en").value = p.enabled ? "1" : "0";
    if (p.custom) document.getElementById("fp_"+p.id+"_tools").value = (p.tools === false) ? "0" : "1";
  });
}
function addCustomProv(){
  const id = "custom_" + Date.now();
  FPP.push({id: id, name: "自定义平台", base: "", models: [], enabled: true,
            has_key: false, key_masked: "", key_from_env: false, homepage: "", tools: true, custom: true});
  renderFree();
  document.getElementById("fp_"+id+"_name").focus();
}
function delProv(id){
  FPP = FPP.filter(p => p.id !== id);
  renderFree();
}
function foSync(changed){
  const a = document.getElementById("fo_1"), b = document.getElementById("fo_2");
  if (a.value === b.value) {   // 两级不能相同，联动互换
    if (changed === 1) b.value = a.value === "flash" ? "smart" : "flash";
    else a.value = b.value === "flash" ? "smart" : "flash";
  }
}
function fillExtra(prefix, ex){
  ["temperature","top_p","max_tokens","frequency_penalty","presence_penalty"].forEach(k => {
    const el = document.getElementById(prefix + k);
    if (el) el.value = (ex[k] != null && ex[k] !== "") ? ex[k] : "";
  });
  const rest = Object.fromEntries(Object.entries(ex).filter(([k]) =>
    !["temperature","top_p","max_tokens","frequency_penalty","presence_penalty"].includes(k)));
  const el = document.getElementById(prefix + "extjson");
  if (el) el.value = Object.keys(rest).length ? JSON.stringify(rest) : "";
}
function extraOf(prefix){
  const ex = {};
  ["temperature","top_p","max_tokens","frequency_penalty","presence_penalty"].forEach(k => {
    const v = document.getElementById(prefix + k).value;
    if (v !== "") ex[k] = parseFloat(v);
  });
  const raw = document.getElementById(prefix + "extjson").value.trim();
  if (raw) Object.assign(ex, JSON.parse(raw));   // JSON 解析错误由 save() 捕获提示
  return ex;
}
function provExtra(id){
  const raw = (document.getElementById("fp_"+id+"_json") || {value:""}).value.trim();
  return raw ? JSON.parse(raw) : {};
}
async function testFree(id){
  const msg = document.getElementById("fp_"+id+"_msg");
  const key = document.getElementById("fp_"+id+"_key").value;
  const p = FPP.find(x => x.id === id) || {};
  if (!key && p.has_key) {
    msg.className = "msg"; msg.textContent = "已配置 key，可直接保存；如需重测请填入 key";
    return;
  }
  msg.className = "msg"; msg.textContent = "测试中...";
  const r = await fetch("/api/test-upstream", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({base_url: FREEPROV[id].base, api_key: key, model: FREEPROV[id].model})});
  const d = await r.json();
  msg.className = "msg " + (d.ok ? "ok" : "err");
  msg.textContent = d.ok ? "连接成功" : "失败：" + (d.detail||("HTTP "+d.status));
}
async function testUp(g){
  const msg = document.getElementById(g+"_msg");
  msg.className = "msg"; msg.textContent = "测试中...";
  const r = await fetch("/api/test-upstream", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({base_url: document.getElementById(g+"_base").value,
      api_key: document.getElementById(g+"_key").value,
      model: document.getElementById(g+"_model").value})});
  const d = await r.json();
  msg.className = "msg " + (d.ok ? "ok" : "err");
  msg.textContent = d.ok ? "连接成功，模型可用" : "失败：" + (d.detail||("HTTP "+d.status));
}
async function save(){
  const btn = document.getElementById("btnSave"); btn.disabled = true;
  const msg = document.getElementById("save_msg");
  msg.className="msg"; msg.textContent = "保存中...";
  let body;
  try {
    body = {
      flash: {base_url: document.getElementById("f_base").value,
              api_key: document.getElementById("f_key").value,
              model: document.getElementById("f_model").value,
              extra: extraOf("f_")},
      smart: {base_url: document.getElementById("s_base").value,
              api_key: document.getElementById("s_key").value,
              model: document.getElementById("s_model").value,
              extra: extraOf("s_")},
      route_mode: document.getElementById("route_mode").value,
      free_pool_enabled: document.getElementById("free_pool").value === "1",
      router_max_concurrent: parseInt(document.getElementById("conc").value) || 8,
      failover_chain: [document.getElementById("fo_1").value, document.getElementById("fo_2").value],
      free_pool: {providers: Object.fromEntries(FPP.map(p => {
        const o = {key: document.getElementById("fp_"+p.id+"_key").value,
                   enabled: document.getElementById("fp_"+p.id+"_en").value === "1",
                   extra: provExtra(p.id)};
        if (p.custom) Object.assign(o, {
          name: document.getElementById("fp_"+p.id+"_name").value,
          base: document.getElementById("fp_"+p.id+"_base").value,
          models: document.getElementById("fp_"+p.id+"_models").value,
          tools: document.getElementById("fp_"+p.id+"_tools").value === "1"});
        return [p.id, o];
      }))},
    thresholds: {
      upgrade_depth_image: parseInt(document.getElementById("th_depth_img").value) || TH.upgrade_depth_image || 24,
      upgrade_depth_text: parseInt(document.getElementById("th_depth_txt").value) || TH.upgrade_depth_text || 40,
      upgrade_tool_repeat: parseInt(document.getElementById("th_tool_rep").value) || TH.upgrade_tool_repeat || 3,
      upgrade_prompt_chars: parseInt(document.getElementById("th_ctx").value) || TH.upgrade_prompt_chars || 800000,
      short_min_chars: parseInt(document.getElementById("th_short_min").value) || TH.short_min_chars || 20000,
      short_max_tokens: parseInt(document.getElementById("th_short_tok").value) || TH.short_max_tokens || 150}};
  } catch(e) {
    msg.className = "msg err"; msg.textContent = "保存失败：高级参数 JSON 格式有误（" + e.message + "）";
    btn.disabled = false;
    return;
  }
  const r = await fetch("/api/setup", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body)});
  const d = await r.json();
  msg.className = "msg " + (d.ok ? "ok" : "err");
  msg.textContent = d.ok ? "已保存并生效（key 留空则保持原值）" : "保存失败：" + (d.detail||"未知错误");
  btn.disabled = false;
  if (d.ok) loadCfg();
}
loadCfg();
</script>
<p class="hint" style="text-align:center;margin-top:16px" id="devstat">加载开发统计…</p>
<div class="ft">© 2026 XRouter·川流 (XRouter) contributors · MIT License</div>
</body></html>"""


def _lan_ip() -> str:
    """探测本机局域网 IP（供其他机器访问虚拟 API）。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return ""


@app.get("/settings")
async def settings_page():
    """设置页：编辑上游 API、查看分流器虚拟 API。"""
    return HTMLResponse(SETTINGS_PAGE)


@app.get("/api/config")
async def api_config():
    """当前配置（key 打码）+ 虚拟 API 信息。"""
    def mask(k: str) -> str:
        return ("****" + k[-4:]) if k else ""
    try:
        with io.open(CONFIG_FILE, "r", encoding="utf-8") as _f:
            _cprovs = {k: v for k, v in ((json.load(_f).get("free_pool") or {}).get("providers") or {}).items()
                       if str(k).startswith("custom_") and isinstance(v, dict)}
    except Exception:
        _cprovs = {}
    lan = _lan_ip()
    _dev_hours, _dev_lines = _dev_stats()
    return {
        "app": {"name": APP_NAME, "name_en": APP_NAME_EN, "version": __version__,
                "author": APP_AUTHOR, "copyright": COPYRIGHT,
                "dev_hours": _dev_hours, "code_lines": _dev_lines},
        "flash": {"base_url": CFG["flash"]["base"], "api_key_masked": mask(CFG["flash"]["key"]),
                  "has_key": bool(CFG["flash"]["key"]), "model": CFG["flash"]["model"],
                  "auth_mode": "inject" if CFG["flash"]["key"] else "passthrough",
                  "extra": CFG["flash"].get("extra") or {}},
        "smart": {"base_url": CFG["smart"]["base"], "api_key_masked": mask(CFG["smart"]["key"]),
                  "has_key": bool(CFG["smart"]["key"]), "model": CFG["smart"]["model"],
                  "auth_mode": "inject" if CFG["smart"]["key"] else "passthrough",
                  "extra": CFG["smart"].get("extra") or {}},
        "route_mode": ROUTE_MODE,
        "failover_chain": list(FAILOVER_CHAIN),
        "free_pool_enabled": FREE_POOL_ENABLED,
        "router_max_concurrent": MAX_CONCURRENT,
        "thresholds": {
            "upgrade_depth_image": STUCK_DEPTH_IMAGE,
            "upgrade_depth_text": STUCK_DEPTH_TEXT,
            "upgrade_tool_repeat": TOOL_SIG_REPEAT,
            "upgrade_prompt_chars": PROMPT_TOKEN_THRESHOLD,
            "short_min_chars": SHORT_MIN_CHARS,
            "short_max_tokens": SHORT_MAX_TOKENS,
            "free_pool_max_chars": FREE_POOL_MAX_CHARS,
        },
        "free_pool": {"enabled": FREE_POOL_ENABLED, "providers": [
            {"id": p["id"], "name": p["name"], "homepage": p["homepage"],
             "base": next((s["base"] for s in FREE_POOL if s.get("provider") == p["id"]), ""),
             "models": [s["model"] for s in FREE_POOL if s.get("provider") == p["id"]],
             "enabled": FREE_PROV_CFG.get(p["id"], {}).get("enabled", True),
             "has_key": any(s["key"] for s in FREE_POOL if s.get("provider") == p["id"]),
             "key_masked": mask(FREE_PROV_CFG.get(p["id"], {}).get("key", "")),
             "key_from_env": bool(not FREE_PROV_CFG.get(p["id"], {}).get("key") and
                                  any(s.get("key_env") for s in FREE_POOL if s.get("provider") == p["id"])),
             "extra": FREE_PROV_CFG.get(p["id"], {}).get("extra") or {}}
            for p in FREE_PROVIDERS] + [
            {"id": pid, "name": (pc.get("name") or pid), "homepage": "", "base": pc.get("base", ""),
             "models": [m.strip() for m in str(pc.get("models") or "").split(",") if m.strip()],
             "enabled": bool(pc.get("enabled", True)),
             "has_key": bool((pc.get("key") or "").strip()),
             "key_masked": mask(pc.get("key") or ""), "key_from_env": False,
             "tools": pc.get("tools", True) is not False,
             "extra": pc.get("extra") if isinstance(pc.get("extra"), dict) else {},
             "custom": True}
            for pid, pc in _cprovs.items() if str(pid).startswith("custom_")]},
        "virtual": {
            "local": f"http://127.0.0.1:{LISTEN_PORT}/v1",
            "lan": f"http://{lan}:{LISTEN_PORT}/v1" if lan and not lan.startswith("127.") else "",
            "endpoint": "/v1/chat/completions",
            "models": [CFG["flash"]["model"], CFG["smart"]["model"]],
        },
    }


@app.post("/api/setup")
async def api_setup(request: Request):
    """保存向导/设置配置：写 config.json + 热生效。api_key 留空 = 沿用原值。"""
    global NEED_SETUP
    try:
        d = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "invalid json"})
    f, s = d.get("flash") or {}, d.get("smart") or {}
    for name, g in (("flash", f), ("smart", s)):
        if not (g.get("base_url") or "").strip():
            return JSONResponse(status_code=400, content={
                "ok": False, "detail": "两个上游的 base_url 均不能为空（团队网关模式填内网地址）"})
        if not (g.get("api_key") or "").strip():
            g["api_key"] = CFG[name]["key"]   # 留空 = 保持原 key 不变
        ex = g.get("extra")
        g["extra"] = ex if isinstance(ex, dict) else {}
    fp_provs = (d.get("free_pool") or {}).get("providers") or {}
    saved_fp = {}
    for p in FREE_PROVIDERS:
        cur = FREE_PROV_CFG.get(p["id"], {})
        pc = fp_provs.get(p["id"]) or {}
        k = (pc.get("key") or "").strip()
        saved_fp[p["id"]] = {"key": k or cur.get("key", ""),   # 留空 = 沿用原 key
                             "enabled": bool(pc.get("enabled", True)),
                             "extra": pc.get("extra") if isinstance(pc.get("extra"), dict) else {}}
    # 自定义免费平台：以提交为准整体重建（未提交的即视为删除）
    try:
        with io.open(CONFIG_FILE, "r", encoding="utf-8") as _f:
            _old_custom = {k: v for k, v in ((json.load(_f).get("free_pool") or {}).get("providers") or {}).items()
                           if str(k).startswith("custom_") and isinstance(v, dict)}
    except Exception:
        _old_custom = {}
    for pid, pc in fp_provs.items():
        if not str(pid).startswith("custom_"):
            continue
        base = (pc.get("base") or "").strip().rstrip("/")
        models = (pc.get("models") or "").strip()
        if not base or not models:
            continue
        k = (pc.get("key") or "").strip() or (_old_custom.get(pid, {}).get("key") or "")
        saved_fp[pid] = {"name": (pc.get("name") or pid).strip(),
                         "base": base, "key": k,
                         "models": models, "tools": pc.get("tools", True) is not False,
                         "enabled": bool(pc.get("enabled", True)),
                         "extra": pc.get("extra") if isinstance(pc.get("extra"), dict) else {}}
    fc = d.get("failover_chain")
    failover = fc if (isinstance(fc, list) and len(fc) >= 2 and set(fc) == {"flash", "smart"}) \
        else list(FAILOVER_CHAIN)
    th_in = d.get("thresholds") or {}
    th_saved = {}
    for tk, cur_v in (("upgrade_depth_image", STUCK_DEPTH_IMAGE),
                      ("upgrade_depth_text", STUCK_DEPTH_TEXT),
                      ("upgrade_tool_repeat", TOOL_SIG_REPEAT),
                      ("upgrade_prompt_chars", PROMPT_TOKEN_THRESHOLD),
                      ("short_min_chars", SHORT_MIN_CHARS),
                      ("short_max_tokens", SHORT_MAX_TOKENS),
                      ("free_pool_max_chars", FREE_POOL_MAX_CHARS)):
        try:
            th_saved[tk] = int(th_in.get(tk, cur_v))
        except Exception:
            th_saved[tk] = cur_v
    cfg = {"flash": f, "smart": s,
           "route_mode": (d.get("route_mode") or "flash").strip().lower(),
           "free_pool_enabled": bool(d.get("free_pool_enabled", True)),
           "router_max_concurrent": int(d.get("router_max_concurrent") or 8),
           "failover_chain": failover,
           "free_pool": {"providers": saved_fp},
           "thresholds": th_saved}
    try:
        with io.open(CONFIG_FILE, "w", encoding="utf-8") as out:
            json.dump(cfg, out, ensure_ascii=False, indent=2)
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "detail": f"写配置失败: {e}"})
    load_config()
    log("setup wizard: config saved")
    return {"ok": True}


@app.get("/logs")
async def get_logs(limit: int = 0, format: str = "html", since: str = "", until: str = ""):
    """分流日志查看。since/until 为 YYYY-MM-DD，闭区间；limit=0 表示不限条数。"""
    if NEED_SETUP:
        return HTMLResponse(SETUP_PAGE)
    lines = []
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        pass
    out = []
    for line in lines:
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = d.get("ts", "")
        if since and ts[:10] < since:
            continue
        if until and ts[:10] > until:
            continue
        out.append(d)
    if limit and limit > 0:
        out = out[-limit:]
    if format == "json":
        return {"count": len(out), "logs": out}
    return HTMLResponse(LOG_PAGE)


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    t_start = time.monotonic()
    body = await request.json()
    headers = {
        "Authorization": request.headers.get("authorization", ""),
        "Content-Type": "application/json",
    }
    stream = bool(body.get("stream", False))
    trace = []
    verdict = None

    # 显式指定真实模型 → 直通
    requested = body.get("model", "")
    msgs = body.get("messages") or []
    # 会话状态：flash 模式的打转/升级记账（带图与纯文本共用同一会话指纹）
    skey = _session_key(body)
    sess = _sess(skey)
    sig_loop = _tool_sig_loop(body)
    depth = _instruction_depth(body)
    # 新指令检测：最后一条 user 消息内容变化 → 空响应/截断计数重置。
    # 上一指令攒的失败账不该让新指令直接背上（同一指令重发不重置，无历史客户端不受影响）
    lu = _last_user_text(body)
    lu_hash = hash(lu) if lu else ""
    if lu_hash and sess.get("cmd") != lu_hash:
        sess["cmd"] = lu_hash
        sess["empty"] = 0
        sess["length"] = 0
    if requested in (FAST_MODEL, SMART_MODEL, SPARK_MODEL):
        if requested == SPARK_MODEL and not _spark_available():
            return JSONResponse(status_code=410, content={
                "error": f"{SPARK_MODEL} 已停用（额度 {_spark_used:,}/{SPARK_QUOTA:,}，"
                         f"有效期至 {SPARK_DEADLINE}，key={'已配置' if SPARK_KEY else '缺失'}）"})
        target, reason = requested, "explicit"
    elif has_image(body):
        # 2026-11-11 策略：带图轮默认 flash 直通原图（flash 本身是视觉模型，无需先识图，
        # 省 40-50s 识图延迟且描述无损）；会话"跑不通"后带图轮改走识图→glm。
        keys = _current_image_keys(body)
        new_img = any(k not in sess["imgs"] for k in keys)
        # 记录已见图（无论走哪条路，都算"见过了"）
        for k in keys:
            if k not in sess["imgs"]:
                sess["imgs"].append(k)
        sess["imgs"] = sess["imgs"][-VISION_SEEN_IMGS:]
        if depth >= STUCK_DEPTH_IMAGE or sig_loop >= TOOL_SIG_REPEAT or is_complex(body):
            # 打转保护（优先级最高）：当前指令跑了 12 轮还没收敛（页面历史不计入），
            # 或同一工具调用重复 ≥3 次（签名含参数，参数相同才是真重复），直接上 glm
            n_desc = await describe_images(body, headers)
            if n_desc:
                trace.append(f"describe:{n_desc}")
            if inject_image_descs(body):
                trace.append("inject-desc")
            target, reason = SMART_MODEL, "vision-glm"
            if sig_loop >= TOOL_SIG_REPEAT:
                trace.append(f"upgrade:toolloop({sig_loop})")
            elif depth >= STUCK_DEPTH_IMAGE:
                trace.append(f"upgrade:depth({depth})")
            else:
                trace.append("upgrade:complex")
        elif new_img:
            # 新图 = 新任务：重置失败计数，重新给 flash 机会
            sess["empty"] = 0
            sess["length"] = 0
            sess["upgraded"] = False
            target, reason = FAST_MODEL, "vision-flash"
            trace.append("new-img:flash")
        elif sess["upgraded"]:
            # 旧图 + 已升级：flash 在这张图上跑通过不了，识图→glm
            n_desc = await describe_images(body, headers)
            if n_desc:
                trace.append(f"describe:{n_desc}")
            if inject_image_descs(body):
                trace.append("inject-desc")
            target, reason = SMART_MODEL, "vision-glm"
            trace.append("upgrade:sticky")
        else:
            # 旧图重试（如客户端重发同图）且未触发升级：仍 flash 直通
            target, reason = FAST_MODEL, "vision-flash"
    elif has_image_anywhere(body):
        # 当前轮无图、历史有图（Agent 常见的"继续"轮）。
        # flash 模式：flash 能看历史图直通（重复传图部分走前缀缓存，命中价极低）；
        # 其他模式：glm 优先，glm 收不了图须先识图注入
        #（2026-11-05 第四个坑：否则历史图被占位符顶掉）。
        if (ROUTE_MODE == "flash" and not sess["upgraded"]
                and depth < STUCK_DEPTH_IMAGE and sig_loop < TOOL_SIG_REPEAT
                and not is_complex(body)):
            target, reason = FAST_MODEL, "vision-flash"
        else:
            n_desc = await describe_images(body, headers)
            if n_desc:
                trace.append(f"describe:{n_desc}")
            if inject_image_descs(body):
                trace.append("inject-desc")
            target, reason = SMART_MODEL, "vision-glm-history"
            if sess["upgraded"] or ROUTE_MODE == "flash":
                trace.append("upgrade:sticky" if sess["upgraded"] else "flash-mode:glm")
        # 历史图入账：否则后续带图轮会把历史图误判为"新图"，错误重置升级状态
        for k in _all_image_keys(body):
            if k not in sess["imgs"]:
                sess["imgs"].append(k)
        sess["imgs"] = sess["imgs"][-VISION_SEEN_IMGS:]
    elif ROUTE_MODE == "flash":
        # 全局 flash 优先（2026-11-11 确认）：所有轮默认 deepseek-v4.1-flash，
        # 确定性失败（空响应/截断记账）或当前指令打转/超大上下文才升 glm。
        # 2026-11-12：粘性升级不再是死锁——新指令若是简短操作指令（提交/继续/重试类），
        # 重新给 flash 机会（失败会立即自动升回，代价一次重试）。
        simple = is_simple_command(body, depth)
        if simple and sess["upgraded"]:
            # 新简单指令：重置粘性升级，flash 重新接手
            sess["upgraded"] = False
            sess["empty"] = 0
            sess["length"] = 0
            trace.append("simple-cmd:flash")
        if (depth >= STUCK_DEPTH_TEXT or sig_loop >= TOOL_SIG_REPEAT
                or is_complex(body) or (sess["upgraded"] and not simple)):
            target, reason = SMART_MODEL, "glm-first"
            if sig_loop >= TOOL_SIG_REPEAT:
                trace.append(f"upgrade:toolloop({sig_loop})")
            elif depth >= STUCK_DEPTH_TEXT:
                trace.append(f"upgrade:depth({depth})")
            elif sess["upgraded"]:
                trace.append("upgrade:sticky")
            else:
                trace.append("upgrade:complex")
        else:
            target, reason = FAST_MODEL, "flash-first"
    elif ROUTE_MODE == "glm":
        target, reason = SMART_MODEL, "glm-first"
    elif ROUTE_MODE == "cache":
        # 首轮无历史 → 缓存必然不命中 → GLM 输入便宜 38%；续轮高命中 → DeepSeek 缓存价便宜 7.6 倍
        target, reason = (SMART_MODEL, "cache-miss") if len(msgs) < 3 else (FAST_MODEL, "cache-hit")
    else:
        complex_q = is_complex(body)
        target, reason = (SMART_MODEL, "complex") if complex_q else (FAST_MODEL, "simple")
    verdict = reason

    # 非视觉路径：历史里残留的图片会让 GLM/免费模型直接 400，替换成描述或占位再发
    # 例外：主目标是 deepseek（FAST_MODEL 本身就是识图模型）时透传原图不替换——
    # 2026-11-05 实测 explicit deepseek + 截图被占位符顶掉，agent 误判为"客户端剥离了图片"
    if reason not in ("vision-glm", "vision-glm-history") and target != FAST_MODEL and inject_image_descs(body):
        trace.append("inject-desc")

    def rec(final_model):
        return {
            "verdict": verdict, "req_model": requested, "path": list(trace),
            "final_model": final_model, "stream": stream,
            "sess": (skey or "")[:10],   # 会话指纹前缀：明细表按会话折叠用
            "prompt_chars": estimate_tokens(body.get("messages") or []),
            "messages": len(body.get("messages") or []),
            "tools": len(body.get("tools") or []),
            "snippet": first_user_snippet(body),
            "user_text": first_user_text(body),
        }

    # 免费池只接极短闲聊（≤400 字符、无 tools、非显式/视觉）
    has_tools = bool(body.get("tools"))
    size = estimate_tokens(msgs)
    # 视觉请求留在团队网关上（网关模型不计成本），不落进免费池小模型。
    # 2026-11-05 修正：原写法排除的是 "vision"，但视觉路径实际 reason 是 "vision-glm"，
    # 导致带图的短请求被免费模型抢走，与设计意图不符。
    # 2026-11-11：vision-flash（flash 直通）同样排除——免费池模型无视觉。
    free_ok = (FREE_POOL_ENABLED and not has_tools and len(msgs) <= 2
               and size <= FREE_POOL_MAX_CHARS
               and reason not in ("explicit", "vision-glm", "vision-glm-history", "vision-flash"))

    if stream and free_ok:
        got = await hedge_stream(body, headers, target, trace) if HEDGE_ENABLED \
            else await try_free_stream(body, headers, trace)
        if got is not None:
            it, status, media, src_name = got
            verdict = "free"
            return StreamingResponse(_tap(it, rec(src_name), t_start), status_code=status, media_type=media)
        # 免费池全失败 → 落回 L2

    # 非流式同样先试免费池
    if not stream and free_ok:
        got = await try_free_stream(body, headers, trace)
        if got is not None:
            it, status, media, src_name = got
            chunks = [c async for c in it]
            raw = b"".join(chunks)
            verdict = "free"
            r = rec(src_name)
            r["total_ms"] = int((time.monotonic() - t_start) * 1000)
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = None
            r.update(_usage_fields((payload or {}).get("usage")))
            log_request(r)
            return JSONResponse(status_code=status, content=payload or {"raw": raw.decode(errors="replace")})

    # L2/L3：主目标 → 按高级设置的调用顺序兜底（默认 glm ↔ deepseek 互备）
    # 2026-11-05 起不再兜底 spark-x2.6：它是纯文本模型，兜底时必然把图片替换成
    # [图片内容已省略]，让 AGENT 误判为"客户端剥离了图片"。网关模型不计成本，
    # glm/deepseek 互备已足够；上游全挂时宁可返回错误，也不静默降级到无视觉能力的模型。
    _names = {"flash": FAST_MODEL, "smart": SMART_MODEL}
    chain = [target]
    for _t in FAILOVER_CHAIN:
        _m = _names.get(_t)
        if _m and _m not in chain:
            chain.append(_m)
    if stream and "stream_options" not in body:
        body["stream_options"] = {"include_usage": True}
    log(f"-> {target} ({reason})")

    last_status, last_detail = 502, b"all upstreams failed"
    for idx, cand in enumerate(chain):
        if idx:
            trace.append(f"fallback:{cand}")
            # vision-flash 失败换 glm：glm 收不了图，先识图注入再发，
            # 否则 glm 400 后 inject 只会塞占位符（无描述），agent 误判"图片被剥"
            if cand == SMART_MODEL and reason == "vision-flash" and has_image_anywhere(body):
                n_desc = await describe_images(body, headers)
                if n_desc:
                    trace.append(f"describe:{n_desc}")
                inject_image_descs(body)
                trace.append("inject-desc(fallback)")
        up_client, up_headers, api_path = _upstream_for(cand, headers)
        body["model"] = cand
        _apply_extra(body, (CFG["smart"] if cand == SMART_MODEL else CFG["flash"]).get("extra"))

        if stream:
            await _acquire_slot(trace)
            resp = None
            for attempt in range(3):
                req = up_client.build_request("POST", api_path, json=body, headers=up_headers)
                resp = await up_client.send(req, stream=True)
                if resp.status_code < 500:
                    break
                last_status, last_detail = resp.status_code, (await resp.aread())[:300]
                await resp.aclose()
                resp = None
                if attempt < 2:
                    trace.append(f"retry5xx:{last_status}")
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    trace.append(f"giveup:{last_status}")
            if resp is None:
                continue
            if resp.status_code == 400:
                d = (await resp.aread())[:300]
                await resp.aclose()
                if b"image" in d.lower() and cand != FAST_MODEL and inject_image_descs(body):
                    trace.append("retry:strip-images")
                    req = up_client.build_request("POST", api_path, json=body, headers=up_headers)
                    resp = await up_client.send(req, stream=True)
                elif _is_account_error(d):
                    # 账户级错误（欠费/额度异常）：换模型大概率同样失败，但仍尝试链路
                    # 下一个模型，避免把底层错误直接透传给客户端
                    last_status, last_detail = 400, d
                    trace.append(f"acctfail:{cand}")
                    _upstream_sem.release()
                    continue
                elif cand == FAST_MODEL and reason != "explicit":
                    # flash 直通轮 400（工具调用格式错等）：跑不通信号，
                    # 当轮升级并换 glm（循环开头会先识图注入）
                    _mark_upgraded(skey)
                    trace.append(f"upgrade:http400({cand})")
                    last_status, last_detail = 400, d
                    _upstream_sem.release()
                    continue
                else:
                    _upstream_sem.release()
                    return JSONResponse(status_code=400,
                                        content={"error": d.decode("utf-8", "ignore")})
            if resp.status_code != 200:
                last_status, last_detail = resp.status_code, b""
                await resp.aclose()
                _upstream_sem.release()
                continue
            return StreamingResponse(
                _tap(resp.aiter_raw(), rec(cand), t_start,
                     closer=lambda: _close_and_release(resp), skey=skey),
                status_code=resp.status_code,
                media_type=resp.headers.get("content-type", "text/event-stream"),
            )

        await _acquire_slot(trace)
        resp, payload = None, None
        try:
            for attempt in range(3):
                resp = await up_client.post(api_path, json=body, headers=up_headers)
                if resp.status_code < 500:
                    break
                last_status = resp.status_code
                if attempt < 2:
                    trace.append(f"retry5xx:{last_status}")
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    trace.append(f"giveup:{last_status}")
            if resp.status_code == 400 and "image" in resp.text.lower() and cand != FAST_MODEL and inject_image_descs(body):
                trace.append("retry:strip-images")
                resp = await up_client.post(api_path, json=body, headers=up_headers)
            if resp.status_code == 400 and cand == FAST_MODEL and reason != "explicit":
                # flash 直通轮 400：跑不通信号，记账升级（循环会自动换 glm，
                # 循环开头的 fallback 识图注入对非流式同样生效）
                _mark_upgraded(skey)
                trace.append(f"upgrade:http400({cand})")
            if resp.status_code == 200:
                payload = resp.json()
            else:
                last_status = resp.status_code
        finally:
            _upstream_sem.release()
        if payload is not None:
            _mirror_usage(payload.get("usage"))
            r = rec(cand)
            r["total_ms"] = int((time.monotonic() - t_start) * 1000)
            r.update(_usage_fields(payload.get("usage")))
            if skey and cand == FAST_MODEL and reason != "explicit":
                v = _flash_payload_verdict(payload)
                if v:
                    _note_flash_fail(skey, v)
                    r["path"].append(f"flashfail:{v}")
            log_request(r)
            return JSONResponse(status_code=200, content=payload)

    trace.append("all-failed")
    return JSONResponse(status_code=last_status,
                        content={"error": last_detail.decode("utf-8", "ignore") or "all upstreams failed"})


if __name__ == "__main__":
    import uvicorn
    load_spark_usage()
    load_config()
    print(f"{APP_NAME} v{__version__} — LLM 分流枢纽（三条波线：免费池 → Flash → 强模型）")
    print(f"  {COPYRIGHT}")
    print(f"  listen: {LISTEN_HOST}:{LISTEN_PORT}"
          + ("  [SETUP REQUIRED: open http://%s:%d/setup]" % (LISTEN_HOST, LISTEN_PORT) if NEED_SETUP else ""))
    for src in FREE_POOL:
        state = "ENABLED" if src["key"] else "disabled (no key)"
        tc = "tools:Y" if src.get("tools") else "tools:N"
        print(f"  L1 free: {src['name']:24s} {state:18s} {tc}")
    print(f"  L2 fast : {FAST_MODEL} @ {CFG['flash']['base']}")
    print(f"  L3 smart: {SMART_MODEL} @ {CFG['smart']['base']}")
    print(f"  L1 free pool: {'ENABLED' if FREE_POOL_ENABLED else 'DISABLED (延迟优先)'}"
          f"{' + hedge' if HEDGE_ENABLED else ''}  (tools 上限 {TOOLS_FREE_MAX_CHARS} 字符)")
    print(f"  主分流模式: {ROUTE_MODE}  (glm=GLM优先 / cache=按缓存估 / deepseek=两级)")
    print(f"  网关并发上限: {MAX_CONCURRENT}（超出排队）")
    print("  兜底链: glm-5.3 <-> deepseek-v4.1-flash；spark-x2.6 不参与任何自动兜底（仅手动显式调用）")
    print(f"  升级条件: 空响应{VISION_EMPTY_LIMIT}次/截断{VISION_LENGTH_LIMIT}次/"
          f"指令深度≥{STUCK_DEPTH_IMAGE}(带图)/{STUCK_DEPTH_TEXT}(纯文本)/工具签名重复{TOOL_SIG_REPEAT}次/"
          f"上游400/超大上下文 → 升级 glm；新图重置失败计数")
    print(f"  TTFB timeout: {FREE_TTFB_TIMEOUT}s")
    uvicorn.run(app, host=LISTEN_HOST, port=LISTEN_PORT, log_level="warning")
