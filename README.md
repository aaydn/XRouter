<div align="center">

<img src="assets/logo.png" alt="XRouter·川流" width="160">

# XRouter·川流

**本机运行的 LLM 三级分流网关：免费池 → Flash → 强模型，按"能省则省、不行就升级"自动选路**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Platform](https://img.shields.io/badge/platform-Windows%2010%2F11-lightgrey)
![Python](https://img.shields.io/badge/python-3.9%2B-green)
![Release](https://img.shields.io/badge/release-v1.0.6-orange)

</div>

客户端只需把 Base URL 指向 `http://127.0.0.1:3100/v1`，其余由本工具自动完成——模型切换、key 注入、失败重试对客户端完全透明。

## 特性

- **三级分流**：L1 免费池（首字节超时即切换）/ L2 Flash / L3 强模型，命中空响应、截断、指令过深、工具签名重复、上游 400 等条件自动升级
- **OpenAI 兼容**：暴露 `/v1/chat/completions`、`/v1/models`，客户端（任意 OpenAI SDK / AI 编码助手）无需改造即可接入
- **日志页**：逐请求分层路径、实际模型、耗时、token、缓存命中与费用估算
- **设置页**：上游 OpenAI 标准高级参数（temperature/top_p/max_tokens/惩罚项 + 扩展 JSON）、免费池自定义平台、兜底链顺序，保存热生效
- **安全**：默认只监听 `127.0.0.1`，密钥只存本机 `config.json`，不上传任何服务器；日志按 20MB 自动轮转，磁盘占用封顶约 40MB
- **免管理员权限**：安装、运行、卸载全程当前用户权限，不弹 UAC

## 快速开始

### 方式一：下载发行包（推荐）

1. 从 [Releases](https://github.com/aaydn/XRouter/releases) 下载最新 `XRouter-川流-vX.X.X.zip` 并解压
2. 双击「双击安装.bat」（需已安装 Python 3.9+，安装时勾选 *Add Python to PATH*）
3. 浏览器自动打开设置页，填入任一上游地址与 API Key 即可使用

安装位置 `%LOCALAPPDATA%\XRouter`，卸载双击包内「双击卸载.bat」。

### 方式二：从源码运行

```bash
pip install -r requirements.txt
python llm_router.py            # 默认 127.0.0.1:3100
python llm_router.py 0.0.0.0 3100
```

首次启动（无 `config.json`）会进入配置向导，浏览器打开 `http://127.0.0.1:3100/setup` 完成配置。

## 工作原理

```
客户端请求 POST /v1/chat/completions
        │
        ├─ 读取请求特征（是否有图片、tools、上下文长度、是否多轮）
        │
        ├─ L1 免费池（可开关）
        │    逐个平台尝试 → 首个字节超时/报错 → 顺延下一个
        │
        ├─ L2 Flash 模型（低价快速）
        │    流式回传；命中升级条件（空响应/截断/指令过深/工具签名重复/上游 400）→ 升级 L3
        │
        ├─ L3 强模型（质量优先）
        │
        └─ 全部失败 → 按「兜底链」顺序重试 → 仍失败返回 502 与上游错误信息
```

## 开发与打包

```powershell
# 打包发行版（产出 dist/XRouter-川流-v<ver>/ 与同名 zip）
powershell -File build-package.ps1

# 校验发行包（载荷 hash、材料齐全性）
powershell -File verify-package.ps1
```

## 目录结构

| 路径 | 说明 |
|---|---|
| `llm_router.py` | 全部程序（FastAPI 服务 + 内嵌页面） |
| `pack/` | 安装器头部 / 卸载程序 / 静默启动器 |
| `docs/` | 需求文档、版本记录、安全自查表、安装使用说明 |
| `assets/` | 项目 Logo |
| `build-package.ps1` | 一键打包 |
| `verify-package.ps1` | 发行包校验 |

## 文档

- [更新日志](CHANGELOG.md)
- [需求文档](docs/requirements.md)
- [安装使用说明](docs/user-guide.txt)
- [安全自查表](docs/security-checklist.md)

## 许可证

[MIT](LICENSE) © 2026 XRouter·川流 (XRouter) contributors
