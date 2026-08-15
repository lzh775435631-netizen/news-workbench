# 部署到 Render（让新闻情报工作台公网可访问）

本应用已做成「单进程 + 后台调度线程」架构，天然适配 Render 的 Web Service 模型：
- 读取 `PORT` 环境变量、绑定 `0.0.0.0`
- 启动时在后台线程拉取真实新闻，不阻塞端口就绪（Render 健康检查秒过）
- 路径全部基于 `__file__`，部署到任何目录都正常
- 通过 `Procfile` / `render.yaml` 一行 `python app.py` 启动，**不要用 gunicorn 多 worker**（多进程会各跑一个调度线程，重复抓取）

---

## 方式 A：Git 仓库 + Render 后台（推荐）

### 1. 在本机把代码推到 GitHub
```bash
cd /Users/alic1688e/WorkBuddy/工作台/news-workbench
git remote add origin https://github.com/<你的用户名>/news-workbench.git
git branch -M main
git push -u origin main
```
（仓库里已 `git init` 并提交，只需加 remote 并 push。`.gitignore` 已排除 `news.db` 与 `__pycache__`，不会把本地数据库推上去。）

### 2. Render 后台操作
1. 打开 https://dashboard.render.com → **New** → **Web Service**
2. 选择上面那个 GitHub 仓库
3. 配置：
   - **Name**: `news-workbench`
   - **Region**: Oregon（美西）
   - **Branch**: `main`
   - **Runtime**: Python 3.11（仓库里已有 `runtime.txt`）
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python app.py`
   - **Plan**: Free
   - **Health Check Path**: `/`（已默认）
4. 点 **Create Web Service**，约 1–2 分钟构建完成，得到一个 `https://news-workbench-xxxx.onrender.com` 公网地址。

### 3.（可选）配 LLM 让 AI 研究/简报更通順
在 Render 服务的 **Environment** 里加变量：
- `LLM_API_KEY` = 你的 OpenAI 兼容 key
- `LLM_BASE_URL` = 如 `https://api.openai.com/v1`
- `LLM_MODEL` = 如 `gpt-4o-mini`
不配也行，默认走启发式 AI（jieba 分词 + 报導速度 spike + 来源权重）。

---

## 方式 B：用 render.yaml（Blueprint）一键建

把仓库连到 Render 后，New → **Blueprint** → 选仓库，Render 直接按 `render.yaml` 建好服务，无需逐项填。

---

## 免费版注意事项（重要）

- **冷启动**：免费实例空闲 15 分钟后会休眠，下次访问需 30–50 秒冷启动，期间后台自动重新抓取约 1500 篇真实新闻。
- **临时磁盘**：Render 文件系统是临时的，每次重新部署会重置 `news.db`（自动重新抓取，无需手动）。所以通过界面「＋ 新增自訂來源」临时加的 RSS 不会跨部署保留——要永久加来源，请改本地 `feeds.json` 再 push。
- **采集频率**：免费实例算力有限，`FETCH_INTERVAL_MINUTES` 默认 15 分钟足够；若想更省，可改大（如 30）。

---

## 本地 / 其他平台

- 本机启动（已装受管 venv）：`PORT=8800 ./start.sh`
- 通用：`pip install -r requirements.txt && python app.py`（默认 8800 端口）
- 同样可部署到 Railway / Fly.io / 任意支持 Python 的 PaaS，启动命令都是 `python app.py`。
