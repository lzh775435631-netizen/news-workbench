#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最終版 · 新聞情報工作台 後端
動態消息(RSS/API) -> 自動擷取 -> 資料庫(SQLite) -> AI處理 -> 新聞工作台
全程使用真實公開新聞來源（Google News 主題/搜尋 RSS + 50+ 國際媒體 RSS）。
"""
import os, json, time, sqlite3, threading, re, math, html, collections, datetime, io
import concurrent.futures
from urllib.parse import urlparse, parse_qs, quote, urlencode
import feedparser, requests
import jieba
from jieba import analyse

jieba.setLogLevel(20)

BASE = os.path.dirname(os.path.abspath(__file__))
# 資料庫路徑：優先使用環境變數 DB_PATH（Render Persistent Disk 掛載時設為 /var/data/news.db）
# 未設定時回退到專案目錄下的 news.db（本地開發 / Render 免費版臨時磁碟）
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE, "news.db"))
# 自動建立資料庫所在目錄（Persistent Disk 掛載後即可直接寫入）
os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
DB = DB_PATH
FEEDS = os.path.join(BASE, "feeds.json")

# ---------------------------------------------------------------------------
# 資料庫抽象層：SQLite / PostgreSQL 雙模式
#   - 本地 / 未設定 DATABASE_URL → SQLite（現有行為完全不變）
#   - 設定 DATABASE_URL（生產 PostgreSQL）→ 自動切換，業務 API 零改動
# 透過 SmartCursor 在遊標層透明轉換 SQLite 風格 SQL，避免改寫任何業務查詢。
# ---------------------------------------------------------------------------
USE_PG = bool(os.environ.get("DATABASE_URL"))

# 統一綱要模板：自增主鍵用 {IDPK} 佔位，依後端替換為
#   SQLite  -> INTEGER PRIMARY KEY AUTOINCREMENT
#   PostgreSQL -> SERIAL PRIMARY KEY
# 其餘欄位 / 約束 / 索引兩端完全一致，確保 18 張表結構相容。
_DDL_TPL = """
CREATE TABLE IF NOT EXISTS news (
  id {IDPK},
  title TEXT, norm_title TEXT UNIQUE, summary TEXT, content TEXT,
  link TEXT, source TEXT, category TEXT, lang TEXT,
  published INTEGER, published_iso TEXT, fetched INTEGER,
  keywords TEXT, hotness INTEGER DEFAULT 0, importance TEXT DEFAULT '低',
  is_breaking INTEGER DEFAULT 0, cluster_id INTEGER DEFAULT 0,
  read INTEGER DEFAULT 0, starred INTEGER DEFAULT 0, later INTEGER DEFAULT 0,
  notes TEXT DEFAULT '', tags TEXT DEFAULT '[]', source_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_cat ON news(category);
CREATE INDEX IF NOT EXISTS idx_pub ON news(published);
CREATE INDEX IF NOT EXISTS idx_clu ON news(cluster_id);
CREATE INDEX IF NOT EXISTS idx_src ON news(source);
CREATE TABLE IF NOT EXISTS clusters (
  cluster_id INTEGER PRIMARY KEY, repr TEXT, category TEXT, size INTEGER,
  sources TEXT, hotness INTEGER, is_breaking INTEGER,
  created INTEGER, updated INTEGER, keywords TEXT
);
CREATE TABLE IF NOT EXISTS searches (id {IDPK}, q TEXT, ts INTEGER);
CREATE TABLE IF NOT EXISTS researches (
  id {IDPK},
  news_id INTEGER, cluster_id INTEGER, title TEXT,
  core_question TEXT, phenomenon TEXT, my_view TEXT,
  status TEXT DEFAULT '待研究', writing_score INTEGER DEFAULT 0,
  event TEXT, why_matters TEXT,
  controversies TEXT, counterintuitive TEXT, extension_questions TEXT,
  created_at TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS arguments (
  id {IDPK},
  research_id INTEGER, content TEXT, explanation TEXT,
  strength TEXT DEFAULT '中', credibility TEXT DEFAULT '中',
  my_response TEXT, counter_view TEXT,
  created_at TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS evidence (
  id {IDPK},
  argument_id INTEGER, type TEXT, title TEXT, content TEXT,
  source TEXT, source_url TEXT, verified INTEGER DEFAULT 0,
  created_at TEXT
);
CREATE TABLE IF NOT EXISTS topics (
  id {IDPK},
  research_id INTEGER, title TEXT, core_question TEXT,
  initial_view TEXT, status TEXT DEFAULT '待研究', score INTEGER DEFAULT 0,
  created_at TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS research_news (
  research_id INTEGER, news_id INTEGER,
  PRIMARY KEY(research_id, news_id)
);
CREATE TABLE IF NOT EXISTS topic_arguments (
  topic_id INTEGER, argument_id INTEGER,
  PRIMARY KEY(topic_id, argument_id)
);
CREATE TABLE IF NOT EXISTS entities (
  id {IDPK},
  type TEXT, name TEXT, description TEXT DEFAULT '',
  created_at TEXT, updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_uniq ON entities(type, name);
CREATE TABLE IF NOT EXISTS news_entities (
  news_id INTEGER, entity_id INTEGER,
  PRIMARY KEY(news_id, entity_id)
);
CREATE TABLE IF NOT EXISTS research_entities (
  research_id INTEGER, entity_id INTEGER,
  PRIMARY KEY(research_id, entity_id)
);
CREATE TABLE IF NOT EXISTS facts (
  id {IDPK},
  research_id INTEGER, content TEXT, source TEXT, source_url TEXT,
  first_seen TEXT, confirm_count INTEGER DEFAULT 1, status TEXT DEFAULT 'single',
  created_at TEXT
);
CREATE TABLE IF NOT EXISTS conflicts (
  id {IDPK},
  research_id INTEGER, type TEXT, claim_a TEXT, source_a TEXT,
  claim_b TEXT, source_b TEXT, created_at TEXT
);
CREATE TABLE IF NOT EXISTS research_updates (
  id {IDPK},
  research_id INTEGER, summary TEXT, new_facts TEXT, new_conflicts TEXT,
  created_at TEXT
);
CREATE TABLE IF NOT EXISTS research_links (
  research_id INTEGER, related_id INTEGER,
  PRIMARY KEY(research_id, related_id)
);
CREATE TABLE IF NOT EXISTS collector_logs (
  id {IDPK},
  run_at TEXT, source TEXT, status TEXT,
  http_status INTEGER, resp_len INTEGER DEFAULT 0,
  entries INTEGER DEFAULT 0, inserted INTEGER DEFAULT 0, error TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_clog_src ON collector_logs(source);
"""

SQLITE_DDL = _DDL_TPL.format(IDPK="INTEGER PRIMARY KEY AUTOINCREMENT")
PG_DDL = _DDL_TPL.format(IDPK="SERIAL PRIMARY KEY")

if USE_PG:
    import psycopg2
    from psycopg2.extras import DictCursor

    class SmartCursor(DictCursor):
        """PostgreSQL 相容層（繼承 DictCursor，保持 row[0]/row['col']/dict(row) 三種存取）。
        execute 時透明轉換：
          - ? 占位符 -> %s
          - INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
          - 自增表 INSERT -> 補 RETURNING id 並回填 lastrowid
        """
        SERIAL_TABLES = {
            "news", "searches", "researches", "arguments", "evidence",
            "topics", "entities", "facts", "conflicts",
            "research_updates", "collector_logs",
        }

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._lastrowid = None

        @property
        def lastrowid(self):
            return self._lastrowid

        def execute(self, sql, args=None):
            s = sql
            is_ignore = False
            if re.search(r"\bINSERT\s+OR\s+IGNORE\b", s, re.I):
                s = re.sub(r"\bINSERT\s+OR\s+IGNORE\b", "INSERT", s, flags=re.I)
                is_ignore = True
            tm = re.search(r"\bINSERT\s+(?:INTO\s+)?(\w+)", s, re.I)
            tbl = tm.group(1).lower() if tm else None
            add_returning = bool(tbl) and tbl in self.SERIAL_TABLES and "RETURNING" not in s.upper()
            s = s.replace("?", "%s")
            if is_ignore and "ON CONFLICT" not in s.upper():
                s += " ON CONFLICT DO NOTHING"
            if add_returning:
                s += " RETURNING id"
            super().execute(s, args)
            if add_returning:
                try:
                    row = self.fetchone()
                    self._lastrowid = row[0] if row is not None else None
                except Exception:
                    self._lastrowid = None
            else:
                self._lastrowid = None
    _PG_CURSOR_FACTORY = SmartCursor
else:
    _PG_CURSOR_FACTORY = None
PORT = int(os.environ.get("PORT", "8800"))

# 可選 LLM（OpenAI 相容）— 設定環境變數即啟用，未設定則使用啟發式 AI
# 統一支援 LLM_API_KEY / LLM_BASE_URL / LLM_MODEL；模型不寫死，可用環境變數切換
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1").rstrip("/")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")
# API 模式：chat = /chat/completions（預設，相容大多數 OpenAI-compatible 端點）
#          responses = OpenAI Responses API（/responses）
LLM_API_MODE = os.environ.get("LLM_API_MODE", "chat").lower()

WRITE_LOCK = threading.Lock()
# 採集請求統一帶瀏覽器 UA，避免被部分新聞網站以 403/空響應拒絕（Render 資料中心 IP 尤為常見）
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
last_sync = 0
last_cycle_new = 0
last_cycle_breaking = []
prev_cluster_hot = {}
prev_avg_hot = 0

# 採集器運行狀態（供 /api/collector/status 使用，絕不包含任何 API Key）
collector_state = {
    "running": True,
    "last_run": 0,
    "last_success": 0,
    "last_inserted": 0,
    "last_failed": 0,
    "total_news": 0,
    "next_run": 0,
    "sources_total": 0,
    "sources_success": 0,
    "sources_failed": 0,
    "last_error": "",
}

STOP = set("的 了 在 是 和 與 及 也 都 就 而 或 一個 我們 你們 他們 這個 那個 已經 可能 可以 如何 為何 為什麼 什麼 哪些 表示 指出 稱 說 將 對 與 等 中 年 月 日 時 分 個 項 起 後 前 上 下 內 外 大 小 the a an and or of to in for on at by with from as is are be was were this that these those it its he she they we you i not no yes if then than so but".split())
NOISE = set("nbsp com www http https news google sina net org cn sg gov co uk html url comments reuters ap afp 图片 视频 直播 专题 中国 美国 日本 俄羅斯 俄 乌克兰 烏克蘭 全球 世界 国际 国内 国家 政府 公司 市场 年 月 日 今天 本周 表示 称 报道 以及 和 与 对 在 将 等 一项 一名 中新网 新浪 新浪网 网易 腾讯 搜狐 央视 新华网 人民网 联合 消息 最新 突发 快讯 使用 数据 显示 chinanews sohu sina view views photo photos after before from with that this what when where why who how new news said say has have will amid into over more top best its his her their our your are was were been being not but they them then than out up down off about just like only also can may might must should would get got see show shows via per vs year years day days week time first last as at by of on to us we you he she it every exterior interior read reading watch watching here there which while where about above below between during against both few each other another".split())
# 聚類時排除的「泛主題/品牌/類別」詞（仍保留作顯示用關鍵字）
GENTOPIC = set("ai google 人工智能 新能源 汽车 科技 财经 体育 健康 政治 国际 国内 经济 文化 军事 社会 娱乐 互联网 公司 商业 房产 網際網路 科学 新闻 数据 显示 使用 最新 突发 快讯 年 月 日 2026 2025 2024 news sport sports tech business health politics world china reuters ap afp bbc cnn bloomberg 新浪 网易 腾讯 搜狐 央视 新华网 人民网 联合 消息 专题 图片 视频 直播 chinanews sohu sina 中国 美国 日本 全球 世界 市场 政府 公司 经济 社会 国际 国内".split())
NOISE |= set("says article review now people court one 中华网 头条新闻 实践 暑期 头条 报道 says said news new".split())

BREAKING_KW = ["突發","快訊","緊急","急報","突發快訊","breaking","urgent","alert","just in","breaking news","突发","breakingnews"]
CONTROVERSY_KW = ["爭議","抗議","批評","衝突","危機","醜聞","訴訟","制裁","譴責","分裂","爭論","controversy","protest","criticism","scandal","lawsuit","sanction","clash","dispute"]
IMPACT_KW = ["影響","衝擊","風險","危機","打擊","提振","推升","下跌","上漲","效應","後果","impact","risk","boost","surge","plunge","effect","threat"]

# ----------------------------------------------------------------------------
# 工具函式
# ----------------------------------------------------------------------------
def now_unix():
    return int(time.time())

def iso(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def clean_html(t):
    t = (t or "").replace("\u00a0", " ").replace("&nbsp;", " ").replace("&#160;", " ")
    t = html.unescape(t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def clean_title(title, source):
    t = (title or "").strip()
    # Google News 形如 "標題 - 來源"
    if source and t.endswith(source):
        t = t[: -len(source)].strip()
    if " - " in t:
        parts = t.rsplit(" - ", 1)
        if len(parts[1]) < 30:
            t = parts[0].strip()
    return t

def norm_title(t):
    t = (t or "").lower()
    t = re.sub(r"[^一-鿿a-z0-9]", "", t)
    return t

def bigrams(s):
    s = re.sub(r"[^一-鿿a-z0-9]", "", (s or "").lower())
    return set(s[i:i+2] for i in range(len(s)-1))

def extract_keywords(title, summary=""):
    text = clean_html(title) + " " + clean_html(summary)[:160]
    kws = []
    try:
        kws = analyse.extract_tags(text, topK=8, withWeight=False)
    except Exception:
        pass
    eng = re.findall(r"[A-Za-z][A-Za-z0-9]{2,}", text)
    seen, out = set(), []
    for w in list(kws) + [w.lower() for w in eng]:
        w = w.strip().lower()
        if not w or w in STOP or w in NOISE or w in GENTOPIC or len(w) < 2:
            continue
        if any(ch in w for ch in "./@"):
            continue
        if any(ch.isdigit() for ch in w):
            continue
        if w not in seen:
            seen.add(w)
            out.append(w)
        if len(out) >= 8:
            break
    return out

def summarize(text, kw, maxlen=170):
    text = clean_html(text)
    if not text:
        return ""
    parts = [p.strip() for p in re.split(r"(?<=[。！？!?\.])", text) if p.strip()]
    if len(parts) <= 2:
        return text[:maxlen]
    score = {}
    for i, p in enumerate(parts):
        s = sum(1 for k in kw if k in p)
        s += 1.0 / (i + 1.5)
        score[i] = s
    top = sorted(score, key=lambda x: -score[x])[:3]
    top.sort()
    return "".join(parts[i] for i in top)[:maxlen]

def recency_factor(ts):
    age = max(0, now_unix() - (ts or now_unix()))
    hours = age / 3600.0
    return max(0.0, 1.0 - hours / 48.0)

def compute_hotness(cluster_size, weight, ts, breaking):
    rec = recency_factor(ts)
    cboost = min(2.0, 1 + 0.22 * math.log10(1 + cluster_size))
    base = 0.5 * rec + 0.22 + 0.28 * min(1.0, (cboost - 1) / 1.0)
    hot = int(base * 82 * weight * (1 + 0.12 * max(0, cluster_size - 1)))
    if breaking:
        hot = min(100, hot + 18)
    return max(1, min(100, hot))

def importance_of(hot, breaking):
    if breaking or hot >= 68:
        return "高"
    if hot >= 42:
        return "中"
    return "低"

def breaking_of(title, summary, pub):
    # 僅以標題判斷（Google News 摘要為多來源聚合，關鍵字噪音大）
    t = (title or "").lower()
    if not any(k in t for k in BREAKING_KW):
        return 0
    return 1 if recency_factor(pub) > 0.5 else 0

# ----------------------------------------------------------------------------
# 資料庫
# ----------------------------------------------------------------------------
def db():
    if USE_PG:
        return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=_PG_CURSOR_FACTORY)
    c = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    c = db()
    if USE_PG:
        # PostgreSQL 不支援 executescript 多語句，逐條執行等價 DDL
        for stmt in PG_DDL.split(";"):
            stmt = stmt.strip()
            if stmt:
                c.execute(stmt)
    else:
        c.executescript(SQLITE_DDL)
    c.execute("CREATE INDEX IF NOT EXISTS idx_facts_rid ON facts(research_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_conflicts_rid ON conflicts(research_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ru_rid ON research_updates(research_id)")
    _migrate_columns(c)
    c.commit(); c.close()

def _column_exists(c, table, col):
    if USE_PG:
        try:
            rows = c.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
                (table, col),
            ).fetchall()
            return len(rows) > 0
        except Exception:
            return False
    try:
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
        return col in cols
    except Exception:
        return False

def _migrate_columns(c):
    """向後相容遷移：為已存在表格補充新欄位（研究緩存 / 論點依據類型）。"""
    for table, col, ddl in (
        ("researches", "ai_generated", "INTEGER DEFAULT 0"),
        ("researches", "ai_model", "TEXT DEFAULT ''"),
        ("researches", "ai_generated_at", "TEXT DEFAULT ''"),
        ("researches", "tracking", "INTEGER DEFAULT 0"),
        ("researches", "last_checked_at", "TEXT DEFAULT ''"),
        ("researches", "last_news_count", "INTEGER DEFAULT 0"),
        ("arguments", "basis", "TEXT DEFAULT '推论'"),
    ):
        if not _column_exists(c, table, col):
            try:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
            except Exception as e:
                print(f"[migrate skip {table}.{col}] {e}")

# ----------------------------------------------------------------------------
# 擷取
# ----------------------------------------------------------------------------
def source_url(src):
    if src["type"] == "gnews_topic":
        return f"https://news.google.com/rss/headlines/section/topic/{src['topic']}?hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    if src["type"] == "gnews_search":
        return f"https://news.google.com/rss/search?q={quote(src['q'])}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    return src["url"]

def fetch_source(src):
    """單一來源採集：使用 requests 顯式控制 timeout / UA / 重定向 / 編碼，
    並以 try/except 隔離——任何單一來源失敗都不會影響其他來源或整個週期。"""
    meta = {"source": src.get("id", "?"), "name": src.get("name", "?"),
            "url": source_url(src), "http_status": None, "resp_len": 0,
            "entries": 0, "inserted": 0, "error": None, "sec": 0.0}
    t = time.time()
    try:
        resp = requests.get(
            meta["url"], timeout=15,
            headers={
                "User-Agent": UA,
                "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            },
            allow_redirects=True)
        meta["http_status"] = resp.status_code
        meta["resp_len"] = len(resp.content)
        meta["sec"] = round(time.time() - t, 1)
        if resp.status_code != 200:
            meta["error"] = f"HTTP {resp.status_code}"
            return [], meta
        # 編碼：feedparser 直接解析位元組流，自動處理 charset
        d = feedparser.parse(io.BytesIO(resp.content))
        out = []
        for e in d.entries[:45]:
            title = clean_title(getattr(e, "title", ""), getattr(e, "source", None) and getattr(e.source, "title", "") or "")
            if not title:
                continue
            summary = clean_html(getattr(e, "summary", "") or getattr(e, "description", ""))
            link = getattr(e, "link", "")
            src_name = (getattr(e, "source", None) and getattr(e.source, "title", "")) or src["name"]
            pub = None
            if getattr(e, "published_parsed", None):
                pub = int(time.mktime(e.published_parsed))
            elif getattr(e, "updated_parsed", None):
                pub = int(time.mktime(e.updated_parsed))
            if not pub:
                pub = now_unix()  # 時間解析失敗時回退為採集時間，不丟棄整條新聞
            out.append({
                "title": title, "summary": summary, "link": link,
                "source": src_name, "source_id": src["id"], "category": src["category"],
                "lang": src.get("lang", "en"), "published": pub,
                "weight": float(src.get("weight", 1.0)),
            })
        meta["entries"] = len(out)
        if not out and getattr(d, "bozo", 0):
            meta["error"] = "parse: " + str(getattr(d, "bozo_exception", "unknown"))[:140]
        return out, meta
    except Exception as ex:
        meta["sec"] = round(time.time() - t, 1)
        meta["error"] = f"{type(ex).__name__}: {str(ex)[:160]}"
        return [], meta

def fetch_all(sources):
    out = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for src, res in zip(sources, ex.map(fetch_source, sources)):
            items, meta = res
            out.append((src, items, meta))
    return out

def _log_collector(meta, conn):
    try:
        conn.execute(
            "INSERT INTO collector_logs(run_at,source,status,http_status,resp_len,entries,inserted,error) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (iso(now_unix()), meta.get("source"),
             "ok" if meta.get("error") is None else "fail",
             meta.get("http_status"), meta.get("resp_len", 0), meta.get("entries", 0),
             meta.get("inserted", 0), (meta.get("error") or "")[:300]))
    except Exception:
        pass

def prune_collector_logs(conn):
    """只保留最近 2000 條採集日誌，避免無限增長。"""
    try:
        conn.execute("DELETE FROM collector_logs WHERE id <= (SELECT MAX(id) - 2000 FROM collector_logs)")
    except Exception:
        pass

# ----------------------------------------------------------------------------
# 處理循環
# ----------------------------------------------------------------------------
def cluster(articles):
    n = len(articles)
    parent = list(range(n))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    df = {}
    ks = []
    for a in articles:
        fk = set(k for k in a["kws"] if k not in GENTOPIC)
        ks.append(fk)
        for k in fk:
            df[k] = df.get(k, 0) + 1
    total = max(1, n)
    sig_kw = set(k for k, v in df.items() if v <= max(2, int(total * 0.10)))
    post = collections.defaultdict(list)
    for i, fk in enumerate(ks):
        for k in fk:
            if k in sig_kw:
                post[k].append(i)
    for k, lst in post.items():
        for i in range(len(lst)):
            for j in range(i + 1, len(lst)):
                a, b = lst[i], lst[j]
                sa, sb = ks[a], ks[b]
                if not sa or not sb:
                    continue
                inter = len(sa & sb)
                uni = len(sa | sb)
                if (uni > 0 and inter / uni >= 0.5) or inter >= 4:
                    union(a, b)
    groups = collections.defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)
    return groups

def process_cycle():
    global last_sync, last_cycle_new, last_cycle_breaking, prev_cluster_hot, prev_avg_hot
    global collector_state
    t0 = time.time()
    try:
        with open(FEEDS, encoding="utf-8") as f:
            cfg = json.load(f)
        sources = cfg["sources"]
    except Exception as e:
        print(f"[cycle] feeds read error: {e}")
        collector_state["last_error"] = f"feeds: {e}"
        return
    collected = fetch_all(sources)
    conn = db()
    metas = []
    for src, items, meta in collected:
        metas.append(meta)
        ins = 0
        for it in items:
            norm = norm_title(it["title"])
            if not norm:
                continue
            kw = extract_keywords(it["title"], it["summary"])
            summ = summarize(it["summary"] or it["title"], kw)
            is_brk = breaking_of(it["title"], it["summary"], it["published"])
            try:
                cur = conn.execute(
                    """INSERT OR IGNORE INTO news
                       (title,norm_title,summary,content,link,source,source_id,category,lang,
                        published,published_iso,fetched,keywords,hotness,importance,is_breaking,cluster_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (it["title"], norm, summ, it["summary"], it["link"], it["source"], it["source_id"],
                     it["category"], it["lang"], it["published"], iso(it["published"]), now_unix(),
                     json.dumps(kw, ensure_ascii=False), 1, "低", is_brk, 0))
                if cur.rowcount == 1:
                    ins += 1
            except Exception as ex:
                meta["error"] = f"insert: {ex}"
        meta["inserted"] = ins
        conn.commit()  # 每個來源提交一次，確保已採集資料即時落庫
    # 記錄每來源採集日誌（失敗來源不影響整體）
    for meta in metas:
        _log_collector(meta, conn)
    prune_collector_logs(conn)
    # 以資料庫為準精確計算本輪新增（避免跨來源重複計數偏差）
    last_cycle_new = conn.execute("SELECT COUNT(*) FROM news WHERE fetched > ?", (int(last_sync),)).fetchone()[0]
    # 更新採集器運行狀態（供 /api/collector/status 使用）
    succ = sum(1 for m in metas if m["error"] is None and m["entries"] > 0)
    fail = sum(1 for m in metas if m["error"] is not None or m["entries"] == 0)
    errs = [f"{m['source']}: {m['error']}" for m in metas if m["error"]]
    collector_state["last_run"] = int(t0)
    collector_state["last_inserted"] = last_cycle_new
    collector_state["last_failed"] = fail
    collector_state["sources_total"] = len(metas)
    collector_state["sources_success"] = succ
    collector_state["sources_failed"] = fail
    collector_state["next_run"] = now_unix() + get_fetch_interval() * 60
    if last_cycle_new > 0 or succ > 0:
        collector_state["last_success"] = now_unix()
    collector_state["total_news"] = conn.execute("SELECT COUNT(*) FROM news").fetchone()[0]
    collector_state["running"] = True
    collector_state["last_error"] = "; ".join(errs[:5])

    # 載入近 10 天文章做聚類
    rows = conn.execute(
        "SELECT id,keywords,category,published,source,title,summary,hotness,is_breaking FROM news WHERE published > ? ORDER BY published DESC LIMIT 5000",
        (now_unix() - 10 * 86400,)).fetchall()
    arts = []
    for r in rows:
        try:
            kws = json.loads(r["keywords"]) if r["keywords"] else []
        except Exception:
            kws = []
        arts.append({"id": r["id"], "kws": kws, "cat": r["category"], "pub": r["published"],
                     "src": r["source"], "title": r["title"], "summary": r["summary"],
                     "hot": r["hotness"], "brk": r["is_breaking"]})
    groups = cluster(arts) if arts else {}
    new_prev = {}
    for root, idxs in groups.items():
        grp = [arts[i] for i in idxs]
        size = len(grp)
        sources_set = sorted(set(a["src"] for a in grp))
        kwcount = collections.Counter()
        for a in grp:
            kwcount.update(a["kws"])
        topkw = [k for k, _ in kwcount.most_common(8)]
        sig = "|".join(topkw[:3])
        cid = (abs(hash(sig)) % 999983) + 1
        max_pub = max(a["pub"] for a in grp)
        now = now_unix()
        recent = [a for a in grp if now - a["pub"] < 3 * 3600]
        distinct = len(sources_set)
        title_brk = any(k in (a["title"] or "").lower() for a in grp for k in BREAKING_KW)
        velocity = (len(recent) >= 3) or (len(recent) >= 2 and distinct >= 2) or (size >= 4 and distinct >= 3)
        brk = int(title_brk or velocity)
        # 以最高熱度文章為代表
        best = max(grp, key=lambda a: a["hot"])
        chot = max(compute_hotness(size, 1.0, a["pub"], False) for a in grp)
        rep_title = best["title"]
        new_prev[cid] = chot
        # 更新該群組每篇文章熱度
        for a in grp:
            a_brk = brk
            hot = compute_hotness(size, 1.0, a["pub"], bool(a_brk))
            imp = importance_of(hot, bool(a_brk))
            conn.execute("UPDATE news SET cluster_id=?, hotness=?, importance=?, is_breaking=? WHERE id=?",
                         (cid, hot, imp, int(bool(a_brk)), a["id"]))
        # upsert clusters
        exist = conn.execute("SELECT cluster_id FROM clusters WHERE cluster_id=?", (cid,)).fetchone()
        if exist:
            conn.execute("UPDATE clusters SET repr=?,category=?,size=?,sources=?,hotness=?,is_breaking=?,updated=? WHERE cluster_id=?",
                         (rep_title, grp[0]["cat"], size, json.dumps(sources_set, ensure_ascii=False), chot, brk, now_unix(), cid))
        else:
            conn.execute("INSERT INTO clusters (cluster_id,repr,category,size,sources,hotness,is_breaking,created,updated,keywords) VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (cid, rep_title, grp[0]["cat"], size, json.dumps(sources_set, ensure_ascii=False), chot, brk, now_unix(), now_unix(), json.dumps(topkw, ensure_ascii=False)))
    # 未歸類文章給予唯一 cluster_id，避免全部落入預設的 0 形成假巨簇
    conn.execute("UPDATE news SET cluster_id = id WHERE cluster_id = 0")
    conn.commit()
    # 平均熱度
    avg = conn.execute("SELECT AVG(hotness) FROM news WHERE published > ?", (now_unix() - 3 * 86400,)).fetchone()[0] or 0
    prev_avg_hot = avg
    # 突發清單
    brk_rows = conn.execute(
        "SELECT title,source,category,link FROM news WHERE is_breaking=1 AND published > ? ORDER BY published DESC LIMIT 12",
        (now_unix() - 12 * 3600,)).fetchall()
    last_cycle_breaking = [dict(r) for r in brk_rows]
    prev_cluster_hot = new_prev
    last_sync = now_unix()
    # 持續追蹤：自動關聯 tracking=1 研究的相關新新聞（失敗不影響採集）
    try:
        update_tracked_researches()
    except Exception as e:
        print(f"[cycle tracked error] {e}")
    conn.close()
    print(f"[cycle] +{last_cycle_new} new, clusters={len(groups)}, breaking={len(last_cycle_breaking)}, {time.time()-t0:.1f}s")

# ----------------------------------------------------------------------------
# 可選 LLM（OpenAI Chat Completions / Responses API 雙模式）
# ----------------------------------------------------------------------------
def _extract_responses_text(data):
    """從 OpenAI Responses API 回傳結構中提取文字內容。"""
    try:
        out = data.get("output") or []
        texts = []
        for item in out:
            if item.get("type") == "message" and "content" in item:
                for c in item["content"]:
                    if c.get("type") == "output_text":
                        texts.append(c.get("text", ""))
            # 部分實作把文字放在 output_text 直接欄位
            if "text" in item and isinstance(item["text"], str):
                texts.append(item["text"])
        # 相容 output[*].content 為純字串陣列
        if not texts and "output" in data:
            for item in data["output"]:
                if isinstance(item, dict) and "content" in item and isinstance(item["content"], list):
                    for c in item["content"]:
                        if isinstance(c, dict) and c.get("type") == "output_text":
                            texts.append(c.get("text", ""))
        return "\n".join(t for t in texts if t).strip()
    except Exception:
        return ""

def call_llm(system, prompt, max_tokens=700, temperature=0.4, timeout=45, json_mode=False):
    """呼叫可選 LLM。未設定 API Key 時回傳 None（呼叫方應使用啟發式兜底）。
    支援兩種模式：
      - chat:      POST {LLM_BASE}/chat/completions
      - responses: POST {LLM_BASE}/responses (OpenAI Responses API)
    任何錯誤（key 錯誤 / 網路失敗 / 模型不存在 / 逾時 / 限流）皆回傳 None，
    由呼叫方統一走 fallback，絕不讓頁面報錯。
    """
    if not LLM_API_KEY:
        return None
    headers = {"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"}
    try:
        if LLM_API_MODE == "responses":
            payload = {
                "model": LLM_MODEL,
                "input": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            }
            if json_mode:
                payload["text"] = {"format": {"type": "json_object"}}
            r = requests.post(f"{LLM_BASE}/responses", headers=headers, json=payload, timeout=timeout)
            r.raise_for_status()
            text = _extract_responses_text(r.json())
            return text or None
        else:
            payload = {
                "model": LLM_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
            if json_mode:
                payload["response_format"] = {"type": "json_object"}
            r = requests.post(f"{LLM_BASE}/chat/completions", headers=headers, json=payload, timeout=timeout)
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"].strip() or None
    except Exception as e:
        print(f"[llm error] {e}")
        return None

def llm_status_info():
    """回傳 LLM 啟用狀態（絕不回傳真實 API Key）。"""
    return {
        "enabled": bool(LLM_API_KEY),
        "model": LLM_MODEL,
        "base_url": LLM_BASE,
        "mode": LLM_API_MODE,
        "key_configured": bool(LLM_API_KEY),
    }

# ----------------------------------------------------------------------------
# API 處理
# ----------------------------------------------------------------------------
def query_news(params):
    """返回經過篩選的 news 行（dict 列表，不含 LIMIT/OFFSET）。供 /api/news 與 /api/events 共用，避免重複篩選邏輯。"""
    cat = params.get("category", ["全部"])[0]
    q = params.get("q", [""])[0].strip()
    source = params.get("source", [""])[0].strip()
    frm = params.get("from", [""])[0].strip()
    to = params.get("to", [""])[0].strip()
    minhot = params.get("minHot", ["0"])[0]
    starred = params.get("starred", ["0"])[0]
    read = params.get("read", [""])[0]
    later = params.get("later", ["0"])[0]
    sort = params.get("sort", ["hot"])[0]
    sql = "SELECT * FROM news WHERE 1=1"
    args = []
    if cat not in ("全部", ""):
        if cat == "收藏":
            sql += " AND starred=1"
        elif cat == "稍后看":
            sql += " AND later=1"
        elif cat == "已读":
            sql += " AND read=1"
        elif cat == "未读":
            sql += " AND read=0"
        else:
            sql += " AND category=?"
            args.append(cat)
    if q:
        sql += " AND (title LIKE ? OR summary LIKE ? OR keywords LIKE ?)"
        args += [f"%{q}%", f"%{q}%", f"%{q}%"]
        # 搜尋紀錄
        try:
            c = db(); c.execute("INSERT INTO searches(q,ts) VALUES(?,?)", (q, now_unix())); c.commit(); c.close()
        except Exception:
            pass
    if source:
        sql += " AND source=?"; args.append(source)
    if frm:
        sql += " AND published >= ?"; args.append(int(datetime.datetime.strptime(frm, "%Y-%m-%d").timestamp()))
    if to:
        sql += " AND published <= ?"; args.append(int(datetime.datetime.strptime(to, "%Y-%m-%d").timestamp()) + 86400)
    try:
        mh = int(minhot); sql += " AND hotness >= ?"; args.append(mh)
    except Exception:
        pass
    if starred == "1":
        sql += " AND starred=1"
    if later == "1":
        sql += " AND later=1"
    if read == "1":
        sql += " AND read=1"
    elif read == "0":
        sql += " AND read=0"
    sql += " ORDER BY " + ("hotness DESC" if sort == "hot" else "published DESC")
    c = db(); rows = c.execute(sql, args).fetchall(); c.close()
    return [dict(r) for r in rows]

def api_news(params):
    limit = int(params.get("limit", ["40"])[0])
    offset = int(params.get("offset", ["0"])[0])
    rows = query_news(params)
    page = rows[offset:offset+limit]
    return {"items": page, "count": len(page)}

def _clu_map():
    """clusters 表快取：cluster_id -> 行，供事件聚合複用 repr/hotness/category。"""
    c = db()
    rs = c.execute("SELECT cluster_id,repr,category,size,sources,hotness,is_breaking,keywords FROM clusters").fetchall()
    c.close()
    return {r["cluster_id"]: dict(r) for r in rs}

def api_events(params):
    """首頁事件流：按 cluster_id 聚合，一個 cluster = 一張事件卡。
    原始 news 完整保留（不刪除、不修改內容），僅在展示層聚合。"""
    sort = params.get("sort", ["hot"])[0]
    limit = int(params.get("limit", ["40"])[0])
    offset = int(params.get("offset", ["0"])[0])
    rows = query_news(params)
    # 按 cluster_id 分組（單例 cluster 的 cluster_id==自身 id，自然成獨立事件）
    groups = {}
    for r in rows:
        groups.setdefault(r["cluster_id"], []).append(r)
    clu = _clu_map()
    iorder = {"高": 3, "中": 2, "低": 1, "": 0}
    events = []
    for cid, members in groups.items():
        # 規則 9：同 URL 不重複（展示層去重，不動庫）
        seen, dm = set(), []
        for m in members:
            key = (m.get("link") or "") or ("#" + str(m["id"]))
            if key in seen:
                continue
            seen.add(key); dm.append(m)
        c = clu.get(cid)
        if c and c.get("repr"):
            title = c["repr"]
        else:
            title = max(dm, key=lambda m: m["hotness"])["title"]
        hot = (c.get("hotness") or 0) if (c and c.get("hotness")) else max(m["hotness"] for m in dm)
        cat = (c.get("category") or "") if (c and c.get("category")) else (dm[0].get("category") or "")
        imp = max(dm, key=lambda m: iorder.get(m.get("importance") or "低", 1))["importance"]
        brk = 1 if any(m.get("is_breaking") for m in dm) else 0
        contro = 1 if any(any(kw in (m.get("title") or "") for kw in CONTROVERSY_KW) for m in dm) else 0
        sources = sorted({m.get("source") for m in dm if m.get("source")})
        latest_iso = max((m.get("published_iso") or "") for m in dm)
        latest_pub = max((m.get("published") or 0) for m in dm)
        members_out = [{"id": m["id"], "source": m.get("source"), "title": m.get("title"),
                        "link": m.get("link"), "published_iso": m.get("published_iso"),
                        "published": m.get("published"), "hotness": m.get("hotness"),
                        "summary": m.get("summary")} for m in dm]
        events.append({"cluster_id": cid, "title": title, "hotness": hot, "category": cat,
                       "importance": imp, "is_breaking": brk, "controversial": contro,
                       "source_count": len(sources), "sources": sources,
                       "latest_update": latest_iso, "latest_pub": latest_pub,
                       "size": len(dm), "members": members_out})
    if sort == "hot":
        events.sort(key=lambda e: -e["hotness"])
    else:
        events.sort(key=lambda e: -e["latest_pub"])
    page = events[offset:offset+limit]
    return {"events": page, "count": len(events)}

def api_categories():
    c = db()
    cats = [r[0] for r in c.execute("SELECT DISTINCT category FROM news ORDER BY category").fetchall()]
    c.close()
    counts = {}
    c = db()
    for cat, cnt in c.execute("SELECT category, COUNT(*) FROM news GROUP BY category").fetchall():
        counts[cat] = cnt
    c.close()
    return {"categories": cats, "counts": counts}

def api_sources():
    c = db()
    rows = c.execute("SELECT source, COUNT(*) c FROM news GROUP BY source ORDER BY c DESC").fetchall()
    c.close()
    return {"sources": [{"name": r[0], "count": r[1]} for r in rows]}

def api_hotspots():
    cutoff = now_unix() - 24 * 3600
    c = db()
    today_hot = [dict(r) for r in c.execute(
        "SELECT * FROM news WHERE published > ? ORDER BY hotness DESC LIMIT 15", (cutoff,)).fetchall()]
    breaking = [dict(r) for r in c.execute(
        "SELECT * FROM news WHERE is_breaking=1 AND published > ? ORDER BY published DESC LIMIT 12", (now_unix() - 12 * 3600,)).fetchall()]
    controversial = [dict(r) for r in c.execute(
        "SELECT * FROM news WHERE published > ? AND (" + " OR ".join(["title LIKE ?"] * len(CONTROVERSY_KW)) + ") ORDER BY hotness DESC LIMIT 12",
        (cutoff,) + tuple(f"%{k}%" for k in CONTROVERSY_KW)).fetchall()]
    ranking = [dict(r) for r in c.execute(
        "SELECT * FROM news WHERE published > ? ORDER BY hotness DESC LIMIT 18", (now_unix() - 3 * 86400,)).fetchall()]
    # 升溫/降溫
    rising, falling = [], []
    cl = c.execute("SELECT cluster_id,repr,category,size,sources,hotness,is_breaking,keywords FROM clusters ORDER BY hotness DESC LIMIT 60").fetchall()
    cl = [dict(r) for r in cl]
    for r in cl:
        cid = r["cluster_id"]; hot = r["hotness"]
        prev = prev_cluster_hot.get(cid)
        if prev is None:
            r["trend"] = "new"
            rising.append(r)
        elif hot - prev >= 8:
            r["trend"] = "rising"; rising.append(r)
        elif prev - hot >= 8:
            r["trend"] = "falling"; falling.append(r)
        else:
            r["trend"] = "stable"
    rising = sorted(rising, key=lambda x: -x["hotness"])[:10]
    falling = sorted(falling, key=lambda x: x["hotness"])[:8]
    # 熱點關鍵字（依熱度加權）
    kw_hot = collections.Counter()
    for r in c.execute("SELECT keywords,hotness FROM news WHERE published > ?", (now_unix() - 2 * 86400,)).fetchall():
        try:
            ks = json.loads(r[0]) or []
        except Exception:
            ks = []
        for k in ks:
            kw_hot[k] += r[1]
    keywords = [{"word": k, "weight": v} for k, v in kw_hot.most_common(22)]
    # 事件關聯圖
    top_clusters = cl[:14]
    nodes = [{"id": r["cluster_id"], "repr": r["repr"], "category": r["category"], "hotness": r["hotness"], "size": r["size"]} for r in top_clusters]
    edges = []
    for i in range(len(top_clusters)):
        for j in range(i + 1, len(top_clusters)):
            a = top_clusters[i]; b = top_clusters[j]
            try:
                ka = set(json.loads(a["keywords"]) or []); kb = set(json.loads(b["keywords"]) or [])
            except Exception:
                ka, kb = set(), set()
            shared = ka & kb
            if len(shared) >= 2:
                edges.append({"source": a["cluster_id"], "target": b["cluster_id"], "shared": list(shared)[:3]})
    c.close()
    return {"today_hot": today_hot, "rising": rising, "falling": falling, "breaking": breaking,
            "controversial": controversial, "ranking": ranking, "keywords": keywords, "graph": {"nodes": nodes, "edges": edges}}

def api_briefing():
    today = datetime.date.today()
    day_start = int(datetime.datetime(today.year, today.month, today.day).timestamp())
    c = db()
    def top(cat, n):
        if cat == "全部":
            rows = c.execute("SELECT * FROM news WHERE published >= ? ORDER BY hotness DESC LIMIT ?", (day_start, n)).fetchall()
        else:
            rows = c.execute("SELECT * FROM news WHERE category=? AND published >= ? ORDER BY hotness DESC LIMIT ?", (cat, day_start, n)).fetchall()
        return [dict(r) for r in rows]
    top10 = top("全部", 10)
    brief = {
        "date": today.strftime("%Y年%-m月%-d日"),
        "top10": top10,
        "财经": top("财经", 4), "国际": top("国际", 4), "国内": top("国内", 4),
        "科技": top("科技", 4), "AI": top("AI", 4),
    }
    # 熱點趨勢
    rising = c.execute("SELECT repr,category,hotness FROM clusters WHERE hotness>=45 ORDER BY hotness DESC LIMIT 8").fetchall()
    brief["trending"] = [dict(r) for r in rising]
    # 持續關注（多來源且活躍）
    watch = c.execute("SELECT repr,category,size,sources,hotness FROM clusters WHERE size>=2 ORDER BY hotness DESC LIMIT 8").fetchall()
    brief["watch"] = [dict(r) for r in watch]
    c.close()
    return brief

def api_research(params):
    cid = params.get("cluster_id", [None])[0]
    event = params.get("event", [""])[0].strip()
    c = db()
    if cid:
        arts = [dict(r) for r in c.execute("SELECT * FROM news WHERE cluster_id=? ORDER BY published ASC", (int(cid),)).fetchall()]
    elif event:
        arts = [dict(r) for r in c.execute("SELECT * FROM news WHERE title LIKE ? OR summary LIKE ? ORDER BY published ASC LIMIT 30", (f"%{event}%", f"%{event}%")).fetchall()]
    else:
        c.close(); return {"error": "need cluster_id or event"}
    if not arts:
        c.close(); return {"error": "no articles"}
    clu = c.execute("SELECT * FROM clusters WHERE cluster_id=?", (arts[0]["cluster_id"],)).fetchone()
    c.close()
    arts_sorted = sorted(arts, key=lambda a: a["published"])
    sources = {}
    for a in arts:
        sources.setdefault(a["source"], []).append(a)
    views = [{"source": s, "title": v[0]["title"], "summary": v[0]["summary"]} for s, v in list(sources.items())[:8]]
    timeline = [{"time": a["published_iso"], "source": a["source"], "title": a["title"]} for a in sorted(arts, key=lambda a: -a["published"])[:10]]
    # 影響
    impact = []
    for a in arts:
        for sent in re.split(r"(?<=[。！？!?])", a["summary"] or ""):
            if any(k in sent for k in IMPACT_KW):
                impact.append(sent.strip())
    impact = impact[:5]
    kw_all = collections.Counter()
    for a in arts:
        try:
            kw_all.update(json.loads(a["keywords"]) or [])
        except Exception:
            pass
    topkw = [k for k, _ in kw_all.most_common(8)]
    cause = arts_sorted[0]["summary"] or arts_sorted[0]["title"]
    latest = arts_sorted[-1]["summary"] or arts_sorted[-1]["title"]
    brief_text = "。".join([a["summary"] or a["title"] for a in arts_sorted[:5] if (a["summary"] or a["title"])]).strip()
    if brief_text and not brief_text.endswith("。"):
        brief_text += "。"
    outlook = (f"事件仍持續發展中，後續需關注：各方是否會有進一步表態或行動、"
               f"對相關市場與產業的連鎖影響、以及官方後續調查/決策結果。"
               f"關鍵觀察指標：{('、'.join(topkw[:5]) if topkw else '相關後續報導')}。")
    result = {
        "cluster_id": arts[0]["cluster_id"],
        "repr": (clu["repr"] if clu else arts_sorted[-1]["title"]),
        "category": arts[0]["category"],
        "size": len(arts),
        "sources": sorted(set(a["source"] for a in arts)),
        "keywords": topkw,
        "cause": cause,
        "latest": latest,
        "views": views,
        "timeline": timeline,
        "impact": impact if impact else ["目前報導主要聚焦於事件本身進展，具體影響尚待後續觀察與官方評估。"],
        "outlook": outlook,
        "briefing": brief_text or latest,
        "articles": [{"title": a["title"], "source": a["source"], "link": a["link"], "published_iso": a["published_iso"], "summary": a["summary"]} for a in arts_sorted[:12]],
    }
    # 可選 LLM 增強
    if LLM_API_KEY:
        enh = call_llm("你是新聞分析助手，請用繁體中文把事件簡報寫成一段通順、客觀、約250字的分析。",
                       f"事件：{result['repr']}\n關鍵字：{topkw}\n原始報導摘要：{brief_text[:1500]}", max_tokens=500)
        if enh:
            result["briefing"] = enh
    return result

def api_stats():
    c = db()
    daily = []
    for r in c.execute("SELECT substr(published_iso,1,10) d, COUNT(*) c FROM news GROUP BY d ORDER BY d DESC LIMIT 14").fetchall():
        daily.append({"date": r[0], "count": r[1]})
    daily.reverse()
    cat = [{"category": r[0], "count": r[1]} for r in c.execute("SELECT category,COUNT(*) FROM news GROUP BY category ORDER BY COUNT(*) DESC").fetchall()]
    src_count = c.execute("SELECT COUNT(DISTINCT source) FROM news").fetchone()[0]
    total = c.execute("SELECT COUNT(*) FROM news").fetchone()[0]
    kw = collections.Counter()
    for r in c.execute("SELECT keywords FROM news WHERE published > ?", (now_unix() - 7 * 86400,)).fetchall():
        try:
            kw.update(json.loads(r[0]) or [])
        except Exception:
            pass
    topkw = [{"word": k, "count": v} for k, v in kw.most_common(15)]
    avg_now = c.execute("SELECT AVG(hotness) FROM news WHERE published > ?", (now_unix() - 3 * 86400,)).fetchone()[0] or 0
    c.close()
    return {"daily": daily, "category_ratio": cat, "source_count": src_count, "total": total,
            "avg_hot_now": round(avg_now, 1), "avg_hot_prev": round(prev_avg_hot, 1),
            "keywords": topkw}

def api_status():
    return {"last_sync": last_sync, "last_sync_iso": iso(last_sync) if last_sync else "",
            "last_cycle_new": last_cycle_new, "breaking": len(last_cycle_breaking),
            "breaking_list": last_cycle_breaking, "total": (db().execute("SELECT COUNT(*) FROM news").fetchone()[0]),
            "fetch_interval": json.load(open(FEEDS, encoding="utf-8")).get("fetch_interval_minutes", 15)}

def api_collector_status():
    """採集器運行狀態。僅含統計/狀態資訊，絕不包含任何 API Key。"""
    st = dict(collector_state)
    st["fetch_interval"] = get_fetch_interval()
    st["last_cycle_new"] = last_cycle_new
    try:
        st["total_news"] = db().execute("SELECT COUNT(*) FROM news").fetchone()[0]
    except Exception:
        st["total_news"] = 0
    c = db()
    try:
        logs = c.execute(
            "SELECT run_at,source,status,http_status,resp_len,entries,inserted,error "
            "FROM collector_logs ORDER BY id DESC LIMIT 80").fetchall()
        st["recent_logs"] = [dict(r) for r in logs]
        rows = c.execute(
            "SELECT l.source,l.status,l.http_status,l.resp_len,l.entries,l.inserted,l.error,l.run_at "
            "FROM collector_logs l WHERE l.id = (SELECT MAX(id) FROM collector_logs WHERE source=l.source) "
            "ORDER BY l.source").fetchall()
        st["per_source"] = [dict(r) for r in rows]
    except Exception:
        st["recent_logs"] = []; st["per_source"] = []
    c.close()
    return st

def api_llm_status():
    """回傳 LLM 啟用狀態。絕不包含真實 API Key。"""
    info = llm_status_info()
    info["fallback"] = not info["enabled"]
    return info

def api_search_history():
    c = db()
    rows = c.execute("SELECT q, MAX(ts) FROM searches GROUP BY q ORDER BY MAX(ts) DESC LIMIT 10").fetchall()
    c.close()
    return {"history": [r[0] for r in rows]}

# ----------------------------------------------------------------------------
# 研究工作台（論點研究）後端
# ----------------------------------------------------------------------------
def load_news_set(news_id=None, cluster_id=None, research_id=None):
    c = db()
    news = []; clu = None
    if research_id:
        r = c.execute("SELECT * FROM researches WHERE id=?", (research_id,)).fetchone()
        if r:
            nids = [x[0] for x in c.execute("SELECT news_id FROM research_news WHERE research_id=?", (research_id,)).fetchall()]
            if nids:
                news = [dict(x) for x in c.execute("SELECT * FROM news WHERE id IN (%s)" % ",".join("?"*len(nids)), nids).fetchall()]
            elif r["cluster_id"]:
                news = [dict(x) for x in c.execute("SELECT * FROM news WHERE cluster_id=?", (r["cluster_id"],)).fetchall()]
            if r["cluster_id"]:
                clu = c.execute("SELECT * FROM clusters WHERE cluster_id=?", (r["cluster_id"],)).fetchone()
    elif cluster_id:
        news = [dict(x) for x in c.execute("SELECT * FROM news WHERE cluster_id=?", (cluster_id,)).fetchall()]
        clu = c.execute("SELECT * FROM clusters WHERE cluster_id=?", (cluster_id,)).fetchone()
    elif news_id:
        n = c.execute("SELECT * FROM news WHERE id=?", (news_id,)).fetchone()
        if n:
            news = [dict(n)]
            if n["cluster_id"]:
                clu = c.execute("SELECT * FROM clusters WHERE cluster_id=?", (n["cluster_id"],)).fetchone()
    c.close()
    return news, (dict(clu) if clu else None)

def writing_score_of(n, clu):
    n = dict(n)
    hot = n.get("hotness") or 0
    imp = n.get("importance") == "高"
    brk = n.get("is_breaking")
    size = (dict(clu).get("size") if clu else 1) or 1
    title = ((n.get("title") or "") + " " + (n.get("summary") or ""))
    fresh = recency_factor(n.get("published") or now_unix()) * 100
    contra = sum(1 for k in CONTROVERSY_KW if k in title)
    controversy = min(100, contra * 40 + (40 if imp else 0))
    cnt = sum(1 for k in ("反轉", "意外", "首次", "打破", "顛覆", "罕見", "不按常理", "跌破眼鏡", "令人意外", "逆襲") if k in title)
    counterintuitive = min(100, cnt * 45)
    univ_kw = set("教育 就業 房價 醫療 養老 收入 物價 婚姻 生育 職場 環境 氣候 隱私 食品安全 青年 中年 退休".split())
    univ = sum(1 for k in univ_kw if k in title) * 28
    if n.get("category") in ("社會", "財經", "健康", "文化"): univ += 20
    univ = min(100, univ)
    emo_kw = set("悲劇 震驚 怒 爭議 感動 崩潰 危機 慘 痛 怒火 譴責 絕望 荒謬".split())
    emo = min(100, sum(1 for k in emo_kw if k in title) * 25)
    social = (60 if imp else 20) + (30 if brk else 0) + min(20, size * 4)
    social = min(100, social)
    try: nkw = json.loads(n.get("keywords") or "[]") or []
    except Exception: nkw = []
    ext = min(100, 20 + len(nkw) * 8 + min(30, (size - 1) * 8))
    conflict = min(100, controversy + (30 if size >= 2 else 0))
    writ = min(100, 30 + (20 if len(n.get("summary") or "") > 60 else 0) + (15 if size >= 2 else 0))
    score = (0.18 * fresh + 0.14 * hot + 0.12 * controversy + 0.10 * counterintuitive + 0.10 * univ
             + 0.08 * emo + 0.10 * social + 0.08 * ext + 0.06 * conflict + 0.04 * writ)
    return max(1, min(100, int(score)))

def research_suggest(n, clu):
    n = dict(n)
    cat = n.get("category", "")
    try: kws = json.loads(n.get("keywords") or "[]") or []
    except Exception: kws = []
    kwtext = "、".join(kws[:4]) if kws else "此事件"
    size = (dict(clu).get("size") if clu else 1) or 1
    why_parts = []
    if n.get("is_breaking"): why_parts.append("屬突發事件，時效性強")
    if n.get("importance") == "高": why_parts.append("重要程度高，社會關注度大")
    if size >= 2: why_parts.append(f"已有 {size} 個來源交叉報導，資訊較紮實")
    why_parts.append(f"核心關鍵詞「{kwtext}」具延展討論空間")
    why = "；".join(why_parts) + "。"
    angle_map = {
        "財經": f"除了表面數字，更值得追問：這波變動背後，普通人的資產與生計會受到什麼實質影響？",
        "科技": f"技術熱點「{kwtext}」之下，誰會受益、誰可能被邊緣化？",
        "AI": f"「{kwtext}」的進展，究竟是工具升級，還是產業權力重分配的訊號？",
        "社會": f"個案之外，這是否反映了一個更普遍、很多人正默默經歷的結構性現象？",
        "國際": f"這場國際動態，會如何外溢到我們的生活與選擇？",
        "國內": f"政策或事件落地後，對普通人最直接的改變是什麼？",
        "健康": f"健康議題之外，我們的醫療與生活方式需要怎樣調整？",
        "軍事": f"軍事動態背後，地緣格局與資源分配正在如何改寫？",
    }
    angle = angle_map.get(cat, f"這則新聞為什麼發生？它映照出當下社會什麼集體情緒或焦慮？")
    base = (writing_score_of(n, clu) if clu else 60)
    topics = [
        {"title": f"「{kwtext}」背後的結構性原因", "core_question": f"是什麼長期因素導致「{kwtext}」成為今天的熱點？", "initial_view": "", "score": min(100, base + 6)},
        {"title": f"如果趨勢延續，半年後會怎樣？", "core_question": "這件事的慣性會把我們帶到哪裡？", "initial_view": "", "score": base},
        {"title": f"誰在這件事上立場分歧最大？", "core_question": "不同群體為什麼會得出相反結論？", "initial_view": "", "score": max(40, base - 6)},
    ]
    return {"why": why, "angle": angle, "topics": topics}

def heuristic_research(news_list, clu):
    news_list = sorted(news_list, key=lambda a: (dict(a).get("published") or 0))
    first = dict(news_list[0])
    kwc = collections.Counter()
    for a in news_list:
        try: kwc.update(json.loads(dict(a).get("keywords") or "[]"))
        except Exception: pass
    topkws = [k for k, _ in kwc.most_common(6)]
    kwtext = "、".join(topkws) if topkws else "此議題"
    size = len(news_list)
    sources = sorted(set(dict(a).get("source", "") for a in news_list if dict(a).get("source")))
    src_text = "、".join(sources[:4])
    event = (dict(clu).get("repr") if clu else None) or (first.get("title") or "")
    cat = first.get("category", "")
    combined = "。".join([(dict(a).get("summary") or dict(a).get("title") or "") for a in news_list[:5]])
    phenomenon = summarize(combined, topkws, 200) or event
    cq_map = {
        "財經": f"「{kwtext}」的波動，本質上是市場情緒、政策轉向還是基本面變化主導？普通人該如何理解？",
        "科技": f"「{kwtext}」的突破，究竟是技術成熟還是資本敘事？它會先改變哪一群人？",
        "AI": f"「{kwtext}」的快速演進，是效率革命還是就業與話語權的重新分配？",
        "社會": f"「{kwtext}」頻頻登上熱搜，映照出當下社會什麼集體焦慮或結構性矛盾？",
        "國際": f"「{kwtext}」這場國際動態，會如何外溢並影響普通人的生活與選擇？",
        "國內": f"「{kwtext}」政策/事件落地後，對普通人最直接的改變是什麼？",
        "健康": f"「{kwtext}」健康議題，我們的預防與應對方式需要怎樣調整？",
        "軍事": f"「{kwtext}」軍事動態，正在如何改寫地緣格局與資源分配？",
    }
    core_question = cq_map.get(cat, f"「{event}」為什麼發生？它反映了什麼更深的趨勢？")
    why_matters = f"來自 {src_text or '多家媒體'} 的 {size} 篇報導顯示，此事具備持續發酵的條件：它同時觸及「{kwtext}」等關鍵維度，且涉及多方利益與價值衝突，值得深入拆解而非僅看熱度。"
    arguments = [
        {"content": f"表面現象：{event} 已經發生並被廣泛關注。",
         "explanation": f"根據 {src_text or '多家媒體'} 的報導，「{kwtext}」是核心事實，且已有 {size} 個來源交叉印證，真實性較高。",
         "strength": "強", "credibility": "高", "basis": "事實"},
        {"content": f"關鍵轉折：此事可能不只是單一事件，而是「{kwtext}」相關趨勢的訊號。",
         "explanation": f"從多來源報導的時間與角度分布看，背後存在結構性因素，值得懷疑其是否為更大變化的前奏。",
         "strength": "中", "credibility": "中", "basis": "推論"},
        {"content": f"風險/代價：若趨勢延續，受影響最大的可能是最缺乏話語權的群體。",
         "explanation": "重大變化往往伴隨分配效應，弱勢方的成本常被熱鬧敘事掩蓋，這是寫作時應補強的視角。",
         "strength": "中", "credibility": "待驗證", "basis": "觀點"},
        {"content": f"受益/代價分配：在「{kwtext}」中，誰是最大受益者、誰在默默承擔成本？",
         "explanation": "多數熱點事件都存在分配效應，釐清利益歸屬能讓文章更有結構、避免只停留在情緒面。",
         "strength": "中", "credibility": "中", "basis": "推論"},
        {"content": f"長期趨勢：即使單一事件平息，「{kwtext}」折射的結構性張力仍會以別的形式出現。",
         "explanation": "把一次性事件放到更長的時間軸，能提升文章的厚重感與可延展性，也更容易引出後續追蹤。",
         "strength": "中", "credibility": "待驗證", "basis": "觀點"},
    ]
    controversies = [
        f"支持者認為「{kwtext}」是進步/必要之舉；質疑者則擔心其代價與副作用是否被低估。",
        f"專業圈與大眾解讀出現分歧：數據指向一回事，情緒與敘事卻走向另一回事。",
        f"媒體與輿論的框架選擇：同一組事實，被強調與被忽略的部分，往往決定了公眾的判斷。",
    ]
    counterintuitive = [f"直覺上這件事會往預期方向發展，但「{kwtext}」的細節顯示，真實路徑可能恰好相反。"]
    extension = [
        f"若「{kwtext}」繼續發酵，三個月後我們會用什麼指標判斷它成功了或失敗了？",
        f"有沒有被主流敘事忽略的第三方視角？",
        f"這件事與半年前類似事件相比，本質差異在哪？",
        f"若站在反方立場，最有力的論據會是什麼？我們該如何回應？",
        f"這件事若發生在另一個國家/群體，敘事會有什麼不同？",
    ]
    base = writing_score_of(first, clu) if clu else 60
    topics = [
        {"title": f"「{kwtext}」的結構性成因", "core_question": f"是什麼長期因素讓「{kwtext}」成為今天的熱點？", "initial_view": "", "score": min(100, base + 6)},
        {"title": f"如果趨勢延續，半年後會怎樣？", "core_question": "這件事的慣性會把我們帶到哪裡？", "initial_view": "", "score": base},
        {"title": f"誰在這件事上立場分歧最大？", "core_question": "不同群體為什麼會得出相反結論？", "initial_view": "", "score": max(40, base - 6)},
        {"title": f"「{kwtext}」中的受益者與承擔者", "core_question": f"利益與成本的分配是否公平？", "initial_view": "", "score": max(40, base - 10)},
        {"title": f"當「{kwtext}」成為常態會怎樣？", "core_question": "我們的社會與制度準備好了嗎？", "initial_view": "", "score": max(40, base - 12)},
    ]
    reason = (f"這條新聞本身熱度為 {first.get('hotness',0)}，但評分更看重其背後矛盾的普遍性"
              f"（關鍵詞「{kwtext}」具普遍關切）、觀點衝突明顯，且存在可延展的多個寫作角度，"
              f"因此具備較高文章價值。")
    return {"event": event, "core_question": core_question, "phenomenon": phenomenon, "why_matters": why_matters,
            "arguments": arguments, "controversies": controversies, "counterintuitive": counterintuitive,
            "extension_questions": extension, "topics": topics, "writing_score": base, "writing_reason": reason}

def norm_strength(s):
    m = {"强": "強", "強": "強", "中": "中", "弱": "弱"}
    return m.get((s or "").strip(), "中")

def norm_cred(s):
    m = {"高": "高", "中": "中", "低": "低", "待验证": "待驗證", "待驗證": "待驗證"}
    return m.get((s or "").strip(), "中")

def norm_basis(s):
    m = {"事实": "事实", "事實": "事实", "推论": "推论", "推論": "推论",
         "观点": "观点", "觀點": "观点", "待验证": "待驗證", "待驗證": "待驗證"}
    return m.get((s or "").strip(), "推论")

def _news_blob_for_ai(news_list, limit=10, per=350, cap=4200):
    """將新聞壓縮為送給 LLM 的文字（控制 token：優先標題+摘要+來源+時間）。"""
    parts, total = [], 0
    for a in news_list[:limit]:
        a = dict(a)
        title = (a.get("title") or "").strip()
        summ = clean_html(a.get("summary") or a.get("title") or "")
        if len(summ) > per:
            summ = summ[:per] + "…"
        chunk = f"- 標題：{title}\n  摘要：{summ}\n  來源：{a.get('source','')} | 時間：{a.get('published_iso','')}"
        if total + len(chunk) > cap:
            break
        parts.append(chunk); total += len(chunk)
    return "\n".join(parts)

def _research_json_schema():
    return (
      '{"event":"這件事到底是什麼（客觀一句，基於新聞）",'
      '"core_question":"值得追問的核心問題",'
      '"phenomenon":"現象描述（基於新聞內容，不要重複新聞原文）",'
      '"why_matters":"為什麼值得寫成文章（普遍性/矛盾/衝突/情緒價值）",'
      '"writing_score":0,'
      '"writing_reason":"寫作價值評分理由（0-100，依據新鮮度/爭議性/普遍性/反常識/情緒價值/社會意義/可延展性/觀點衝突/文章可寫性）",'
      '"arguments":[{"content":"論點（一句可被質疑的判斷）","explanation":"論證與依據","strength":"強/中/弱","credibility":"高/中/低/待驗證","basis":"事實/推論/觀點/待驗證","counter_argument":"反方最可能的反駁","my_response":"作者可如何回應"}],'
      '"controversies":["爭議點1","爭議點2","爭議點3"],'
      '"counterintuitive":["反直覺發現1"],'
      '"extension_questions":["延伸問題1","延伸問題2","延伸問題3","延伸問題4","延伸問題5"],'
      '"topics":[{"title":"潛在選題（新聞背後值得寫、但標題沒直接說的問題）","core_question":"選題核心問題","initial_view":"初步觀點(可空)","score":0}]}'
    )

def ai_news_research(news_list, clu):
    if LLM_API_KEY and news_list:
        try:
            blob = _news_blob_for_ai(news_list)
            cluinfo = (f"事件聚合代表標題：{dict(clu).get('repr')}\n來源數：{dict(clu).get('size')}\n來源列表：{dict(clu).get('sources')}") if clu else "（單篇新聞，尚無多來源聚合）"
            sys_p = (
                "你是一位資深媒體研究編輯與專欄寫作顧問。你的任務不是『總結新聞』，而是「從新聞中挖掘值得寫成文章的觀點」。"
                "請站在寫作者視角，主動尋找：新聞背後的問題、普遍性、矛盾、反常識、利益衝突、不同立場，以及可以寫成文章的角度。"
                "嚴格只輸出一個 JSON 物件，不要任何額外說明、不要 markdown 程式碼塊。"
                "關於證據與真實性：所有「數據/事實/案例/研究結論/專家/機構/引用」都必須來自提供的新聞原文；"
                "若新聞本身不足以支持某個結論，絕對不要編造具體數字、論文、專家姓名、機構或新聞來源，請在對應欄位填寫「證據不足，待驗證」。"
                "對每個論點標註 basis：事實（新聞明確提供）/ 推論（由事實推理）/ 觀點（可討論非事實）/ 待驗證（無可靠證據）。"
                "硬性要求：arguments 至少 5 個；controversies 至少 3 個；counterintuitive 至少 1 個；extension_questions 至少 5 個；topics 至少 5 個。"
            )
            user_p = (f"以下是一組關於同一事件/主題的真實新聞：\n{blob}\n\n{cluinfo}\n\n請輸出如下結構的 JSON：\n" + _research_json_schema())
            out = call_llm(sys_p, user_p, max_tokens=2200, json_mode=True)
            if out:
                m = re.search(r"\{.*\}", out, re.S)
                data = json.loads(m.group(0)) if m else None
                if data and isinstance(data, dict):
                    data = _normalize_research_json(data, news_list, clu)
                    data["_from_llm"] = True
                    return data
        except Exception as e:
            print(f"[ai research fallback] {e}")
    return heuristic_research(news_list, clu)

def _normalize_research_json(data, news_list, clu):
    for k in ("event", "core_question", "phenomenon", "why_matters", "writing_reason"):
        data.setdefault(k, "")
    for k in ("arguments", "controversies", "counterintuitive", "extension_questions", "topics"):
        data.setdefault(k, [])
    try:
        data["writing_score"] = int(data.get("writing_score") or 0)
    except Exception:
        data["writing_score"] = 0
    if not (0 < data["writing_score"] <= 100):
        data["writing_score"] = writing_score_of(dict(news_list[0]), clu) if clu else 60
    norm_args = []
    for a in data["arguments"]:
        if not isinstance(a, dict):
            continue
        norm_args.append({
            "content": (a.get("content") or "").strip(),
            "explanation": (a.get("explanation") or "").strip(),
            "strength": norm_strength(a.get("strength")),
            "credibility": norm_cred(a.get("credibility")),
            "basis": norm_basis(a.get("basis")),
            "counter_argument": (a.get("counter_argument") or a.get("counter_view") or "").strip(),
            "my_response": (a.get("my_response") or "").strip(),
        })
    data["arguments"] = [x for x in norm_args if x["content"]]
    # 數量保底：不足時以啟發式補齊（優先保留 AI 內容）
    h = heuristic_research(news_list, clu)
    def pad(key, need):
        cur = list(data.get(key) or [])
        extra = [x for x in (h.get(key) or []) if x not in cur]
        while len(cur) < need and extra:
            cur.append(extra.pop(0))
        data[key] = cur
    pad("arguments", 5)
    pad("controversies", 3)
    pad("counterintuitive", 1)
    pad("extension_questions", 5)
    pad("topics", 5)
    return data

# ----------------------------------------------------------------------------
# 證據自動識別 / 相關素材檢索
# ----------------------------------------------------------------------------
_DATE_RE = re.compile(r"\d{4}[-/年]\d{1,2}[-/月]\d{1,2}(日)?|\d{1,2}月\d{1,2}日|(今天|昨日|本周|本月|去年|前年|近日|當地時間|当地时间)")
_NUM_RE = re.compile(r"\d[\d,\.]*\s?(?:%|％|億|万|萬|千|倍|美元|歐元|元|日圓|英镑|percent|亿|人|名|起|例|宗|件)")
_ORG_RE = re.compile(r"[\u4e00-\u9fa5]{2,}(?:公司|集團|集团|銀行|银行|大學|大学|部|署|局|委員會|委员会|研究所|實驗室|实验室|基金會|基金会|協會|协会)")
_ENGORG_RE = re.compile(r"\b([A-Z][A-Za-z0-9&]+(?:\s[A-Z][A-Za-z0-9&]+){0,3}(?:\s(?:Inc|Corp|Corporation|Group|Bank|Labs|Ltd|LLC|PLC))?)\b")

def _nearest_sentence(text, token, maxlen=120):
    for s in re.split(r"(?<=[。！？!?；;])", text):
        if token in s:
            return s.strip()[:maxlen]
    return token

def extract_evidence(news_list, limit=8):
    """從新聞原文自動抽取 事實/數據/案例/人物/機構/時間/地點，verified=1（來自原文）。"""
    evs, seen = [], set()
    for a in news_list[:10]:
        a = dict(a)
        text = clean_html(a.get("summary") or a.get("title") or "")
        src = a.get("source", "")
        url = a.get("link", "")
        # 數據
        for mm in _NUM_RE.findall(text):
            seg = _nearest_sentence(text, mm)
            key = ("data", seg)
            if key in seen: continue
            seen.add(key)
            evs.append({"type": "数据", "title": f"數據：{seg[:38]}", "content": seg, "source": src, "source_url": url, "verified": 1})
        # 機構（中文）
        for mm in _ORG_RE.findall(text):
            if ("org", mm) in seen: continue
            seen.add(("org", mm))
            evs.append({"type": "机构", "title": f"機構：{mm}", "content": f"新聞提及機構「{mm}」（來源：{src}）", "source": src, "source_url": url, "verified": 1})
        # 機構（英文）
        for mm in _ENGORG_RE.findall(text):
            mm = mm.strip()
            if len(mm) < 3 or ("engorg", mm) in seen: continue
            seen.add(("engorg", mm))
            evs.append({"type": "机构", "title": f"機構：{mm}", "content": f"新聞提及機構「{mm}」（來源：{src}）", "source": src, "source_url": url, "verified": 1})
        # 時間
        for mm in _DATE_RE.findall(text):
            if ("date", mm) in seen: continue
            seen.add(("date", mm))
            evs.append({"type": "时间", "title": f"時間：{mm}", "content": f"新聞提及時間點「{mm}」（來源：{src}）", "source": src, "source_url": url, "verified": 1})
        if len(evs) >= limit:
            break
    return evs[:limit]

def find_related_news(news_list, clu, exclude_ids, limit=8):
    """在 news 庫中按關鍵詞/標題/摘要尋找相關歷史新聞，作為「已有相關素材」。"""
    kws = set()
    for a in news_list:
        try: kws.update(json.loads(dict(a).get("keywords") or "[]") or [])
        except Exception: pass
    kws = [k for k in kws if len(k) >= 2][:8]
    if not kws:
        return []
    like = " OR ".join(["(title LIKE ? OR summary LIKE ? OR keywords LIKE ?)"] * len(kws))
    args = []
    for k in kws:
        args += [f"%{k}%", f"%{k}%", f"%{k}%"]
    sql = f"SELECT id,title,source,link,summary,published_iso FROM news WHERE ({like})"
    if exclude_ids:
        sql += f" AND id NOT IN ({','.join('?'*len(exclude_ids))})"
        args += list(exclude_ids)
    sql += " ORDER BY published DESC LIMIT ?"
    args.append(limit)
    c = db()
    rows = c.execute(sql, args).fetchall()
    c.close()
    return [{"id": r["id"], "title": r["title"], "source": r["source"], "link": r["link"],
             "summary": (r["summary"] or "")[:200], "published_iso": r["published_iso"]} for r in rows]

# ----------------------------------------------------------------------------
# 實體 / 事實 / 衝突 抽取（規則優先；LLM 僅負責複雜判斷，不憑空創造）
# ----------------------------------------------------------------------------
_ENTITY_CACHE = {}
def _get_or_create_entity(c, etype, name, desc=""):
    name = (name or "").strip()
    if not name or len(name) < 2: return None
    key = (etype, name)
    if key in _ENTITY_CACHE: return _ENTITY_CACHE[key]
    row = c.execute("SELECT id FROM entities WHERE type=? AND name=?", (etype, name)).fetchone()
    if row:
        _ENTITY_CACHE[key] = row[0]; return row[0]
    try:
        cur = c.execute("INSERT INTO entities(type,name,description,created_at,updated_at) VALUES(?,?,?,?,?)",
                        (etype, name, desc, iso(now_unix()), iso(now_unix())))
        eid = cur.lastrowid
    except Exception:
        r2 = c.execute("SELECT id FROM entities WHERE type=? AND name=?", (etype, name)).fetchone()
        if not r2: return None
        eid = r2[0]
    _ENTITY_CACHE[key] = eid
    return eid

_PERSON_RE = re.compile(r"(?:總統|主席|首相|總理|執行長|CEO|創辦人|部長|市長|州長|教授|專家|發言人|官員|分析師|法院|檢察官|律師|記者|董事長|局長|校長|將軍|司令)[\s：:、]*([\u4e00-\u9fa5]{2,3})")
_PERSON_TRIM = ("表示","說","说","稱","称","指出","認為","认为","強調","强调","透露","宣布","稱為","称为","表示，","表示:")
_LOC_RE = re.compile(r"[\u4e00-\u9fa5]{2,}(?:國|省|市|縣|區|島|半島|海|河|山|州|灣|港|高原|平原|群島)")
_LOC_KNOWN = set("中國 美國 日本 俄羅斯 烏克蘭 台灣 香港 北京 上海 廣州 深圳 東京 華盛頓 倫敦 巴黎 柏林 莫斯科 首爾 新德里 以色列 巴勒斯坦 加薩 歐盟 聯合國 北韓 南韓 伊朗 敘利亞 法國 德國 英國 印度 巴西 加拿大 澳洲 朝鮮 泰國 越南 新加坡 澳門".split())

def extract_entities(news_list, limit=24):
    out = []; seen = set()
    for a in news_list[:12]:
        a = dict(a)
        text = clean_html(a.get("summary") or a.get("title") or "")
        nid = a.get("id"); src = a.get("source", ""); url = a.get("link", "")
        for mm in list(_ORG_RE.findall(text)) + list(_ENGORG_RE.findall(text)):
            mm = mm.strip()
            if len(mm) < 3 or mm in seen: continue
            seen.add(mm); out.append({"type": "organization", "name": mm, "news_id": nid, "source": src, "url": url, "desc": f"新聞提及機構（{src}）"})
        for mm in _LOC_RE.findall(text):
            if mm in seen: continue
            if mm not in _LOC_KNOWN and len(mm) > 5: continue
            seen.add(mm); out.append({"type": "location", "name": mm, "news_id": nid, "source": src, "url": url, "desc": f"新聞提及地點（{src}）"})
        for mm in _PERSON_RE.findall(text):
            name = mm.strip()
            for suf in _PERSON_TRIM:
                if name.endswith(suf): name = name[: -len(suf)]
            if len(name) < 2 or name in seen: continue
            seen.add(name); out.append({"type": "person", "name": name, "news_id": nid, "source": src, "url": url, "desc": f"新聞提及人物（{src}）"})
        if len(out) >= limit: break
    return out[:limit]

def extract_facts(news_list, limit=12):
    cands = []
    for a in news_list[:12]:
        a = dict(a)
        text = clean_html(a.get("summary") or a.get("title") or "")
        src = a.get("source", ""); url = a.get("link", ""); pub = a.get("published_iso", "")
        for mm in _NUM_RE.findall(text):
            subj = _nearest_subject(text, mm)
            content = f"{subj}{mm}"
            key = re.sub(r"[^一-鿿a-z0-9]", "", mm)
            cands.append({"text": content, "source": src, "url": url, "pub": pub, "key": key})
        for mm in _ORG_RE.findall(text) + list(_ENGORG_RE.findall(text)):
            mm = mm.strip()
            if len(mm) < 3: continue
            seg = f"{mm}（{src} 報導）"
            key = re.sub(r"[^一-鿿a-z0-9]", "", mm)[:12]
            cands.append({"text": seg, "source": src, "url": url, "pub": pub, "key": key})
    bykey = collections.defaultdict(list)
    for cd in cands:
        if cd["key"]: bykey[cd["key"]].append(cd)
    facts = []; seen_text = set()
    for key, items in bykey.items():
        srcs = set(i["source"] for i in items if i["source"])
        rep = max(items, key=lambda x: len(x["text"]))
        content = rep["text"]
        if content in seen_text: continue
        seen_text.add(content)
        confirm = len(srcs)
        status = "confirmed" if confirm >= 2 else "single"
        first_seen = min((i["pub"] for i in items if i["pub"]), default="")
        facts.append({"content": content, "source": rep["source"], "source_url": rep["url"],
                      "confirm_count": confirm, "status": status, "first_seen": first_seen})
    return facts[:limit]

_SUBJ_KW = ["傷亡","伤亡","受傷","受伤","死亡","罹難","遇难","遇難","罰款","罚款","罰金","罚金","投資","投资","融資","融资","裁員","裁员","損失","损失","營收","营收","獲利","获利","盈利","虧損","亏损","GDP","通膨","通胀","失業","失业","產值","产值","交易","募資","募资","預算","预算","債務","债务","匯率","汇率","利率","增長","增长","下跌","上漲","上涨","爆發","爆发","感染","確診","确诊"]
def _nearest_subject(text, token):
    """取數字前最近的「度量詞」（如 造成/遇難/裁員），跨來源穩定可用於衝突分組。"""
    for s in re.split(r"(?<=[。！？!?；;])", text):
        if token in s:
            idx = s.index(token)
            pre = s[:idx]
            pre = re.sub(r"[\s，。、,.;:：！!?？；;]+$", "", pre)
            m = re.search(r"[\u4e00-\u9fa5]{1,4}$", pre)
            if m:
                subj = m.group(0)
                if subj not in STOP:
                    return subj
            for w in re.findall(r"[\u4e00-\u9fa5]{2,4}", s):
                if w not in STOP: return w
            return "事件"
    return "事件"

def detect_conflicts(news_list, limit=10):
    groups = collections.defaultdict(list)  # (subj,unit) -> [(source, valuestr), ...]
    for a in news_list[:12]:
        a = dict(a)
        text = clean_html(a.get("summary") or a.get("title") or "")
        src = a.get("source", "")
        seen_in_news = set()
        for mm in _NUM_RE.findall(text):
            m = re.match(r"([\d,\.]+)\s*(.*)", mm)
            if not m: continue
            num, unit = m.group(1), m.group(2)
            key = (num, unit, src)
            if key in seen_in_news: continue
            seen_in_news.add(key)
            subj = _nearest_subject(text, mm)
            groups[(subj, unit)].append((src, f"{num}{unit}"))
    conflicts = []
    for (subj, unit), pairs in groups.items():
        # 找兩個「不同數值、不同來源」的說法
        distinct = {}
        for src, val in pairs:
            distinct.setdefault(val, set()).add(src)
        if len(distinct) < 2: continue
        vals = list(distinct.keys())
        a0, a1 = vals[0], vals[1]
        conflicts.append({"type": "数字", "claim_a": f"{subj}：{a0}", "source_a": "/".join(sorted(distinct[a0])),
                          "claim_b": f"{subj}：{a1}", "source_b": "/".join(sorted(distinct[a1]))})
        if len(conflicts) >= limit: break
    return conflicts

def rebuild_research_derived(c, rid, news_list):
    """根據關聯新聞重建：實體連結、已確認事實、信息衝突。"""
    ents = extract_entities(news_list)
    for e in ents:
        eid = _get_or_create_entity(c, e["type"], e["name"], e.get("desc", ""))
        if not eid: continue
        c.execute("INSERT OR IGNORE INTO research_entities(research_id,entity_id) VALUES(?,?)", (rid, eid))
        if e.get("news_id"):
            c.execute("INSERT OR IGNORE INTO news_entities(news_id,entity_id) VALUES(?,?)", (e["news_id"], eid))
    c.execute("DELETE FROM facts WHERE research_id=?", (rid,))
    for f in extract_facts(news_list):
        c.execute("INSERT INTO facts(research_id,content,source,source_url,first_seen,confirm_count,status,created_at) VALUES(?,?,?,?,?,?,?,?)",
                  (rid, f["content"], f["source"], f["source_url"], f["first_seen"], f["confirm_count"], f["status"], iso(now_unix())))
    c.execute("DELETE FROM conflicts WHERE research_id=?", (rid,))
    for cf in detect_conflicts(news_list):
        c.execute("INSERT INTO conflicts(research_id,type,claim_a,source_a,claim_b,source_b,created_at) VALUES(?,?,?,?,?,?,?)",
                  (rid, cf["type"], cf["claim_a"], cf["source_a"], cf["claim_b"], cf["source_b"], iso(now_unix())))

def find_related_researches(rid, limit=8):
    """依賴關聯新聞關鍵詞重疊，發現相關歷史事件（不強制相同事件）。"""
    c = db()
    cur_kw = set()
    for r in c.execute("SELECT n.keywords FROM news n JOIN research_news rn ON n.id=rn.news_id WHERE rn.research_id=?", (rid,)).fetchall():
        try: cur_kw.update(json.loads(r[0] or "[]") or [])
        except Exception: pass
    cur_kw = set(k for k in cur_kw if len(k) >= 2)
    if not cur_kw:
        c.close(); return []
    others = [x[0] for x in c.execute("SELECT DISTINCT research_id FROM research_news WHERE research_id!=?", (rid,)).fetchall()]
    result = []
    for oid in others:
        okw = set()
        for r in c.execute("SELECT n.keywords FROM news n JOIN research_news rn ON n.id=rn.news_id WHERE rn.research_id=?", (oid,)).fetchall():
            try: okw.update(json.loads(r[0] or "[]") or [])
            except Exception: pass
        shared = cur_kw & okw
        if shared:
            score = min(99, int(40 + 60 * len(shared) / max(1, len(cur_kw))))
            title = c.execute("SELECT title FROM researches WHERE id=?", (oid,)).fetchone()
            result.append({"id": oid, "title": title[0] if title else "", "score": score, "shared": list(shared)[:5]})
    c.close()
    result.sort(key=lambda x: -x["score"])
    return result[:limit]

def update_tracked_researches():
    """採集完成後自動關聯 tracking=1 研究的相關新新聞；不呼叫 LLM、不拋錯。"""
    try:
        c = db()
        rows = [dict(r) for r in c.execute("SELECT id,cluster_id,last_news_count FROM researches WHERE tracking=1").fetchall()]
        for r in rows:
            rid = r["id"]
            nids = set(x[0] for x in c.execute("SELECT news_id FROM research_news WHERE research_id=?", (rid,)).fetchall())
            existing = [dict(x) for x in c.execute("SELECT * FROM news WHERE id IN (%s)" % (",".join("?"*len(nids)) or "0"), list(nids)).fetchall()] if nids else []
            kws = set()
            for n in existing:
                try: kws.update(json.loads(n.get("keywords") or "[]") or [])
                except Exception: pass
            kws = [k for k in kws if len(k) >= 2][:8]
            new_added = 0
            if kws:
                like = " OR ".join(["(title LIKE ? OR summary LIKE ? OR keywords LIKE ?)"] * len(kws))
                args = []
                for k in kws: args += [f"%{k}%", f"%{k}%", f"%{k}%"]
                cand = [dict(x) for x in c.execute(f"SELECT * FROM news WHERE ({like})", args).fetchall()]
                for n in cand:
                    if n["id"] in nids: continue
                    cur = c.execute("INSERT OR IGNORE INTO research_news(research_id,news_id) VALUES(?,?)", (rid, n["id"]))
                    if cur.rowcount > 0:
                        new_added += 1; nids.add(n["id"])
            if r["cluster_id"]:
                cln = [x[0] for x in c.execute("SELECT id FROM news WHERE cluster_id=? AND id NOT IN (%s)" % (",".join("?"*len(nids)) or "0"), [r["cluster_id"]] + list(nids)).fetchall()]
                for nid in cln:
                    cur = c.execute("INSERT OR IGNORE INTO research_news(research_id,news_id) VALUES(?,?)", (rid, nid))
                    if cur.rowcount > 0:
                        new_added += 1; nids.add(nid)
            now_iso = iso(now_unix()); total = len(nids)
            if new_added > 0:
                c.execute("UPDATE researches SET last_checked_at=?,last_news_count=?,updated_at=? WHERE id=?", (now_iso, total, now_iso, rid))
                allnews = [dict(x) for x in c.execute("SELECT * FROM news WHERE id IN (%s)" % (",".join("?"*len(nids)) or "0"), list(nids)).fetchall()] if nids else []
                try: rebuild_research_derived(c, rid, allnews)
                except Exception as e: print(f"[tracked derive err] {e}")
            else:
                c.execute("UPDATE researches SET last_checked_at=?,last_news_count=? WHERE id=?", (now_iso, total, rid))
        c.commit(); c.close()
    except Exception as e:
        print(f"[update_tracked_researches error] {e}")

_board_ai_cache = {"ts": 0, "key": "", "data": None}
BOARD_AI_TTL = 30 * 60

def ai_board_evaluate(cands):
    """單次批次呼叫 LLM，對候選 TOP30 做寫作價值評分 + 背後選題角度。
    回傳 {news_id: {"score":int,"angle":str}}；失敗/未配置回傳 {}（呼叫方以啟發式兜底，不報錯）。"""
    global _board_ai_cache
    if not LLM_API_KEY or not cands:
        return {}
    items = cands[:30]
    key = "|".join(str(x["id"]) for x in items)
    now = now_unix()
    if _board_ai_cache["key"] == key and now - _board_ai_cache["ts"] < BOARD_AI_TTL:
        return _board_ai_cache["data"] or {}
    lines = []
    for i, x in enumerate(items):
        lines.append(f"{i+1}. id={x['id']} | 分類={x.get('category','')} | 來源={x.get('source','')} | 熱度={x.get('hotness',0)} | 標題：{x.get('title','')}")
    blob = "\n".join(lines)
    sys_p = ("你是媒體選題編輯。下方是今日候選新聞（編號+id+標題）。請對每一條評估「寫作價值」(0-100，"
             "綜合新鮮度/爭議性/普遍性/反常識/情緒價值/社會意義/可延展性/觀點衝突/文章可寫性)，"
             "並給出一個「新聞標題沒直接說、但值得寫成文章的角度」（從事件進入社會問題/普遍情緒/結構矛盾，不要只重複事件本身）。"
             "嚴格只輸出 JSON 陣列，順序與輸入編號一致，每項格式："
             '{"id":<數字>,"score":<0-100整數>,"angle":"角度文字"}。不要任何其他文字。')
    user_p = f"候選新聞：\n{blob}\n\n請輸出 JSON 陣列。"
    out = call_llm(sys_p, user_p, max_tokens=2600, json_mode=True, timeout=60)
    result = {}
    if out:
        try:
            m = re.search(r"\[.*\]", out, re.S)
            arr = json.loads(m.group(0)) if m else None
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, dict) and "id" in it:
                        try: sc = int(it.get("score") or 0)
                        except Exception: sc = 0
                        result[int(it["id"])] = {"score": max(0, min(100, sc)), "angle": (it.get("angle") or "").strip()}
        except Exception as e:
            print(f"[board ai parse fail] {e}")
    _board_ai_cache = {"ts": now, "key": key, "data": result}
    return result

def api_research_board():
    c = db()
    researched_news = set(r[0] for r in c.execute("SELECT news_id FROM researches WHERE news_id IS NOT NULL").fetchall())
    researched_clusters = set(r[0] for r in c.execute("SELECT cluster_id FROM researches WHERE cluster_id IS NOT NULL").fetchall())
    rows = c.execute("SELECT * FROM news WHERE published>? ORDER BY hotness DESC LIMIT 150", (now_unix() - 3 * 86400,)).fetchall()
    cands = []; clu_cache = {}
    for n in rows:
        n = dict(n)
        if n["id"] in researched_news: continue
        if n["cluster_id"] and n["cluster_id"] in researched_clusters: continue
        cid = n["cluster_id"]
        clu = clu_cache.get(cid)
        if clu is None and cid:
            r2 = c.execute("SELECT * FROM clusters WHERE cluster_id=?", (cid,)).fetchone()
            clu = dict(r2) if r2 else None
            clu_cache[cid] = clu
        ws = writing_score_of(n, clu)
        sg = research_suggest(n, clu)
        cands.append({"id": n["id"], "cluster_id": cid, "title": n["title"], "category": n["category"],
                      "source": n["source"], "link": n["link"], "published_iso": n["published_iso"],
                      "hotness": n["hotness"], "writing_score": ws, "why": sg["why"], "angle": sg["angle"], "topics": sg["topics"]})
    # 第一階段：本地規則排序（熱度/重要度/突發/聚類）取候選
    cands.sort(key=lambda x: -x["writing_score"])
    ai_used = False
    # 第二階段：僅對 TOP30 做一次批次 AI 價值判斷（緩存 30 分鐘；失敗回退啟發式）
    if LLM_API_KEY and cands:
        ai_scores = ai_board_evaluate(cands)
        if ai_scores:
            for n in cands:
                if n["id"] in ai_scores:
                    n["writing_score"] = ai_scores[n["id"]]["score"]
                    if ai_scores[n["id"]]["angle"]:
                        n["angle"] = ai_scores[n["id"]]["angle"]
            ai_used = True
            cands.sort(key=lambda x: -x["writing_score"])
    today_worthy = cands[:10]
    my = [dict(r) for r in c.execute("SELECT * FROM researches ORDER BY updated_at DESC").fetchall()]
    def by(st): return [x for x in my if x["status"] in st]
    studying = by(("研究中",))
    need_ev = by(("待补证据",))
    need_verify = by(("待验证",))
    done = by(("已完成", "已转文章"))
    # 今日選題機會：來自評分後 TOP30 中高分者，突出「標題背後的問題」
    opps = []
    for n in cands[:30]:
        if n["writing_score"] < 50: continue
        opps.append({"news_id": n["id"], "cluster_id": n["cluster_id"], "news_title": n["title"],
                     "angle": n["angle"], "writing_score": n["writing_score"]})
        if len(opps) >= 8: break
    acount = c.execute("SELECT COUNT(*) FROM arguments").fetchone()[0]
    tcount = c.execute("SELECT COUNT(*) FROM topics").fetchone()[0]
    # 🔔 持續追蹤：有進展的研究（關聯新聞數 > 上次檢查時數量）
    tracking_updates = []
    for r in my:
        if (r.get("tracking") or 0) == 1:
            cur_n = c.execute("SELECT COUNT(*) FROM research_news WHERE research_id=?", (r["id"],)).fetchone()[0]
            last_n = r.get("last_news_count") or 0
            if cur_n > last_n:
                tracking_updates.append({"id": r["id"], "title": r["title"], "new_count": cur_n - last_n, "updated_at": r["updated_at"]})
    c.close()
    return {"today_worthy": today_worthy, "studying": studying, "need_evidence": need_ev,
            "need_verify": need_verify, "done": done, "topic_opportunities": opps,
            "tracking_updates": tracking_updates,
            "ai_enabled": bool(LLM_API_KEY), "ai_used": ai_used,
            "counts": {"research": len(my), "argument": acount, "topic": tcount}}

def api_research_detail(params):
    rid = params.get("id", [None])[0]
    try: rid = int(rid)
    except Exception: return {"error": "need id"}
    c = db()
    r = c.execute("SELECT * FROM researches WHERE id=?", (rid,)).fetchone()
    if not r: c.close(); return {"error": "not found"}
    r = dict(r)
    news = [dict(x) for x in c.execute("SELECT * FROM news n JOIN research_news rn ON n.id=rn.news_id WHERE rn.research_id=?", (rid,)).fetchall()]
    if not news and r["cluster_id"]:
        news = [dict(x) for x in c.execute("SELECT * FROM news WHERE cluster_id=?", (r["cluster_id"],)).fetchall()]
    args = [dict(x) for x in c.execute("SELECT * FROM arguments WHERE research_id=? ORDER BY id", (rid,)).fetchall()]
    for a in args:
        a["evidence"] = [dict(e) for e in c.execute("SELECT * FROM evidence WHERE argument_id=?", (a["id"],)).fetchall()]
    topics = [dict(x) for x in c.execute("SELECT * FROM topics WHERE research_id=? ORDER BY id", (rid,)).fetchall()]
    for t in topics:
        t["argument_ids"] = [x[0] for x in c.execute("SELECT argument_id FROM topic_arguments WHERE topic_id=?", (t["id"],)).fetchall()]
    for f in ("controversies", "counterintuitive", "extension_questions"):
        try: r[f] = json.loads(r.get(f) or "[]")
        except Exception: r[f] = []
    # 已有相關素材：在 news 庫中按關鍵詞搜尋（不含已連結的新聞）
    clu = None
    if r["cluster_id"]:
        cl = c.execute("SELECT * FROM clusters WHERE cluster_id=?", (r["cluster_id"],)).fetchone()
        clu = dict(cl) if cl else None
    nids = [n["id"] for n in news]
    related_news = find_related_news(news, clu, exclude_ids=set(nids), limit=8)
    # 事件研究系統：事實 / 衝突 / 實體 / 相關事件 / 更新歷史（均來自關聯新聞，不憑空生成）
    facts = [dict(x) for x in c.execute("SELECT * FROM facts WHERE research_id=? ORDER BY confirm_count DESC, id", (rid,)).fetchall()]
    conflicts = [dict(x) for x in c.execute("SELECT * FROM conflicts WHERE research_id=? ORDER BY id", (rid,)).fetchall()]
    ent_rows = [dict(x) for x in c.execute("SELECT e.* FROM entities e JOIN research_entities re ON e.id=re.entity_id WHERE re.research_id=?", (rid,)).fetchall()]
    entities = {}
    for e in ent_rows: entities.setdefault(e["type"], []).append(e)
    auto_related = find_related_researches(rid)
    manual_related = []
    for x in c.execute("SELECT related_id FROM research_links WHERE research_id=?", (rid,)).fetchall():
        rr = c.execute("SELECT id,title FROM researches WHERE id=?", (x[0],)).fetchone()
        if rr: manual_related.append({"id": rr[0], "title": rr[1], "score": 100, "shared": [], "manual": True})
    related = auto_related + manual_related
    ups = [dict(x) for x in c.execute("SELECT * FROM research_updates WHERE research_id=? ORDER BY id", (rid,)).fetchall()]
    for u in ups:
        try: u["new_facts"] = json.loads(u.get("new_facts") or "[]")
        except Exception: u["new_facts"] = []
        try: u["new_conflicts"] = json.loads(u.get("new_conflicts") or "[]")
        except Exception: u["new_conflicts"] = []
    c.close()
    return {"research": r, "news": news, "arguments": args, "topics": topics, "related_news": related_news,
            "facts": facts, "conflicts": conflicts, "entities": entities, "related": related, "updates": ups,
            "tracking": r.get("tracking", 0), "last_checked_at": r.get("last_checked_at", ""), "last_news_count": r.get("last_news_count", 0)}

def api_research_create(body):
    news_id = body.get("news_id"); cluster_id = body.get("cluster_id")
    title = body.get("title")
    c = db()
    if not title:
        if cluster_id:
            cl = c.execute("SELECT repr FROM clusters WHERE cluster_id=?", (cluster_id,)).fetchone(); title = cl["repr"] if cl else "未命名研究"
        elif news_id:
            n = c.execute("SELECT title FROM news WHERE id=?", (news_id,)).fetchone(); title = n["title"] if n else "未命名研究"
        else: title = "未命名研究"
    ct = iso(now_unix())
    cur = c.execute("INSERT INTO researches(news_id,cluster_id,title,core_question,phenomenon,my_view,status,writing_score,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (news_id, cluster_id, title, "", "", "", "待研究", 0, ct, ct))
    rid = cur.lastrowid
    if news_id:
        c.execute("INSERT OR IGNORE INTO research_news(research_id,news_id) VALUES(?,?)", (rid, news_id))
    elif cluster_id:
        for nid in c.execute("SELECT id FROM news WHERE cluster_id=?", (cluster_id,)).fetchall():
            c.execute("INSERT OR IGNORE INTO research_news(research_id,news_id) VALUES(?,?)", (rid, nid[0]))
    news = [dict(x) for x in c.execute("SELECT * FROM news WHERE id IN (SELECT news_id FROM research_news WHERE research_id=?)", (rid,)).fetchall()]
    try: rebuild_research_derived(c, rid, news)
    except Exception as e: print(f"[create derive err] {e}")
    c.commit(); c.close()
    return {"id": rid, "ok": True}

def api_research_update(body):
    rid = body.get("id")
    if not rid: return {"error": "need id"}
    c = db()
    r = c.execute("SELECT id FROM researches WHERE id=?", (rid,)).fetchone()
    if not r: c.close(); return {"error": "not found"}
    flds = {}
    for k in ("title", "core_question", "phenomenon", "my_view", "writing_score", "event", "why_matters"):
        if k in body and body[k] is not None: flds[k] = body[k]
    if body.get("status"): flds["status"] = body["status"]
    if flds:
        setc = ",".join(f"{k}=?" for k in flds) + ",updated_at=?"
        c.execute(f"UPDATE researches SET {setc} WHERE id=?", list(flds.values()) + [iso(now_unix()), rid])
    c.commit(); c.close()
    return {"ok": True}

def api_research_status(body):
    rid = body.get("id"); st = body.get("status")
    if not rid or not st: return {"error": "need id & status"}
    c = db(); c.execute("UPDATE researches SET status=?,updated_at=? WHERE id=?", (st, iso(now_unix()), rid)); c.commit(); c.close()
    return {"ok": True}

def api_research_ai(body):
    news_id = body.get("news_id"); cluster_id = body.get("cluster_id"); research_id = body.get("research_id")
    force = bool(body.get("force"))
    news, clu = load_news_set(news_id, cluster_id, research_id)
    if not news: return {"error": "no news"}
    c = db()
    # 定位已存在的研究（用於緩存判斷）
    existing = None
    if research_id:
        existing = c.execute("SELECT * FROM researches WHERE id=?", (research_id,)).fetchone()
    elif news_id or cluster_id:
        if news_id:
            existing = c.execute("SELECT * FROM researches WHERE news_id=?", (news_id,)).fetchone()
        if not existing and cluster_id:
            existing = c.execute("SELECT * FROM researches WHERE cluster_id=?", (cluster_id,)).fetchone()
    # 命中有效 AI 緩存且不強制重分析 -> 直接讀庫，不重複呼叫 LLM
    if existing and not force and (existing["ai_generated"] or 0) == 1:
        c.close()
        return {"id": existing["id"], "ok": True, "cached": True, "ai_generated": True, "from_llm": True}
    rid = existing["id"] if existing else None
    ai = ai_news_research(news, clu)
    used_llm = bool(ai.pop("_from_llm", False))
    ct = iso(now_unix())
    ai_gen = 1 if used_llm else 0
    ai_model = LLM_MODEL if used_llm else ""
    ai_at = ct if used_llm else ""
    if rid is None:
        title = body.get("title")
        if not title:
            if clu: title = clu.get("repr")
            elif news: title = dict(news[0]).get("title") or "未命名研究"
            else: title = "未命名研究"
        cur = c.execute("INSERT INTO researches(news_id,cluster_id,title,core_question,phenomenon,my_view,status,writing_score,event,why_matters,controversies,counterintuitive,extension_questions,ai_generated,ai_model,ai_generated_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (news_id, (clu.get("cluster_id") if clu else (cluster_id or None)), title, ai["core_question"], ai["phenomenon"], "", "研究中", ai["writing_score"],
             ai["event"], ai["why_matters"], json.dumps(ai["controversies"], ensure_ascii=False),
             json.dumps(ai["counterintuitive"], ensure_ascii=False), json.dumps(ai["extension_questions"], ensure_ascii=False),
             ai_gen, ai_model, ai_at, ct, ct))
        rid = cur.lastrowid
        for n in news:
            c.execute("INSERT OR IGNORE INTO research_news(research_id,news_id) VALUES(?,?)", (rid, dict(n)["id"]))
    else:
        c.execute("UPDATE researches SET core_question=?,phenomenon=?,event=?,why_matters=?,controversies=?,counterintuitive=?,extension_questions=?,writing_score=?,status=?,ai_generated=?,ai_model=?,ai_generated_at=?,updated_at=? WHERE id=?",
            (ai["core_question"], ai["phenomenon"], ai["event"], ai["why_matters"],
             json.dumps(ai["controversies"], ensure_ascii=False), json.dumps(ai["counterintuitive"], ensure_ascii=False),
             json.dumps(ai["extension_questions"], ensure_ascii=False), ai["writing_score"], "研究中", ai_gen, ai_model, ai_at, iso(now_unix()), rid))
    # 先清舊證據與論點，再重建
    c.execute("DELETE FROM evidence WHERE argument_id IN (SELECT id FROM arguments WHERE research_id=?)", (rid,))
    c.execute("DELETE FROM arguments WHERE research_id=?", (rid,))
    arg_ids = []
    for a in ai["arguments"]:
        cur = c.execute("INSERT INTO arguments(research_id,content,explanation,strength,credibility,basis,my_response,counter_view,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (rid, a.get("content",""), a.get("explanation",""), a.get("strength","中"), a.get("credibility","中"),
             a.get("basis","推论"), a.get("my_response",""), a.get("counter_argument",""), iso(now_unix()), iso(now_unix())))
        arg_ids.append(cur.lastrowid)
    # 證據自動識別：從新聞原文抽取（verified=1），並連結到最相關的論點
    evs = extract_evidence(news, limit=8)
    def best_arg(ev_content):
        best, bestscore = (arg_ids[0] if arg_ids else None), -1
        if not arg_ids: return None
        for i, a in enumerate(ai["arguments"]):
            score = sum(1 for w in (a.get("content","")+a.get("explanation","")) if w and w in ev_content)
            if score > bestscore:
                bestscore = score; best = arg_ids[i]
        return best
    for ev in evs:
        aid = best_arg(ev["content"])
        if aid:
            c.execute("INSERT INTO evidence(argument_id,type,title,content,source,source_url,verified,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (aid, ev["type"], ev["title"], ev["content"], ev["source"], ev["source_url"], 1, iso(now_unix())))
    c.execute("DELETE FROM topics WHERE research_id=?", (rid,))
    for t in ai.get("topics", []):
        c.execute("INSERT INTO topics(research_id,title,core_question,initial_view,status,score,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (rid, t.get("title", ""), t.get("core_question", ""), t.get("initial_view", ""), "待研究", int(t.get("score", 0) or 0), iso(now_unix()), iso(now_unix())))
    # 重建事件研究衍生資料：實體 / 事實 / 衝突（基於關聯新聞，不憑空生成）
    allnews = [dict(x) for x in c.execute("SELECT * FROM news WHERE id IN (SELECT news_id FROM research_news WHERE research_id=?)", (rid,)).fetchall()]
    try: rebuild_research_derived(c, rid, allnews)
    except Exception as e: print(f"[ai derive err] {e}")
    c.commit(); c.close()
    # 若已配置 LLM 但本次呼叫失敗，標記 fallback（仍回傳啟發式結果，絕不讓頁面報錯）
    llm_error = bool(LLM_API_KEY) and not used_llm
    return {"id": rid, "ok": True, "ai_generated": bool(ai_gen), "from_llm": bool(used_llm),
            "cached": False, "llm_error": llm_error, "fallback": llm_error, "ai": ai}

def api_research_add_related_evidence(body):
    """將一則「已有相關素材」新聞加入第一個論點的證據（verified=1，來自新聞原文）。"""
    rid = body.get("research_id"); nid = body.get("news_id")
    if not rid or not nid: return {"error": "need research_id & news_id"}
    c = db()
    a = c.execute("SELECT id FROM arguments WHERE research_id=? ORDER BY id LIMIT 1", (rid,)).fetchone()
    if not a: c.close(); return {"error": "no argument"}
    n = c.execute("SELECT title,summary,source,link FROM news WHERE id=?", (nid,)).fetchone()
    if not n: c.close(); return {"error": "news not found"}
    cur = c.execute("INSERT INTO evidence(argument_id,type,title,content,source,source_url,verified,created_at) VALUES(?,?,?,?,?,?,?,?)",
        (a["id"], "新聞", n["title"], (n["summary"] or "")[:200], n["source"], n["link"], 1, iso(now_unix())))
    eid = cur.lastrowid; c.commit(); c.close()
    return {"id": eid, "ok": True}

def api_argument_create(body):
    rid = body.get("research_id")
    if not rid: return {"error": "need research_id"}
    c = db()
    cur = c.execute("INSERT INTO arguments(research_id,content,explanation,strength,credibility,my_response,counter_view,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (rid, body.get("content", ""), body.get("explanation", ""), body.get("strength", "中"), body.get("credibility", "中"), body.get("my_response", ""), body.get("counter_view", ""), iso(now_unix()), iso(now_unix())))
    aid = cur.lastrowid; c.commit(); c.close()
    return {"id": aid, "ok": True}

def api_argument_update(body):
    aid = body.get("id")
    if not aid: return {"error": "need id"}
    c = db()
    flds = {}
    for k in ("content", "explanation", "strength", "credibility", "my_response", "counter_view"):
        if k in body and body[k] is not None: flds[k] = body[k]
    if flds:
        setc = ",".join(f"{k}=?" for k in flds) + ",updated_at=?"
        c.execute(f"UPDATE arguments SET {setc} WHERE id=?", list(flds.values()) + [iso(now_unix()), aid])
    c.commit(); c.close()
    return {"ok": True}

def api_argument_delete(body):
    aid = body.get("id")
    if not aid: return {"error": "need id"}
    c = db(); c.execute("DELETE FROM evidence WHERE argument_id=?", (aid,)); c.execute("DELETE FROM topic_arguments WHERE argument_id=?", (aid,)); c.execute("DELETE FROM arguments WHERE id=?", (aid,)); c.commit(); c.close()
    return {"ok": True}

def api_evidence_create(body):
    aid = body.get("argument_id")
    if not aid: return {"error": "need argument_id"}
    c = db()
    cur = c.execute("INSERT INTO evidence(argument_id,type,title,content,source,source_url,verified,created_at) VALUES(?,?,?,?,?,?,?,?)",
        (aid, body.get("type", "其他"), body.get("title", ""), body.get("content", ""), body.get("source", ""), body.get("source_url", ""), 1 if body.get("verified") else 0, iso(now_unix())))
    eid = cur.lastrowid; c.commit(); c.close()
    return {"id": eid, "ok": True}

def api_topic_create(body):
    rid = body.get("research_id")
    if not rid: return {"error": "need research_id"}
    c = db()
    cur = c.execute("INSERT INTO topics(research_id,title,core_question,initial_view,status,score,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
        (rid, body.get("title", ""), body.get("core_question", ""), body.get("initial_view", ""), body.get("status", "待研究"), int(body.get("score", 0) or 0), iso(now_unix()), iso(now_unix())))
    tid = cur.lastrowid; c.commit(); c.close()
    return {"id": tid, "ok": True}

def api_topic_update(body):
    tid = body.get("id")
    if not tid: return {"error": "need id"}
    c = db()
    flds = {}
    for k in ("title", "core_question", "initial_view", "status", "score"):
        if k in body and body[k] is not None: flds[k] = body[k]
    if flds:
        setc = ",".join(f"{k}=?" for k in flds) + ",updated_at=?"
        c.execute(f"UPDATE topics SET {setc} WHERE id=?", list(flds.values()) + [iso(now_unix()), tid])
    c.commit(); c.close()
    return {"ok": True}

def api_search(params):
    q = params.get("q", [""])[0].strip()
    if not q: return {"results": []}
    like = f"%{q}%"
    c = db(); out = []
    for r in c.execute("SELECT id,title,category,source FROM news WHERE title LIKE ? OR summary LIKE ? OR keywords LIKE ? ORDER BY hotness DESC LIMIT 12", (like, like, like)).fetchall():
        out.append({"type": "新聞", "id": r["id"], "title": r["title"], "sub": f"{r['category']} · {r['source']}"})
    for r in c.execute("SELECT id,title,status,writing_score FROM researches WHERE title LIKE ? OR core_question LIKE ? OR phenomenon LIKE ? ORDER BY updated_at DESC LIMIT 12", (like, like, like)).fetchall():
        out.append({"type": "研究", "id": r["id"], "title": r["title"], "sub": f"{r['status']} · 寫作價值 {r['writing_score']}"})
    for r in c.execute("SELECT id,content,research_id FROM arguments WHERE content LIKE ? OR explanation LIKE ? ORDER BY id DESC LIMIT 12", (like, like)).fetchall():
        out.append({"type": "論點", "id": r["id"], "rid": r["research_id"], "title": (r["content"] or "")[:60], "sub": f"研究 #{r['research_id']}"})
    for r in c.execute("SELECT id,title,status,score,research_id FROM topics WHERE title LIKE ? OR core_question LIKE ? ORDER BY id DESC LIMIT 12", (like, like)).fetchall():
        out.append({"type": "選題", "id": r["id"], "rid": r["research_id"], "title": r["title"], "sub": f"{r['status']} · 寫作價值 {r['score']}"})
    for r in c.execute("SELECT id,name,type,description FROM entities WHERE name LIKE ? OR description LIKE ? ORDER BY updated_at DESC LIMIT 8", (like, like)).fetchall():
        out.append({"type": "實體", "id": r["id"], "eid": r["id"], "title": r["name"], "sub": f"{r['type']} · {(r['description'] or '')[:30]}"})
    c.close()
    return {"results": out}

def api_arguments_list(params):
    q = params.get("q", [""])[0].strip()
    strength = params.get("strength", [""])[0].strip()
    credibility = params.get("credibility", [""])[0].strip()
    c = db()
    sql = "SELECT * FROM arguments WHERE 1=1"
    args = []
    if q: sql += " AND (content LIKE ? OR explanation LIKE ?)"; args += [f"%{q}%", f"%{q}%"]
    if strength: sql += " AND strength=?"; args.append(strength)
    if credibility: sql += " AND credibility=?"; args.append(credibility)
    sql += " ORDER BY id DESC"
    rows = [dict(r) for r in c.execute(sql, args).fetchall()]
    out = []
    for a in rows:
        r = c.execute("SELECT title FROM researches WHERE id=?", (a["research_id"],)).fetchone()
        news_cnt = c.execute("SELECT COUNT(*) FROM research_news WHERE research_id=?", (a["research_id"],)).fetchone()[0]
        topic_cnt = c.execute("SELECT COUNT(*) FROM topic_arguments WHERE argument_id=?", (a["id"],)).fetchone()[0]
        out.append({**a, "research_title": (r["title"] if r else ""), "news_count": news_cnt, "topic_count": topic_cnt})
    c.close()
    return {"arguments": out}

def api_topics_list(params):
    q = params.get("q", [""])[0].strip()
    status = params.get("status", [""])[0].strip()
    c = db()
    sql = "SELECT * FROM topics WHERE 1=1"
    args = []
    if q: sql += " AND (title LIKE ? OR core_question LIKE ?)"; args += [f"%{q}%", f"%{q}%"]
    if status: sql += " AND status=?"; args.append(status)
    sql += " ORDER BY id DESC"
    rows = [dict(r) for r in c.execute(sql, args).fetchall()]
    out = []
    for t in rows:
        r = c.execute("SELECT title FROM researches WHERE id=?", (t["research_id"],)).fetchone()
        arg_cnt = c.execute("SELECT COUNT(*) FROM topic_arguments WHERE topic_id=?", (t["id"],)).fetchone()[0]
        out.append({**t, "research_title": (r["title"] if r else ""), "argument_count": arg_cnt})
    c.close()
    return {"topics": out}

# ----------------------------------------------------------------------------
# 事件研究系統：時間線 / 來源 / 事實 / 衝突 / 實體 / 相關事件 / 追蹤 / 更新
# ----------------------------------------------------------------------------
def _linked_news(c, rid):
    news = [dict(x) for x in c.execute("SELECT n.* FROM news n JOIN research_news rn ON n.id=rn.news_id WHERE rn.research_id=? ORDER BY n.published ASC", (rid,)).fetchall()]
    if not news:
        r = c.execute("SELECT cluster_id FROM researches WHERE id=?", (rid,)).fetchone()
        if r and r["cluster_id"]:
            news = [dict(x) for x in c.execute("SELECT * FROM news WHERE cluster_id=? ORDER BY published ASC", (r["cluster_id"],)).fetchall()]
    return news

def api_research_timeline(params):
    try: rid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db(); news = _linked_news(c, rid); out = []
    for n in news:
        try: kws = json.loads(n.get("keywords") or "[]")
        except Exception: kws = []
        out.append({"published_iso": n["published_iso"], "title": n["title"], "source": n["source"],
                    "link": n["link"], "progress": summarize(n.get("summary") or n.get("title") or "", kws, 90)})
    c.close()
    return {"timeline": out}

def api_research_sources(params):
    try: rid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db(); news = _linked_news(c, rid)
    agg = collections.defaultdict(lambda: {"count": 0, "earliest": "", "latest": ""})
    for n in news:
        a = agg[n["source"]]; a["count"] += 1
        if not a["earliest"] or (n["published_iso"] or "") < a["earliest"]: a["earliest"] = n["published_iso"] or ""
        if (n["published_iso"] or "") > a["latest"]: a["latest"] = n["published_iso"] or ""
    sources = [{"source": s, **v} for s, v in sorted(agg.items(), key=lambda kv: -kv[1]["count"])]
    c.close()
    return {"sources": sources}

def api_research_facts(params):
    try: rid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db()
    facts = [dict(r) for r in c.execute("SELECT * FROM facts WHERE research_id=? ORDER BY confirm_count DESC, id", (rid,)).fetchall()]
    c.close()
    return {"facts": facts}

def api_research_conflicts(params):
    try: rid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db()
    conflicts = [dict(r) for r in c.execute("SELECT * FROM conflicts WHERE research_id=? ORDER BY id", (rid,)).fetchall()]
    c.close()
    return {"conflicts": conflicts}

def api_research_entities(params):
    try: rid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db()
    rows = [dict(r) for r in c.execute("SELECT e.* FROM entities e JOIN research_entities re ON e.id=re.entity_id WHERE re.research_id=?", (rid,)).fetchall()]
    c.close()
    grouped = {}
    for e in rows: grouped.setdefault(e["type"], []).append(e)
    return {"entities": grouped}

def api_research_related(params):
    try: rid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db()
    auto = find_related_researches(rid)
    manual = []
    for r in c.execute("SELECT related_id FROM research_links WHERE research_id=?", (rid,)).fetchall():
        rr = c.execute("SELECT id,title FROM researches WHERE id=?", (r[0],)).fetchone()
        if rr: manual.append({"id": rr[0], "title": rr[1], "score": 100, "shared": [], "manual": True})
    c.close()
    return {"related": auto + manual}

def api_research_updates(params):
    try: rid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db()
    ups = [dict(r) for r in c.execute("SELECT * FROM research_updates WHERE research_id=? ORDER BY id", (rid,)).fetchall()]
    for u in ups:
        try: u["new_facts"] = json.loads(u.get("new_facts") or "[]")
        except Exception: u["new_facts"] = []
        try: u["new_conflicts"] = json.loads(u.get("new_conflicts") or "[]")
        except Exception: u["new_conflicts"] = []
    c.close()
    return {"updates": ups}

def api_entities(params):
    q = params.get("q", [""])[0].strip()
    etype = params.get("type", [""])[0].strip()
    c = db()
    sql = "SELECT * FROM entities WHERE 1=1"; args = []
    if q: sql += " AND (name LIKE ? OR description LIKE ?)"; args += [f"%{q}%", f"%{q}%"]
    if etype: sql += " AND type=?"; args.append(etype)
    sql += " ORDER BY updated_at DESC LIMIT 60"
    rows = [dict(r) for r in c.execute(sql, args).fetchall()]
    c.close()
    return {"entities": rows}

def api_entities_detail(params):
    try: eid = int(params.get("id", [None])[0])
    except Exception: return {"error": "need id"}
    c = db()
    e = c.execute("SELECT * FROM entities WHERE id=?", (eid,)).fetchone()
    if not e: c.close(); return {"error": "not found"}
    e = dict(e)
    events = []
    for r in c.execute("SELECT r.id,r.title,r.status FROM researches r JOIN research_entities re ON r.id=re.research_id WHERE re.entity_id=?", (eid,)).fetchall():
        events.append({"id": r[0], "title": r[1], "status": r[2]})
    news = []
    for r in c.execute("SELECT n.id,n.title,n.source,n.published_iso,n.link FROM news n JOIN news_entities ne ON n.id=ne.news_id WHERE ne.entity_id=? ORDER BY n.published DESC LIMIT 20", (eid,)).fetchall():
        news.append({"id": r[0], "title": r[1], "source": r[2], "published_iso": r[3], "link": r[4]})
    c.close()
    return {"entity": e, "events": events, "news": news}

def api_research_tracking(body):
    rid = body.get("id"); on = 1 if body.get("on") else 0
    if not rid: return {"error": "need id"}
    c = db()
    r = c.execute("SELECT id FROM researches WHERE id=?", (rid,)).fetchone()
    if not r: c.close(); return {"error": "not found"}
    ncnt = c.execute("SELECT COUNT(*) FROM research_news WHERE research_id=?", (rid,)).fetchall()
    ncnt = ncnt[0][0] if ncnt else 0
    c.execute("UPDATE researches SET tracking=?,last_checked_at=?,last_news_count=? WHERE id=?", (on, iso(now_unix()), ncnt, rid))
    c.commit(); c.close()
    return {"ok": True, "tracking": on}

def api_research_update_summary(body):
    rid = body.get("id")
    if not rid: return {"error": "need id"}
    c = db()
    r = c.execute("SELECT * FROM researches WHERE id=?", (rid,)).fetchone()
    if not r: c.close(); return {"error": "not found"}
    r = dict(r)
    news = _linked_news(c, rid)
    last_up = c.execute("SELECT MAX(created_at) FROM research_updates WHERE research_id=?", (rid,)).fetchone()[0]
    new_news = [n for n in news if (n.get("published_iso") or "") > (last_up or "")] if last_up else news
    new_news = new_news or news
    nf = extract_facts(new_news)[:6]
    nc = detect_conflicts(new_news)[:4]
    if LLM_API_KEY:
        try:
            blob = _news_blob_for_ai(new_news, limit=10, per=260, cap=3200)
            sys_p = ("你是事件追蹤編輯。基於「舊研究摘要」與「新增新聞」，輸出 JSON："
                     "{\"summary\":\"最新事件進展（客觀、基於新聞）\",\"new_facts\":[\"新事實1\"],\"new_conflicts\":[\"新衝突1\"]}。"
                     "只輸出 JSON，不要解釋；所有事實必須來自新聞，不可編造；無則填空陣列。")
            user_p = (f"舊研究摘要：\n事件：{r.get('event','')}\n現象：{r.get('phenomenon','')}\n為什麼關注：{r.get('why_matters','')}\n\n"
                      f"新增新聞（{len(new_news)} 則）：\n{blob}\n\n請輸出 JSON。")
            out = call_llm(sys_p, user_p, max_tokens=1400, json_mode=True)
            if out:
                m = re.search(r"\{.*\}", out, re.S)
                data = json.loads(m.group(0)) if m else None
                if data and isinstance(data, dict):
                    summary = (data.get("summary") or "").strip()
                    llm_nf = data.get("new_facts") or []
                    llm_nc = data.get("new_conflicts") or []
                    if summary:
                        c.execute("INSERT INTO research_updates(research_id,summary,new_facts,new_conflicts,created_at) VALUES(?,?,?,?,?)",
                                  (rid, summary, json.dumps(llm_nf, ensure_ascii=False), json.dumps(llm_nc, ensure_ascii=False), iso(now_unix())))
                        c.execute("UPDATE researches SET updated_at=? WHERE id=?", (iso(now_unix()), rid))
                        c.commit(); c.close()
                        return {"ok": True, "from_llm": True, "summary": summary, "new_facts": llm_nf, "new_conflicts": llm_nc}
        except Exception as e:
            print(f"[update summary llm fail] {e}")
    summary = f"截至 {iso(now_unix())}，本事件共關聯 {len(news)} 則新聞。"
    if last_up: summary += f" 其中 {len(new_news)} 則為最近一次更新後新增。"
    nf_text = json.dumps([f["content"] for f in nf], ensure_ascii=False)
    nc_text = json.dumps([f"{x['claim_a']}（{x['source_a']}） vs {x['claim_b']}（{x['source_b']}）" for x in nc], ensure_ascii=False)
    c.execute("INSERT INTO research_updates(research_id,summary,new_facts,new_conflicts,created_at) VALUES(?,?,?,?,?)",
              (rid, summary, nf_text, nc_text, iso(now_unix())))
    c.execute("UPDATE researches SET updated_at=? WHERE id=?", (iso(now_unix()), rid))
    c.commit(); c.close()
    return {"ok": True, "from_llm": False, "summary": summary,
            "new_facts": [f["content"] for f in nf], "new_conflicts": [f"{x['claim_a']} vs {x['claim_b']}" for x in nc]}

def api_research_link_news(body):
    rid = body.get("id") or body.get("research_id"); nid = body.get("news_id")
    if not rid or not nid: return {"error": "need id & news_id"}
    c = db()
    cur = c.execute("INSERT OR IGNORE INTO research_news(research_id,news_id) VALUES(?,?)", (rid, nid))
    ok = cur.rowcount > 0
    news = [dict(x) for x in c.execute("SELECT * FROM news WHERE id IN (SELECT news_id FROM research_news WHERE research_id=?)", (rid,)).fetchall()]
    try: rebuild_research_derived(c, rid, news)
    except Exception as e: print(f"[link news derive err] {e}")
    c.commit(); c.close()
    return {"ok": True, "added": ok}

def api_research_link_related(body):
    rid = body.get("id") or body.get("research_id"); rel = body.get("related_id")
    if not rid or not rel: return {"error": "need id & related_id"}
    c = db()
    c.execute("INSERT OR IGNORE INTO research_links(research_id,related_id) VALUES(?,?)", (rid, rel))
    c.commit(); c.close()
    return {"ok": True}

# ----------------------------------------------------------------------------
# HTTP 服務
# ----------------------------------------------------------------------------
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def _send(self, code, data, ctype="application/json; charset=utf-8"):
        body = data if isinstance(data, bytes) else json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        u = urlparse(self.path)
        path = u.path
        params = parse_qs(u.query)
        if path == "/" or path == "/index.html":
            try:
                with open(os.path.join(BASE, "index.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except Exception:
                self._send(404, {"error": "index.html not found"})
            return
        try:
            if path == "/api/status": return self._send(200, api_status())
            if path == "/api/collector/status": return self._send(200, api_collector_status())
            if path == "/api/llm/status": return self._send(200, api_llm_status())
            if path == "/api/categories": return self._send(200, api_categories())
            if path == "/api/sources": return self._send(200, api_sources())
            if path == "/api/news": return self._send(200, api_news(params))
            if path == "/api/events": return self._send(200, api_events(params))
            if path == "/api/hotspots": return self._send(200, api_hotspots())
            if path == "/api/briefing": return self._send(200, api_briefing())
            if path == "/api/research": return self._send(200, api_research(params))
            if path == "/api/stats": return self._send(200, api_stats())
            if path == "/api/search/history": return self._send(200, api_search_history())
            if path == "/api/research/board": return self._send(200, api_research_board())
            if path == "/api/research/detail": return self._send(200, api_research_detail(params))
            if path == "/api/research/timeline": return self._send(200, api_research_timeline(params))
            if path == "/api/research/sources": return self._send(200, api_research_sources(params))
            if path == "/api/research/facts": return self._send(200, api_research_facts(params))
            if path == "/api/research/conflicts": return self._send(200, api_research_conflicts(params))
            if path == "/api/research/entities": return self._send(200, api_research_entities(params))
            if path == "/api/research/related": return self._send(200, api_research_related(params))
            if path == "/api/research/updates": return self._send(200, api_research_updates(params))
            if path == "/api/entities": return self._send(200, api_entities(params))
            if path == "/api/entities/detail": return self._send(200, api_entities_detail(params))
            if path == "/api/arguments": return self._send(200, api_arguments_list(params))
            if path == "/api/topics": return self._send(200, api_topics_list(params))
            if path == "/api/search": return self._send(200, api_search(params))
        except Exception as e:
            self._send(500, {"error": str(e)})
        self._send(404, {"error": "not found"})
    def do_POST(self):
        u = urlparse(self.path); path = u.path
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception:
            body = {}
        if path == "/api/news/action":
            nid = body.get("id"); act = body.get("action"); val = body.get("value", "")
            c = db()
            if act == "read": c.execute("UPDATE news SET read=? WHERE id=?", (1 if body.get("on", True) else 0, nid))
            elif act == "star": c.execute("UPDATE news SET starred=? WHERE id=?", (1 if body.get("on", True) else 0, nid))
            elif act == "later": c.execute("UPDATE news SET later=? WHERE id=?", (1 if body.get("on", True) else 0, nid))
            elif act == "note": c.execute("UPDATE news SET notes=? WHERE id=?", (val, nid))
            elif act == "tag": c.execute("UPDATE news SET tags=? WHERE id=?", (json.dumps(val if isinstance(val, list) else [val], ensure_ascii=False), nid))
            else: return self._send(400, {"error": "bad action"})
            c.commit(); c.close()
            return self._send(200, {"ok": True})
        if path == "/api/feeds":
            try:
                with open(FEEDS, encoding="utf-8") as f:
                    cfg = json.load(f)
                cfg.setdefault("sources", []).append({
                    "id": "custom_" + str(now_unix()), "name": body.get("name", "自訂來源"),
                    "category": body.get("category", "自訂分類"), "type": "rss",
                    "url": body.get("url", ""), "lang": body.get("lang", "zh"), "weight": 1.0})
                with open(FEEDS, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, ensure_ascii=False, indent=2)
                return self._send(200, {"ok": True})
            except Exception as e:
                return self._send(500, {"error": str(e)})
        if path == "/api/refresh":
            try:
                run_cycle()
                return self._send(200, api_collector_status())
            except Exception as e:
                return self._send(500, {"error": str(e)})
        if path == "/api/research/create": return self._send(200, api_research_create(body))
        if path == "/api/research/update": return self._send(200, api_research_update(body))
        if path == "/api/research/ai": return self._send(200, api_research_ai(body))
        if path == "/api/research/related-evidence": return self._send(200, api_research_add_related_evidence(body))
        if path == "/api/research/status": return self._send(200, api_research_status(body))
        if path == "/api/research/tracking": return self._send(200, api_research_tracking(body))
        if path == "/api/research/update-summary": return self._send(200, api_research_update_summary(body))
        if path == "/api/research/link-news": return self._send(200, api_research_link_news(body))
        if path == "/api/research/link-related": return self._send(200, api_research_link_related(body))
        if path == "/api/arguments/create": return self._send(200, api_argument_create(body))
        if path == "/api/arguments/update": return self._send(200, api_argument_update(body))
        if path == "/api/arguments/delete": return self._send(200, api_argument_delete(body))
        if path == "/api/evidence/create": return self._send(200, api_evidence_create(body))
        if path == "/api/topics/create": return self._send(200, api_topic_create(body))
        if path == "/api/topics/update": return self._send(200, api_topic_update(body))
        self._send(404, {"error": "not found"})

def get_fetch_interval():
    """採集間隔（分鐘）。優先環境變數，其次 feeds.json，異常時回退 15 分鐘。"""
    try:
        iv = int(os.environ.get("FETCH_INTERVAL_MINUTES", "0")) or \
             json.load(open(FEEDS, encoding="utf-8")).get("fetch_interval_minutes", 15)
        if iv <= 0:
            iv = 15
    except Exception:
        iv = 15
    return iv

def run_cycle():
    """統一採集入口：加鎖執行，避免與 scheduler / refresh 併發寫入。"""
    with WRITE_LOCK:
        process_cycle()

def scheduler():
    while True:
        try:
            run_cycle()
        except Exception as e:
            print(f"[scheduler error] {e}")
            collector_state["last_error"] = str(e)[:200]
        iv = get_fetch_interval()
        time.sleep(iv * 60)

def main():
    init_db()
    # 背景立即執行首次採集（不阻塞服務啟動）
    threading.Thread(target=lambda: (time.sleep(1), run_cycle()), daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), H)
    print(f"新聞情報工作台已啟動: http://localhost:{PORT}")
    srv.serve_forever()

if __name__ == "__main__":
    main()
