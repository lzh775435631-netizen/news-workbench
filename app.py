#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最終版 · 新聞情報工作台 後端
動態消息(RSS/API) -> 自動擷取 -> 資料庫(SQLite) -> AI處理 -> 新聞工作台
全程使用真實公開新聞來源（Google News 主題/搜尋 RSS + 50+ 國際媒體 RSS）。
"""
import os, json, time, sqlite3, threading, re, math, html, collections, datetime
import concurrent.futures
from urllib.parse import urlparse, parse_qs, quote, urlencode
import feedparser, requests
import jieba
from jieba import analyse

jieba.setLogLevel(20)

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "news.db")
FEEDS = os.path.join(BASE, "feeds.json")
PORT = int(os.environ.get("PORT", "8800"))

# 可選 LLM（OpenAI 相容）— 設定環境變數即啟用，未設定則使用啟發式 AI
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_BASE = os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")

WRITE_LOCK = threading.Lock()
last_sync = 0
last_cycle_new = 0
last_cycle_breaking = []
prev_cluster_hot = {}
prev_avg_hot = 0

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
    c = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS news (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    CREATE TABLE IF NOT EXISTS searches (id INTEGER PRIMARY KEY AUTOINCREMENT, q TEXT, ts INTEGER);
    """)
    c.commit(); c.close()

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
    try:
        d = feedparser.parse(source_url(src))
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
                pub = now_unix()
            out.append({
                "title": title, "summary": summary, "link": link,
                "source": src_name, "source_id": src["id"], "category": src["category"],
                "lang": src.get("lang", "en"), "published": pub,
                "weight": float(src.get("weight", 1.0)),
            })
        return out
    except Exception as ex:
        print(f"[fetch error] {src['id']}: {ex}")
        return []

def fetch_all(sources):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for r in ex.map(fetch_source, sources):
            results.extend(r)
    return results

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
    t0 = time.time()
    with open(FEEDS, encoding="utf-8") as f:
        cfg = json.load(f)
    sources = cfg["sources"]
    raw = fetch_all(sources)
    inserted = 0
    conn = db()
    for it in raw:
        norm = norm_title(it["title"])
        if not norm:
            continue
        kw = extract_keywords(it["title"], it["summary"])
        summ = summarize(it["summary"] or it["title"], kw)
        is_brk = breaking_of(it["title"], it["summary"], it["published"])
        try:
            conn.execute(
                """INSERT OR IGNORE INTO news
                   (title,norm_title,summary,content,link,source,source_id,category,lang,
                    published,published_iso,fetched,keywords,hotness,importance,is_breaking,cluster_id)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (it["title"], norm, summ, it["summary"], it["link"], it["source"], it["source_id"],
                 it["category"], it["lang"], it["published"], iso(it["published"]), now_unix(),
                 json.dumps(kw, ensure_ascii=False), 1, "低", is_brk, 0))
            inserted += conn.total_changes and 0  # placeholder
        except Exception:
            pass
    # 精確計算新增數
    cur = conn.execute("SELECT COUNT(*) FROM news WHERE fetched > ?", (int(last_sync),)).fetchone()[0]
    last_cycle_new = cur
    conn.commit()

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
    conn.close()
    print(f"[cycle] +{last_cycle_new} new, clusters={len(groups)}, breaking={len(last_cycle_breaking)}, {time.time()-t0:.1f}s")

# ----------------------------------------------------------------------------
# 可選 LLM
# ----------------------------------------------------------------------------
def call_llm(system, prompt, max_tokens=600):
    if not LLM_API_KEY:
        return None
    try:
        r = requests.post(f"{LLM_BASE}/chat/completions",
                          headers={"Authorization": f"Bearer {LLM_API_KEY}", "Content-Type": "application/json"},
                          json={"model": LLM_MODEL, "messages": [{"role": "system", "content": system}, {"role": "user", "content": prompt}],
                                "temperature": 0.4, "max_tokens": max_tokens}, timeout=40)
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[llm error] {e}")
        return None

# ----------------------------------------------------------------------------
# API 處理
# ----------------------------------------------------------------------------
def api_news(params):
    cat = params.get("category", ["全部"])[0]
    q = params.get("q", [""])[0].strip()
    source = params.get("source", [""])[0].strip()
    frm = params.get("from", [""])[0].strip()
    to = params.get("to", [""])[0].strip()
    minhot = params.get("minHot", ["0"])[0]
    sort = params.get("sort", ["hot"])[0]
    starred = params.get("starred", ["0"])[0]
    read = params.get("read", [""])[0]
    later = params.get("later", ["0"])[0]
    limit = int(params.get("limit", ["40"])[0])
    offset = int(params.get("offset", ["0"])[0])
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
    sql += " LIMIT ? OFFSET ?"; args += [limit, offset]
    c = db(); rows = c.execute(sql, args).fetchall(); c.close()
    return {"items": [dict(r) for r in rows], "count": len(rows)}

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

def api_search_history():
    c = db()
    rows = c.execute("SELECT q, MAX(ts) FROM searches GROUP BY q ORDER BY MAX(ts) DESC LIMIT 10").fetchall()
    c.close()
    return {"history": [r[0] for r in rows]}

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
            if path == "/api/categories": return self._send(200, api_categories())
            if path == "/api/sources": return self._send(200, api_sources())
            if path == "/api/news": return self._send(200, api_news(params))
            if path == "/api/hotspots": return self._send(200, api_hotspots())
            if path == "/api/briefing": return self._send(200, api_briefing())
            if path == "/api/research": return self._send(200, api_research(params))
            if path == "/api/stats": return self._send(200, api_stats())
            if path == "/api/search/history": return self._send(200, api_search_history())
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
                with WRITE_LOCK:
                    process_cycle()
                return self._send(200, api_status())
            except Exception as e:
                return self._send(500, {"error": str(e)})
        self._send(404, {"error": "not found"})

def scheduler():
    while True:
        try:
            with WRITE_LOCK:
                process_cycle()
        except Exception as e:
            print(f"[scheduler error] {e}")
        try:
            iv = int(os.environ.get("FETCH_INTERVAL_MINUTES", "0")) or \
                 json.load(open(FEEDS, encoding="utf-8")).get("fetch_interval_minutes", 15)
        except Exception:
            iv = 15
        time.sleep(iv * 60)

def main():
    init_db()
    # 背景立即執行首次採集（不阻塞服務啟動）
    threading.Thread(target=lambda: (time.sleep(1), process_cycle()), daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), H)
    print(f"新聞情報工作台已啟動: http://localhost:{PORT}")
    srv.serve_forever()

if __name__ == "__main__":
    main()
