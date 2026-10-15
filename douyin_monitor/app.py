"""Douyin short-drama hotspot monitor: ingest verified public records, rank recent works.
No fabricated data or unauthorized scraping. Python standard library only.
Run: python -m douyin_monitor.app
"""
import csv, io, json, os, re, sqlite3, time, threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DB = os.getenv("DOUYIN_DB_PATH", "/tmp/douyin_hotspots.sqlite3")
TOKEN = os.getenv("INGEST_TOKEN", "")
KEYWORDS = ["漫剧","短剧","网剧","观看教程","在哪看","后续","AI漫剧","动态漫","微短剧","怎么看","求剧名","全集","大结局","新剧上线","首播","第一集","更新了"]

def connect():
    c=sqlite3.connect(DB)
    c.row_factory=sqlite3.Row
    c.execute("""CREATE TABLE IF NOT EXISTS videos (
      id TEXT PRIMARY KEY, title TEXT NOT NULL, author TEXT DEFAULT '',
      url TEXT NOT NULL, published INTEGER NOT NULL,
      likes INTEGER DEFAULT 0, comments INTEGER DEFAULT 0,
      shares INTEGER DEFAULT 0, first_seen INTEGER NOT NULL,
      last_seen INTEGER NOT NULL, source TEXT NOT NULL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS snapshots (
      video_id TEXT, sampled INTEGER, likes INTEGER, comments INTEGER,
      shares INTEGER, PRIMARY KEY(video_id,sampled))""")
    c.commit()
    return c

def ingest(items):
    now=int(time.time()); count=0
    with connect() as c:
        for x in items:
            try:
                vid=str(x["id"]).strip()
                title=str(x["title"]).strip()
                url=str(x["url"]).strip()
                published=int(x["published"])
                if not vid or not title or not url.startswith("https://") or published<=0 or published>now+3600: continue
                likes=max(0,int(x.get("likes",0)))
                comments=max(0,int(x.get("comments",0)))
                shares=max(0,int(x.get("shares",0)))
                author=str(x.get("author",""))[:160]
                source=str(x.get("source","external"))[:80]
                c.execute("""INSERT INTO videos(id,title,author,url,published,likes,comments,shares,first_seen,last_seen,source)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?)
                  ON CONFLICT(id) DO UPDATE SET title=excluded.title,author=excluded.author,
                  url=excluded.url,likes=excluded.likes,comments=excluded.comments,
                  shares=excluded.shares,last_seen=excluded.last_seen,source=excluded.source""",
                  (vid,title,author,url,published,likes,comments,shares,now,now,source))
                c.execute("INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?,?)",(vid,now,likes,comments,shares))
                count+=1
            except (KeyError,TypeError,ValueError): continue
    return count

def report(hours=72):
    now=int(time.time()); cutoff=now-hours*3600
    with connect() as c:
        rows=c.execute("SELECT * FROM videos WHERE published>=? ORDER BY published DESC",(cutoff,)).fetchall()
        output=[]
        for r in rows:
            title=r["title"]
            if not any(k.lower() in title.lower() for k in KEYWORDS): continue
            first=c.execute("SELECT * FROM snapshots WHERE video_id=? ORDER BY sampled ASC LIMIT 1",(r["id"],)).fetchone()
            growth=max(0,r["likes"]-(first["likes"] if first else r["likes"]))
            age=max(1,(now-r["published"])/3600)
            score=round((r["likes"]+r["comments"]*3+r["shares"]*5)**0.5/(age+2)**0.4+growth**0.5,2)
            output.append(dict(r)|{"like_growth":growth,"score":score,"sample_count":c.execute("SELECT COUNT(*) FROM snapshots WHERE video_id=?",(r["id"],)).fetchone()[0]})
    return sorted(output,key=lambda x:x["score"],reverse=True)

class Handler(BaseHTTPRequestHandler):
    def reply(self,status,data):
        payload=json.dumps(data,ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Content-Length",str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
    def do_GET(self):
        p=urlparse(self.path)
        if p.path=="/":
            page="""<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>短剧热点监测</title><style>body{font-family:system-ui;background:#101827;color:#eaf0ff;max-width:900px;margin:40px auto;padding:18px}button{background:#4476ef;color:white;border:0;padding:12px;border-radius:8px}article{background:#1c2a40;margin:12px 0;padding:16px;border-radius:12px}a{color:#91b7ff}small{color:#bac5d9}</style><h1>短剧 / 漫剧热点监测</h1><p>公开搜索引擎索引发现 · 非抖音官方实时数据。缺少播放量、点赞等数据时不虚构。</p><button onclick="load()">刷新数据</button><p id="status">正在读取...</p><main id="list"></main><script>async function load(){let s=document.getElementById("status"),m=document.getElementById("list");try{let d=await(await fetch("/api/hotspots?hours=72")).json();s.textContent="最近72小时 · "+d.items.length+" 条候选线索";m.replaceChildren();for(let x of d.items){let a=document.createElement("article"),h=document.createElement("h3"),link=document.createElement("a"),info=document.createElement("small");link.href=x.url;link.target="_blank";link.rel="noopener noreferrer";link.textContent=x.title;h.append(link);info.textContent="发布时间："+new Date(x.published*1000).toLocaleString()+" · 来源："+x.source;a.append(h,info);m.append(a)}}catch(e){s.textContent="数据加载失败："+e}}</script><script>load()</script>"""
            body=page.encode();self.send_response(200);self.send_header("Content-Type","text/html; charset=utf-8");self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body);return
        if p.path=="/api/collect":
            from .collector import collect
            return self.reply(200,collect())
        if p.path=="/health":
            return self.reply(200,{"service":"douyin-hotspot-monitor","status":"ok","data_source":"external ingest required"})
        if p.path=="/api/hotspots":
            try: hours=min(168,max(1,int(parse_qs(p.query).get("hours",["72"])[0])))
            except ValueError: hours=72
            return self.reply(200,{"hours":hours,"items":report(hours),"live_douyin_connected":False})
        return self.reply(404,{"error":"not found"})
    def do_POST(self):
        if urlparse(self.path).path!="/api/ingest":return self.reply(404,{"error":"not found"})
        if not TOKEN:return self.reply(503,{"error":"INGEST_TOKEN not configured"})
        if self.headers.get("Authorization")!="Bearer "+TOKEN:return self.reply(401,{"error":"unauthorized"})
        try:
            length=int(self.headers.get("Content-Length","0"))
            if length<1 or length>2_000_000:return self.reply(413,{"error":"invalid size"})
            obj=json.loads(self.rfile.read(length))
            if not isinstance(obj,list):return self.reply(400,{"error":"expected JSON array"})
            return self.reply(200,{"ingested":ingest(obj)})
        except (ValueError,TypeError,json.JSONDecodeError):return self.reply(400,{"error":"invalid JSON"})

if __name__=="__main__":
    port=int(os.getenv("PORT","8000"))
    def worker():
        while True:
            try:
                from .collector import collect
                print("collection:",collect(),flush=True)
            except Exception as e: print("collection error:",str(e),flush=True)
            time.sleep(3600)
    threading.Thread(target=worker,daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0",port),Handler).serve_forever()
