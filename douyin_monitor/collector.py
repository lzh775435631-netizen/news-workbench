"""Best-effort public-index discovery, NOT a Douyin first-party search API.
Search engine RSS can miss new works, so never represent coverage as complete.
"""
import calendar, hashlib, html, re, time
from urllib.parse import quote, urlparse
import requests
import feedparser
from .app import ingest

QUERIES=["site:douyin.com/video 漫剧 第一集","site:douyin.com/video AI短剧 更新","site:douyin.com/video 短剧 观看教程","site:douyin.com/video 漫剧 后续","site:douyin.com/video 动态漫 首播","site:douyin.com/video 微短剧 新剧上线","site:douyin.com/video 求剧名 后续","site:douyin.com/video AI漫剧 第一集","site:douyin.com/video 在哪看 全集","site:douyin.com/video 自制短剧 第一集"]
def collect():
    items=[]; errors=[]; seen=set(); now=int(time.time())
    for q in QUERIES:
        try:
            rss="https://news.google.com/rss/search?q="+quote(q)+"&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            res=requests.get(rss,timeout=15,headers={"User-Agent":"Mozilla/5.0"})
            res.raise_for_status()
            feed=feedparser.parse(res.content)
            for e in feed.entries:
                title=html.unescape(re.sub("<[^>]+>","",e.get("title",""))).strip()
                link=e.get("link","")
                if not title or not link or link in seen:continue
                stamp=e.get("published_parsed") or e.get("updated_parsed")
                if not stamp:continue
                published=calendar.timegm(stamp)
                if published<now-72*3600 or published>now+3600:continue
                seen.add(link)
                vid=hashlib.sha256(link.encode()).hexdigest()[:24]
                items.append({"id":vid,"title":title,"url":link,"published":published,"source":"Google News RSS indexed discovery; not direct Douyin","likes":0,"comments":0,"shares":0})
        except Exception as ex: errors.append(str(ex)[:180])
    return {"found":len(items),"ingested":ingest(items),"errors":errors,"source":"search-engine-index","direct_douyin":False,"warning":"RSS timestamps describe index articles, not verified Douyin upload dates; no video engagement data"}
