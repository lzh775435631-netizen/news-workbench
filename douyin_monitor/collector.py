"""Broad, rotating keyword coverage for public-index discovery.
Not a direct Douyin API. A broad vocabulary does not imply complete coverage.
"""
import calendar, hashlib, html, itertools, os, re, time
from urllib.parse import quote
import requests, feedparser
from .app import ingest

# Full vocabulary: categories, discovery intents, freshness, community language.
CATEGORIES=[
"漫剧","短剧","网剧","AI短剧","AI漫剧","动态漫","微短剧","AI动态漫",
"AI动画","AI动漫","动漫短剧","真人短剧","竖屏短剧","短剧全集",
"自制短剧","原创短剧","个人自制AI短剧","自制AI漫剧","短视频连续剧",
"沙雕动画","修仙漫剧","玄幻漫剧","都市短剧","古装短剧","穿越短剧",
"重生短剧","逆袭短剧","甜宠短剧","悬疑短剧","爽剧","男频短剧","女频短剧",
"红果短剧","抖音短剧","抖音漫剧",
"短剧解说","漫剧解说","AI生成短剧","AI自制短剧","国漫短剧","漫画短剧","小说改编短剧","小说改编漫剧","AI真人短剧","短剧系列","漫剧系列","动画连续剧","原创漫剧","自制漫剧","个人创作漫剧","AI动画短剧","AI微短剧","真人漫改","二维漫剧","三维漫剧","竖屏漫剧","国产短剧","国产漫剧","古风漫剧","科幻短剧","悬疑漫剧","校园短剧","恋爱漫剧","都市漫剧","系统流漫剧","末世漫剧"]
DISCOVERY=[
"观看教程","在哪看","怎么看","后续","全集","大结局","完整版",
"后续在哪看","求剧名","剧名是什么","这是什么剧","求后续",
"求全集","求资源","全集在哪看","未删减","无删减","一口气看完",
"更新了","已更新","更新至","追更","连载","最新一集","第二集",
"第三集","下一集","后续剧情","后面还有吗","看后续","结局",
"评论区求剧名","求链接","入口","看全集","全集已出","全集上线",
"完整版在哪看","后续怎么观看","哪里能看","哪里看全集","全集入口","后续更新了吗","求更新","坐等更新","蹲后续","蹲全集","求第二集","求下一集","后续来了","后续来了踢我","这剧叫啥","谁知道剧名","有人知道吗","分享链接","观看方法","播放地址","更新地址","完整版教程","点主页看后续","合集","剧集合集","追剧","求片名","合集在哪看","有没有全集","全集放出来了吗","看完整版","追剧入口","在哪追","追剧指南","哪里可以追","后续怎么找","哪里看后续","找不到后续","求名字","谁有后续","这部叫什么","全集多少集","后续在哪","后续求踢","更新提醒"]
FRESHNESS=[
"第一集","第1集","首播","首发","新剧上线","刚上线","新出","新作",
"开播","今天更新","昨日更新","刚更新","上线了","最新上线",
"爆火","热播","爆款","冲上热门","播放破万","热门推荐",
"新系列","新番","第一话","第1话","更新第一集","刚出两集",
"刚出三集","前两集","新上线","今日首播","今晚首播","新剧开更","新漫剧","新短剧","新剧推荐","近期新剧","最近新剧","刚发布","刚发","刚更新第一集","首更","开更","连更","刚火","突然火了","热度上涨","新剧爆了","最新短剧","最新漫剧","2026新剧","2026漫剧","更新第二集","更新第三集","今日上线","新剧首发","最新一话","第一季","第二季","更新预告","预告片","新剧预告","即将开播","刚开播","今天首发","刚出第一集","刚出第二集","播放暴涨","讨论热度","刚刚更新"]
BASE_QUERIES=[
"漫剧 观看教程","短剧 观看教程","AI短剧 第一集","AI漫剧 第一集",
"动态漫 新剧上线","自制AI短剧 首播","个人创作者 AI短剧",
"微短剧 求剧名","网剧 后续","短剧 在哪看","漫剧 全集","短剧 求后续"]
SITES=["site:douyin.com/video","site:douyin.com","site:iesdouyin.com"]
# Rotate combinations across runs to keep hourly requests bounded.
MAX_QUERIES=max(5,min(60,int(os.getenv("DOUYIN_SEARCH_QUERIES_PER_RUN","24"))))
def all_queries():
    queries=[f"{site} {q}" for site in SITES for q in BASE_QUERIES]
    queries += [f"{site} {cat} {intent}" for site in SITES for cat in CATEGORIES for intent in DISCOVERY+FRESHNESS]
    return list(dict.fromkeys(queries))
def selected_queries(now=None):
    allq=all_queries()
    # Stable rotating schedule: each hour processes a different portion.
    slot=int((now or time.time())//3600)
    offset=(slot*MAX_QUERIES)%len(allq)
    return [allq[(offset+i)%len(allq)] for i in range(min(MAX_QUERIES,len(allq)))]

def collect():
    items=[];errors=[];seen=set();now=int(time.time())
    queries=selected_queries(now)
    for q in queries:
        try:
            rss="https://news.google.com/rss/search?q="+quote(q)+"&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            res=requests.get(rss,timeout=12,headers={"User-Agent":"DouyinDramaResearch/1.0"})
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
                items.append({"id":vid,"title":title,"url":link,"published":published,
                              "source":"Google News RSS indexed discovery; not direct Douyin",
                              "likes":0,"comments":0,"shares":0})
        except Exception as ex:errors.append({"query":q,"error":str(ex)[:120]})
    return {"found":len(items),"ingested":ingest(items),"errors":errors[:8],
            "query_count":len(queries),"vocabulary":{"categories":len(CATEGORIES),
            "discovery":len(DISCOVERY),"freshness":len(FRESHNESS)},
            "total_query_combinations":len(all_queries()),
            "source":"search-engine-index","direct_douyin":False,
            "warning":"Search index timestamp is not Douyin upload time; engagement unavailable"}
