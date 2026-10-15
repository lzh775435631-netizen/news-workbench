# 抖音短剧/漫剧热点监测（开发版）

当前模块是**真实数据接收与分析 API**，不是已接通的抖音爬虫。没有数据时返回空列表，不生成假热点。

## 本地启动
```bash
python -m douyin_monitor.app
```
服务默认端口 8000，可设置 PORT。GET /health 查看状态，GET /api/hotspots?hours=72 查看最近72小时数据。

## 导入公开数据
先设置环境变量 INGEST_TOKEN（不要提交到仓库）。调用 POST /api/ingest，Header 为 Authorization: Bearer <token>，请求体为 JSON 数组，例如：
```json
[{"id":"example-123","title":"某部新漫剧第一集","url":"https://www.douyin.com/video/example","published":1792000000,"likes":100,"comments":10,"shares":5,"author":"示例","source":"manual"}]
```
示例只是字段格式说明，不代表真实视频。published 是 Unix 秒时间戳。相同 ID 再导入时会保存互动数据快照用于增长分析。

## Render 部署注意
目前未修改原有 app.py、render.yaml 和 main 分支；本模块独立运行。若在 Render 免费 Web Service 上替换 start command 为 `python -m douyin_monitor.app`，将取代原新闻工作台。先验证新分支再切换。免费实例可能休眠，/tmp 数据可能丢失；正式部署应使用持久数据库。

## 待完成
- 接入经过验证、符合平台规则的抖音搜索数据源
- 增加作品级归并和来源校验
- 解决持久化与定时采集
- 实际部署并验证 /health 和 /api/hotspots
