pool = []
dict1 = {
    "host": "news.baidu.com",
    "url": "http://news.baidu.com/12345.html",
    "status": "pending/waiting",
    "mode": "url/hub",
    "pendedtime": 0,
    "failure": 0
}
dict2 = {
    "host": "sport.baidu.com",
    "url": "http://news.baidu.com/54321.html",
    "status": "pending/waiting",
    "mode": "url/hub",
    "pendedtime": 0,
    "failure": 0
}
pool.append(dict1)
pool.append({
    "host": "sport.baidu.com",
    "url": "http://news.baidu.com/54321.html",
    "status": "pending/waiting",
    "mode": "url/hub",
    "pendedtime": 0,
    "failure": 0
})
print(pool)
url = "http://news.baidu.com/12345.html"
_status = [d for d in pool if d['url'] == url][0]
print(_status)
