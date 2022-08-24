# import speedtest
# st = speedtest.Speedtest()
# st.get_best_server()
# print(f"Your ping is: {st.results.ping} ms")
# print(f"Your download speed: {round(st.download() / 1000000, 1)} Mbit/s")
# print(f"Your upload speed: {round(st.upload() / 1000 / 1000, 1)} Mbit/s")

import speedtest
import psutil
mem = psutil.virtual_memory()
# 系统总计内存(单位字节)
zj = float(mem.total) / 1000000000
ysy = float(mem.used) / 1000000000
print((0.8 * zj - ysy) / 0.16 * 24)
st = speedtest.Speedtest()
st.get_best_server()
ns = st.download() / 1000000
print(ns / 11 * 24)
