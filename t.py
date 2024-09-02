from datetime import datetime

timestamp1 = 1498261945
timestamp2 = 1498307928
print(datetime.fromtimestamp(timestamp1).strftime('%Y-%m-%d %H:%M:%S'))
print(datetime.fromtimestamp(timestamp2).strftime('%Y-%m-%d %H:%M:%S'))