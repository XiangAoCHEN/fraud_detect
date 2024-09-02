import pandas as pd
import numpy as np

# 读取CSV文件
file_path = '/home/cxa/fraud_detect/eth_dataset/d2_10node_10f/x_with_amount.csv'
df = pd.read_csv(file_path)

# 将所有的 -inf 替换为 -1
df.replace(-np.inf, -1, inplace=True)

# 将修改后的数据保存为一个新的CSV文件
new_file_path = '/home/cxa/fraud_detect/eth_dataset/d2_10node_10f/x_with_amount_modified.csv'
df.to_csv(new_file_path, index=False)

print(f"new file: {new_file_path}")
