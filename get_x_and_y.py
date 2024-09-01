import pandas as pd

# 读取原始CSV文件
input_file = 'eth_dataset/d2_10node_10f/filtered_nodes.csv'
df = pd.read_csv(input_file)

# 提取fraud_label列保存为y.csv
y = df[['fraud_label']]
y.to_csv('./eth_dataset/d2_10node_10f/y.csv', index=False)

# 提取除fraud_label列以外的其他列保存为x.csv
x = df.drop(columns=['fraud_label'])
x.to_csv('./eth_dataset/d2_10node_10f/x.csv', index=False)

print("ok")
