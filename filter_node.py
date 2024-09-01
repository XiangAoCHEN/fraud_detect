import dask.dataframe as dd
import numpy as np

# 读取csv文件
file_path = './eth_dataset/node_total.csv'  # 请将路径替换为您的文件路径
df = dd.read_csv(file_path)

# 需要保留的列
columns_to_keep = [
    'account_id', 'fraud_label', 'send_amount_max', 'send_amount_avg',
    'receive_amount_max', 'receive_amount_avg', 'in_degree', 'out_degree',
    'total_degree', 'earliest_transaction', 'latest_transaction', 'transaction_interval'
]

# 过滤出fraud_label为1的行
fraud_df = df[df['fraud_label'] == 1]

# 过滤出fraud_label为0的行
non_fraud_df = df[df['fraud_label'] == 0]

# 随机抽取10%的非欺诈节点
non_fraud_sample = non_fraud_df.sample(frac=0.1, random_state=42)

# 合并欺诈和非欺诈样本，并保留指定列
final_df = dd.concat([fraud_df, non_fraud_sample])[columns_to_keep]

# 保存最终的数据到新文件
final_df.to_csv('./eth_dataset/d2_10node_10f/filtered_nodes.csv', index=False, single_file=True)
