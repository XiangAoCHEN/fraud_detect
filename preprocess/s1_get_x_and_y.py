import dask.dataframe as dd

input_node_file = '../eth_dataset/all/node_total_processed.csv'
output_x_file = '../eth_dataset/all/x.csv'
output_y_file = '../eth_dataset/all/y.csv'

# 读取原始CSV文件
print(f"Reading {input_node_file}...")
df = dd.read_csv(input_node_file)

# 提取fraud_label列保存为y.csv
y = df[['fraud_label']]
y.to_csv(output_y_file, single_file=True, index=False)
print(f"y.csv saved {output_y_file}")

# 提取除fraud_label列以外的其他列保存为x.csv
x = df.drop(columns=['fraud_label'])
x.to_csv(output_x_file, single_file=True, index=False)
print(f"x.csv saved {output_x_file}")

print("ok")
