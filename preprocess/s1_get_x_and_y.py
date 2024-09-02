import dask.dataframe as dd

# 读取原始CSV文件
print("Reading CSV file...")
input_file = '../eth_dataset/all/node_total_processed.csv'
df = dd.read_csv(input_file)

# 提取fraud_label列保存为y.csv
y = df[['fraud_label']]
y.to_csv('../eth_dataset/all/y.csv', single_file=True, index=False)
print("y.csv saved")

# 提取除fraud_label列以外的其他列保存为x.csv
x = df.drop(columns=['fraud_label'])
x.to_csv('../eth_dataset/all/x.csv', single_file=True, index=False)
print("x.csv saved")

print("ok")
