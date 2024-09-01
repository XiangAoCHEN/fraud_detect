import dask.dataframe as dd

# 读取节点和边的文件
nodes_path = 'eth_dataset/d2_10node_10f/filtered_nodes.csv'
edges_path = 'eth_dataset/edge_total.csv'

# 使用Dask读取数据，并指定'dtype'以避免类型推断错误
edge_dtype = {
    'txn_hash': 'object',
    'nonce': 'float64',
    'block_hash': 'object',
    'block_number': 'int64',
    'transaction_index': 'int64',
    'from': 'object',
    'to': 'object',
    'value': 'object',  # 由于可能存在非常大的整数，使用object类型
    'gas': 'int64',
    'gas_price': 'int64',
    'input': 'object',
    'block_timestamp': 'int64',
    'cumulative_gas_used': 'int64'
}

# 使用Dask读取数据
nodes_df = dd.read_csv(nodes_path)
edges_df = dd.read_csv(edges_path,dtype=edge_dtype)

# 获取存在的节点ID列表
valid_nodes = nodes_df['account_id'].compute()

# 过滤边，只保留from和to都在valid_nodes中的边
filtered_edges_df = edges_df[(edges_df['from'].isin(valid_nodes)) & (edges_df['to'].isin(valid_nodes))]

# # 选择保留的列
# columns_to_keep = ['txn_hash', 'nonce', 'block_hash', 'block_number', 'transaction_index',
#                    'from', 'to', 'value', 'gas', 'gas_price', 'input', 'block_timestamp', 'cumulative_gas_used']

# # 根据需要的列索引获取列名
# selected_columns = [columns_to_keep[i] for i in [1, 3, 4, 7, 8, 9, 10, 11, 12]]

# 将结果保存到新的文件中
filtered_edges_df.to_csv('eth_dataset/d2_10node_10f/filtered_edges.csv', index=False, single_file=True)
