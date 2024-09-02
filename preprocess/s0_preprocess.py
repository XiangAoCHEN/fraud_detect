import pandas as pd
import dask.dataframe as dd
import networkx as nx
import numpy as np

float64_max = np.finfo(np.float64).max
int64_max = np.iinfo(np.int64).max

input_node_file = '../eth_dataset/node_total.csv'
input_edge_file = '../eth_dataset/edge_total.csv'
new_node_file = '../eth_dataset/all/node_total_processed.csv'
new_edge_file = '../eth_dataset/all/edge_total_processed.csv'

# Load node and edge data
# 0 account_id,fraud_label,
# 2 send_amount_max,send_amount_min,send_amount_total,send_amount_avg,
# 6 receive_amount_max,receive_amount_min,receive_amount_total,receive_amount_avg,
# 10 send_fuel_quantity_max,send_fuel_quantity_min,send_fuel_quantity_total,send_fuel_quantity_avg,
# 14 receive_fuel_quantity_max,receive_fuel_quantity_min,receive_fuel_quantity_total,receive_fuel_quantity_avg,
# 18 send_fuel_unit_price_max,send_fuel_unit_price_min,send_fuel_unit_price_total,send_fuel_unit_price_avg,
# 22 receive_fuel_unit_price_max,receive_fuel_unit_price_min,receive_fuel_unit_price_total,receive_fuel_unit_price_avg,
# 26 send_gas_cost_max,send_gas_cost_min,send_gas_cost_total,send_gas_cost_avg,
# 30 receive_gas_cost_max,receive_gas_cost_min,receive_gas_cost_total,receive_gas_cost_avg,
# 34 in_degree,out_degree,total_degree,
# 37 earliest_transaction,latest_transaction,transaction_interval
node_dtypes = {
    'account_id': 'object',# address
    'fraud_label': 'int64',# 0 or 1, whether is fraud account
    'send_amount_max': 'float64', # wei
    'send_amount_min': 'float64',
    'send_amount_total': 'float64',
    'send_amount_avg': 'float64',
    'receive_amount_max': 'float64',
    'receive_amount_min': 'float64',
    'receive_amount_total': 'float64',
    'receive_amount_avg': 'float64',
    'send_fuel_quantity_max': 'float64', # Gas Usage by Txn: Maximum amount of gas allocated for transaction 
    'send_fuel_quantity_min': 'float64',
    'send_fuel_quantity_total': 'float64',
    'send_fuel_quantity_avg': 'float64',
    'receive_fuel_quantity_max': 'float64',
    'receive_fuel_quantity_min': 'float64',
    'receive_fuel_quantity_total': 'float64',
    'receive_fuel_quantity_avg': 'float64',
    'send_fuel_unit_price_max': 'float64', #Gas Price, wei
    'send_fuel_unit_price_min': 'float64',
    'send_fuel_unit_price_total': 'float64',
    'send_fuel_unit_price_avg': 'float64',
    'receive_fuel_unit_price_max': 'float64',
    'receive_fuel_unit_price_min': 'float64',
    'receive_fuel_unit_price_total': 'float64',
    'receive_fuel_unit_price_avg': 'float64',
    'send_gas_cost_max': 'float64', # Transaction Fee: Amount paid to the miner for processing the transaction
    'send_gas_cost_min': 'float64',
    'send_gas_cost_total': 'float64',
    'send_gas_cost_avg': 'float64',
    'receive_gas_cost_max': 'float64',
    'receive_gas_cost_min': 'float64',
    'receive_gas_cost_total': 'float64',
    'receive_gas_cost_avg': 'float64',
    'in_degree': 'int64',
    'out_degree': 'int64',
    'total_degree': 'int64',
    'earliest_transaction': 'int64', # timestamp
    'latest_transaction': 'int64',
    'transaction_interval': 'int64'
}
print("Loading node data...")
node_df = dd.read_csv(input_node_file, dtype=node_dtypes)

def check_integrity(df, name:str):
    print(f"Checking integrity of {name} data...")

    # # Check for missing values
    # missing_values = df.isnull().sum()
    # print(f"Found {len(missing_values)} columns with missing values:")
    # print(missing_values)

    # Check for inf values
    inf_values = df.map_partitions(lambda df: df.isin([np.inf]).any()).compute()
    inf_columns = list(set(inf_values[inf_values].index.tolist()))
    print(f"{len(inf_columns)} columns with inf values:")
    print(", ".join(inf_columns))
    # Check for -inf values
    minus_inf_values = df.map_partitions(lambda df: df.isin([-np.inf]).any()).compute()
    minus_inf_columns = list(set(minus_inf_values[minus_inf_values].index.tolist()))
    print(f"\n\n{len(minus_inf_columns)} columns with -inf values:")
    print(", ".join(minus_inf_columns))
    # Check for NA values
    na_values = df.isna().compute()
    na_columns = list(set(na_values.columns[na_values.isna().any()].tolist()))
    print(f"\n\n{len(na_columns)} columns with NA values:")
    print(", ".join(na_columns))

    return inf_columns, minus_inf_columns, na_columns



inf_columns, minus_inf_columns, na_columns = check_integrity(node_df, "node_df")

# Define NA replacement values for each column
na_replacement_values_node = {
    # 'fraud_label': 0,
    'send_amount_max': 0,
    'send_amount_min': 0,
    'send_amount_total': 0,
    'send_amount_avg': 0,
    'receive_amount_max': 0,
    'receive_amount_min': 0,
    'receive_amount_total': 0,
    'receive_amount_avg': 0,
    'send_fuel_quantity_max': 0,
    'send_fuel_quantity_min': 0,
    'send_fuel_quantity_total': 0,
    'send_fuel_quantity_avg': 0,
    'receive_fuel_quantity_max': 0,
    'receive_fuel_quantity_min': 0,
    'receive_fuel_quantity_total': 0,
    'receive_fuel_quantity_avg': 0,
    'send_fuel_unit_price_max': 0,
    'send_fuel_unit_price_min': 0,
    'send_fuel_unit_price_total': 0,
    'send_fuel_unit_price_avg': 0,
    'receive_fuel_unit_price_max': 0,
    'receive_fuel_unit_price_min': 0,
    'receive_fuel_unit_price_total': 0,
    'receive_fuel_unit_price_avg': 0,
    'send_gas_cost_max': 0,
    'send_gas_cost_min': 0,
    'send_gas_cost_total': 0,
    'send_gas_cost_avg': 0,
    'receive_gas_cost_max': 0,
    'receive_gas_cost_min': 0,
    'receive_gas_cost_total': 0,
    'receive_gas_cost_avg': 0,
    'in_degree': 0,
    'out_degree': 0,
    'total_degree': 0,
    'earliest_transaction': 0,
    'latest_transaction': 0,
    'transaction_interval': 0
}



inf_relacement_values_node ={
    'send_amount_max': float64_max,
    'send_amount_min': float64_max,
    'send_amount_total': float64_max,
    'send_amount_avg': float64_max,
    'receive_amount_max': float64_max,
    'receive_amount_min': float64_max,
    'receive_amount_total': float64_max,
    'receive_amount_avg': float64_max,
    'send_fuel_quantity_max': float64_max,
    'send_fuel_quantity_min': float64_max,
    'send_fuel_quantity_total': float64_max,
    'send_fuel_quantity_avg': float64_max,
    'receive_fuel_quantity_max': float64_max,
    'receive_fuel_quantity_min': float64_max,
    'receive_fuel_quantity_total': float64_max,
    'receive_fuel_quantity_avg': float64_max,
    'send_fuel_unit_price_max': float64_max,
    'send_fuel_unit_price_min': float64_max,
    'send_fuel_unit_price_total': float64_max,
    'send_fuel_unit_price_avg': float64_max,
    'receive_fuel_unit_price_max': float64_max,
    'receive_fuel_unit_price_min': float64_max,
    'receive_fuel_unit_price_total': float64_max,
    'receive_fuel_unit_price_avg': float64_max,
    'send_gas_cost_max': float64_max,
    'send_gas_cost_min': float64_max,
    'send_gas_cost_total': float64_max,
    'send_gas_cost_avg': float64_max,
    'receive_gas_cost_max': float64_max,
    'receive_gas_cost_min': float64_max,
    'receive_gas_cost_total': float64_max,
    'receive_gas_cost_avg': float64_max,
    # 'in_degree': int64_max,
    # 'out_degree': int64_max,
    # 'total_degree': int64_max,
    # 'earliest_transaction': int64_max,
    # 'latest_transaction': int64_max,
    # 'transaction_interval': int64_max
}


minus_inf_replacement_values_node = {
    'send_amount_max': 0,
    'send_amount_min': 0,
    'send_amount_total': 0,
    'send_amount_avg': 0,
    'receive_amount_max': 0,
    'receive_amount_min': 0,
    'receive_amount_total': 0,
    'receive_amount_avg': 0,
    'send_fuel_quantity_max': 0,
    'send_fuel_quantity_min': 0,
    'send_fuel_quantity_total': 0,
    'send_fuel_quantity_avg': 0,
    'receive_fuel_quantity_max': 0,
    'receive_fuel_quantity_min': 0,
    'receive_fuel_quantity_total': 0,
    'receive_fuel_quantity_avg': 0,
    'send_fuel_unit_price_max': 0,
    'send_fuel_unit_price_min': 0,
    'send_fuel_unit_price_total': 0,
    'send_fuel_unit_price_avg': 0,
    'receive_fuel_unit_price_max': 0,
    'receive_fuel_unit_price_min': 0,
    'receive_fuel_unit_price_total': 0,
    'receive_fuel_unit_price_avg': 0,
    'send_gas_cost_max': 0,
    'send_gas_cost_min': 0,
    'send_gas_cost_total': 0,
    'send_gas_cost_avg': 0,
    'receive_gas_cost_max': 0,
    'receive_gas_cost_min': 0,
    'receive_gas_cost_total': 0,
    'receive_gas_cost_avg': 0,
    'in_degree': 0,
    'out_degree': 0,
    'total_degree': 0,
    'earliest_transaction': 0,
    'latest_transaction': 0,
    'transaction_interval': 0
}

# Replace NA values
for col in na_columns:
    node_df[col] = node_df[col].fillna(na_replacement_values_node[col])
    
# replace inf values
for col in inf_columns:
    node_df[col] = node_df[col].replace([np.inf], inf_relacement_values_node[col])

for col in minus_inf_columns:
    node_df[col] = node_df[col].replace([-np.inf], minus_inf_replacement_values_node[col])

if len(inf_columns) > 0 or len(minus_inf_columns) > 0 or len(na_columns) > 0:
    print("after replacing, checking for node feature values...")
    inf_columns, minus_inf_columns, na_columns = check_integrity(node_df, "node_df")

# save the processed node data
node_df.to_csv(new_node_file, single_file=True, index=False)
print("Node data saved to ", new_node_file)



# 0 txn_hash,nonce,block_hash,block_number,transaction_index,
# 5 from,to,value,gas,gas_price,input,
# 11 block_timestamp,cumulative_gas_used
columns = [
    'txn_hash', 'nonce', 'block_hash', 'block_number', 'transaction_index',
    'from', 'to', 'value', 'gas', 'gas_price', 'input', 'block_timestamp', 'cumulative_gas_used'
]
edge_dtypes = {
    'txn_hash': 'object',
    'nonce': 'int64', # a transaction counter of from account
    'block_hash': 'object',
    'block_number': 'int64',
    'transaction_index': 'int64',
    'from': 'object',
    'to': 'object',
    'value': 'float64',  # Using float64 to avoid overflow issues
    'gas': 'int64',
    'gas_price': 'int64',
    'input': 'object',
    'block_timestamp': 'int64',
    'cumulative_gas_used': 'float64'
}
print("loading edge csv")
edge_df = dd.read_csv(
    input_edge_file,
    names=columns,  # 手动指定列名
    dtype=edge_dtypes,
    header=None  # 指定文件没有列头
)

inf_columns, minus_inf_columns, na_columns = check_integrity(edge_df, "edge_df")

na_replacement_values_edge = {
    'nonce': -1,
    'block_number': -1,
    'transaction_index': -1,
    'value': 0,
    'gas': -1,
    'gas_price': -1,
    'block_timestamp': -1,
    'cumulative_gas_used': -1
}

inf_relacement_values_edge ={
    'nonce': int64_max,
    'block_number': int64_max,
    'transaction_index': int64_max,
    'value': float64_max,
    'gas': int64_max,
    'gas_price': int64_max,
    'block_timestamp': int64_max,
    'cumulative_gas_used': float64_max
}

minus_inf_replacement_values_edge = {
    'nonce': 0,
    'block_number': 0,
    'transaction_index': 0,
    'value': 0,
    'gas': 0,
    'gas_price': 0,
    'block_timestamp': 0,
    'cumulative_gas_used': 0
}

# Replace NA values
for col in na_columns:
    edge_df[col] = edge_df[col].fillna(na_replacement_values_edge[col])
for col in inf_columns:
    edge_df[col] = edge_df[col].replace([np.inf], inf_relacement_values_edge[col])
for col in minus_inf_columns:
    edge_df[col] = edge_df[col].replace([-np.inf], minus_inf_replacement_values_edge[col])

if len(inf_columns) > 0 or len(minus_inf_columns) > 0 or len(na_columns) > 0:
    print("after replacing, checking for edge feature values...")
    inf_columns, minus_inf_columns, na_columns = check_integrity(edge_df, "edge_df")

# save the processed edge data

edge_df.to_csv(new_edge_file, single_file=True, index=False)
print("Edge data saved to ", new_edge_file)

print("ok")