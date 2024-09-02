import pandas as pd
import dask.dataframe as dd
import networkx as nx
import numpy as np

input_node_file = '../eth_dataset/all/node_total_processed.csv'
input_edge_file = '../eth_dataset/all/edge_total_processed.csv'
output_graphml_file = '../eth_dataset/all/graph.graphml'

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
print("Loading edge data...")
edge_df = dd.read_csv(input_edge_file, dtype=edge_dtypes)

# select features  
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
node_columns = [2,4,6,8,34,35,36,37,38,39]
# print selected column name
all_columns = node_df.columns
selected_features = [all_columns[i] for i in node_columns]
print("Selected node features:")
print(selected_features)


# Define the relevant columns for edges
# 0 txn_hash,nonce,block_hash,block_number,transaction_index,
# 5 from,to,value,gas,gas_price,input,
# 11 block_timestamp,cumulative_gas_used
source_col = 5
destination_col = 6
edge_columns = [3, 7, 8]  # Adjusted to zero-indexed
all_columns = edge_df.columns
selected_features = [all_columns[i] for i in edge_columns]
selected_features.append(all_columns[source_col])
selected_features.append(all_columns[destination_col])
print("Selected edge features:")
print(selected_features)

# Create a directed graph
G = nx.DiGraph()

# Add nodes to the graph (excluding account_id and fraud_label from attributes)
def add_nodes_to_graph(row):
    node_id = row['account_id']
    label = row['fraud_label']
    attributes = {f'{col}': row.iloc[col] for col in node_columns}
    G.add_node(node_id, **attributes, label=label)

# Use Dask's map_partitions to apply the function across partitions
print("Adding nodes to the graph...")
node_df.map_partitions(lambda df: df.apply(add_nodes_to_graph, axis=1)).compute()

# Add edges to the graph with specified attributes
def add_edges_to_graph(row):
    source = row.iloc[source_col]
    destination = row.iloc[destination_col]
    edge_attributes = {f'attr_{col}': row.iloc[col] for col in edge_columns}
    G.add_edge(source, destination, **edge_attributes)

# Use Dask's map_partitions to apply the function across partitions
print("Adding edges to the graph...")
edge_df.map_partitions(lambda df: df.apply(add_edges_to_graph, axis=1)).compute()

# Save the graph to a GraphML file
nx.write_graphml(G, output_graphml_file)
print("GraphML file saved")
print("ok")