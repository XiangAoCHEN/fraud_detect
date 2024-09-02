import pandas as pd
import dask.dataframe as dd
import networkx as nx
import numpy as np

# # Load node and edge data
# node_df = pd.read_csv('eth_dataset/d2_10node_10f/filtered_nodes.csv')
# edge_df = pd.read_csv('eth_dataset/d2_10node_10f/filtered_edges.csv')

# # Define the relevant columns for edges
# # txn_hash,nonce,block_hash,block_number,transaction_index,from,to,value,gas,gas_price,input,block_timestamp,cumulative_gas_used
# source_col = 5
# destination_col = 6
# edge_columns = [1, 3, 4, 7, 8, 9, 10, 11, 12]  # Adjusted to zero-indexed

# # Create a directed graph
# G = nx.DiGraph()

# # Add nodes to the graph (excluding account_id and fraud_label from attributes)
# for index, row in node_df.iterrows():
#     node_id = row['account_id']
#     label = row['fraud_label']
#     attributes = row.drop(['account_id', 'fraud_label']).to_dict()
#     G.add_node(node_id, **attributes, label=label)

# # Add edges to the graph with specified attributes
# for index, row in edge_df.iterrows():
#     source = row[source_col]
#     destination = row[destination_col]
#     edge_attributes = {f'attr_{col}': row[col] for col in edge_columns}
#     G.add_edge(source, destination, **edge_attributes)

# # Save the graph to a GraphML file
# nx.write_graphml(G, 'eth_dataset/d2_10node_10f/graph.graphml')


# Define the data types for the edge columns
dtypes = {
    'value': 'object',  # Treat as object to avoid OverflowError
    'gas': 'int64',
    'gas_price': 'int64',
    # Add other columns as needed
}

# Load node and edge data
node_df = dd.read_csv('eth_dataset/node_total.csv')
edge_df = dd.read_csv('eth_dataset/edge_total.csv', dtype=dtypes)



# Check for inf values
inf_values = node_df.map_partitions(lambda df: df.isin([np.inf, -np.inf]).any()).compute()

# Check for NA values
na_values = node_df.isna().compute()

# Display columns with inf or NA values
print("Columns with inf values:")
print(inf_values[inf_values])

print("Columns with NA values:")
print(na_values[na_values])

exit(0)

# preprocess node feature
# Define the columns related to amounts
amount_columns = [
    'send_amount_max', 'send_amount_min', 'send_amount_total', 'send_amount_avg',
    'receive_amount_max', 'receive_amount_min', 'receive_amount_total', 'receive_amount_avg',
    'send_fuel_quantity_max', 'send_fuel_quantity_min', 'send_fuel_quantity_total', 'send_fuel_quantity_avg',
    'receive_fuel_quantity_max', 'receive_fuel_quantity_min', 'receive_fuel_quantity_total', 'receive_fuel_quantity_avg',
    'send_fuel_unit_price_max', 'send_fuel_unit_price_min', 'send_fuel_unit_price_total', 'send_fuel_unit_price_avg',
    'receive_fuel_unit_price_max', 'receive_fuel_unit_price_min', 'receive_fuel_unit_price_total', 'receive_fuel_unit_price_avg',
    'send_gas_cost_max', 'send_gas_cost_min', 'send_gas_cost_total', 'send_gas_cost_avg',
    'receive_gas_cost_max', 'receive_gas_cost_min', 'receive_gas_cost_total', 'receive_gas_cost_avg'
]

# Function to replace -inf with -1
def replace_neg_inf(df):
    return df.map_partitions(lambda part: part.replace(-np.inf, -1))

# Apply the function to replace -inf with -1
node_df = replace_neg_inf(node_df)


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


# Define the relevant columns for edges
# 0 txn_hash,nonce,block_hash,block_number,transaction_index,
# 5 from,to,value,gas,gas_price,input,
# 11 block_timestamp,cumulative_gas_used
source_col = 5
destination_col = 6
edge_columns = [3, 7, 8]  # Adjusted to zero-indexed





# Create a directed graph
G = nx.DiGraph()

# Add nodes to the graph (excluding account_id and fraud_label from attributes)
def add_nodes_to_graph(row):
    node_id = row['account_id']
    label = row['fraud_label']
    attributes = {f'{col}': row.iloc[col] for col in node_columns}
    G.add_node(node_id, **attributes, label=label)

# Use Dask's map_partitions to apply the function across partitions
node_df.map_partitions(lambda df: df.apply(add_nodes_to_graph, axis=1)).compute()

# Add edges to the graph with specified attributes
def add_edges_to_graph(row):
    source = row.iloc[source_col]
    destination = row.iloc[destination_col]
    edge_attributes = {f'attr_{col}': row.iloc[col] for col in edge_columns}
    G.add_edge(source, destination, **edge_attributes)

# Use Dask's map_partitions to apply the function across partitions
edge_df.map_partitions(lambda df: df.apply(add_edges_to_graph, axis=1)).compute()

# Save the graph to a GraphML file
nx.write_graphml(G, 'eth_dataset/all/graph.graphml')