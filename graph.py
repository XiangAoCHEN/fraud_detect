import pandas as pd
import networkx as nx

# Load node and edge data
node_df = pd.read_csv('node_total.csv')
edge_df = pd.read_csv('edge_total.csv', header=None)

# Define the relevant columns for edges
source_col = 5
destination_col = 6
edge_columns = [1, 3, 4, 7, 8, 9, 10, 11, 12]  # Adjusted to zero-indexed

# Create a directed graph
G = nx.DiGraph()

# Add nodes to the graph (excluding account_id and fraud_label from attributes)
for index, row in node_df.iterrows():
    node_id = row['account_id']
    label = row['fraud_label']
    attributes = row.drop(['account_id', 'fraud_label']).to_dict()
    G.add_node(node_id, **attributes, label=label)

# Add edges to the graph with specified attributes
for index, row in edge_df.iterrows():
    source = row[source_col]
    destination = row[destination_col]
    edge_attributes = {f'attr_{col}': row[col] for col in edge_columns}
    G.add_edge(source, destination, **edge_attributes)

# Save the graph to a GraphML file
nx.write_graphml(G, 'graph.graphml')