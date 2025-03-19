import pandas as pd

def save_nodes_and_edges(file_path, nodes_output, edges_output):
    # Load the network data
    network_data = pd.read_csv(file_path)

    # Ensure IDs are consistent (strip whitespace and enforce lowercase if needed)
    network_data['source'] = network_data['source'].str.strip()
    network_data['target'] = network_data['target'].str.strip()

    # Extract unique nodes with attributes
    nodes = pd.DataFrame({
        'Id': pd.concat([network_data['source'], network_data['target']]).unique()
    })

    # Prepare the edges table
    edges = network_data[['source', 'target', 'relation', 'likes']].rename(columns={
        'source': 'Source', 'target': 'Target', 'likes': 'Weight'
    })

    # Add a Direction column to clarify relationships
    edges['Direction'] = edges.apply(
        lambda row: 'Target->Source' if row['relation'] == 'follower' else 'Source->Target',
        axis=1
    )

    # Convert Boolean likes to integers for weight (True = 2, False = 1)
    edges['Weight'] = edges['Weight'].astype(int) + 1

    # Save nodes and edges to CSV for Gephi
    nodes.to_csv(nodes_output, index=False)
    edges.to_csv(edges_output, index=False)

# File paths
file_path = "filepath" #replace with filepath
nodes_output = "nodes_output" #replace with nodes output
edges_output = "edges_output" #replace with edges output

# Process the data
save_nodes_and_edges(file_path, nodes_output, edges_output)

print(f"Nodes file saved to: {nodes_output}")
print(f"Edges file saved to: {edges_output}")

