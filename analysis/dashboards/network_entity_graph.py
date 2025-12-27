import pandas as pd
import plotly.graph_objects as go
from collections import defaultdict
import networkx as nx
import json

from utils.utils import DATA_DIR

# Load your parquet file
df = pd.read_parquet(DATA_DIR / 'staging/sentiments.parquet')

# Build co-occurrence network (entities appearing in same articles)
entity_pairs = defaultdict(int)

for link in df['link'].unique():
    entities = df[df['link'] == link]['entity'].tolist()
    # Create edges between all entities in same article
    for i, e1 in enumerate(entities):
        for e2 in entities[i+1:]:
            pair = tuple(sorted([e1, e2]))
            entity_pairs[pair] += 1

# Create NetworkX graph for layout
G = nx.Graph()
for (e1, e2), weight in entity_pairs.items():
    if weight > 2:  # Increased threshold to reduce edges
        G.add_edge(e1, e2, weight=weight)

# Keep only nodes with at least 2 connections
G.remove_nodes_from([node for node in G.nodes() if G.degree(node) < 2])

# Detect communities using Louvain algorithm
communities = nx.community.louvain_communities(G, seed=42)

# Assign community colours
node_to_community = {}
community_colours = [
    '#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8',
    '#F7DC6F', '#BB8FCE', '#85C1E2', '#F8B88B', '#52B788'
]

for idx, community in enumerate(communities):
    colour = community_colours[idx % len(community_colours)]
    for node in community:
        node_to_community[node] = (idx, colour)

# Use community-aware layout
pos = nx.spring_layout(G, k=1.5, iterations=50, seed=42)

# Prepare data for all nodes and their neighbors
node_data = {}
for node in G.nodes():
    neighbors = list(G.neighbors(node))
    entity_data = df[df['entity'] == node].iloc[0]
    
    node_data[node] = {
        'pos': pos[node],
        'neighbors': neighbors,
        'colour': node_to_community[node][1],
        'mentions': entity_data['mention_count'],
        'sentiment': entity_data['average_sentiment_score'],
        'degree': G.degree(node)
    }

# Prepare edge weights dictionary
edge_weights = {}
for e1, e2, data in G.edges(data=True):
    key1 = f"{e1}|||{e2}"
    key2 = f"{e2}|||{e1}"
    edge_weights[key1] = data['weight']
    edge_weights[key2] = data['weight']

# Convert node data to JSON-safe format
node_data_json = {}
for node, data in node_data.items():
    node_data_json[node] = {
        'pos': [float(data['pos'][0]), float(data['pos'][1])],
        'neighbors': data['neighbors'],
        'colour': data['colour'],
        'mentions': int(data['mentions']),
        'sentiment': float(data['sentiment']),
        'degree': int(data['degree'])
    }

# Create HTML with custom JavaScript for interactivity
html_content = """
<!DOCTYPE html>
<html>
<head>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {
            margin: 0;
            padding: 20px;
            font-family: Arial, sans-serif;
        }
        #graph {
            width: 100%;
            height: 800px;
        }
        #info {
            padding: 10px;
            background: #f0f0f0;
            margin-bottom: 10px;
            border-radius: 5px;
        }
        #reset {
            padding: 8px 16px;
            background: #4ECDC4;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }
        #reset:hover {
            background: #45B7D1;
        }
    </style>
</head>
<body>
    <div id="info">
        <button id="reset">Reset View</button>
        <span id="status" style="margin-left: 20px;">Click on a node to view its connections</span>
    </div>
    <div id="graph"></div>
    
    <script>
"""

html_content += f"""
        const nodeData = {json.dumps(node_data_json)};
        const edgeWeights = {json.dumps(edge_weights)};
        
        let currentFilter = null;
        
        function createGraph(filterNode = null) {{
            let edges = [];
            let nodes = [];
            
            if (filterNode === null) {{
                // Show all nodes and edges
                Object.keys(nodeData).forEach(n1 => {{
                    nodeData[n1].neighbors.forEach(n2 => {{
                        const key = n1 + "|||" + n2;
                        const weight = edgeWeights[key] || 1;
                        const pos1 = nodeData[n1].pos;
                        const pos2 = nodeData[n2].pos;
                        
                        edges.push({{
                            x: [pos1[0], pos2[0], null],
                            y: [pos1[1], pos2[1], null],
                            mode: 'lines',
                            line: {{width: weight * 0.3, color: 'rgba(136, 136, 136, 0.3)'}},
                            hoverinfo: 'none',
                            showlegend: false
                        }});
                    }});
                }});
                
                // Remove duplicate edges
                const edgeSet = new Set();
                edges = edges.filter(edge => {{
                    const key = edge.x[0] + "," + edge.y[0] + "," + edge.x[1] + "," + edge.y[1];
                    const reverseKey = edge.x[1] + "," + edge.y[1] + "," + edge.x[0] + "," + edge.y[0];
                    if (edgeSet.has(key) || edgeSet.has(reverseKey)) return false;
                    edgeSet.add(key);
                    return true;
                }});
                
                // Create all nodes
                nodes = Object.keys(nodeData).map(node => {{
                    const data = nodeData[node];
                    return {{
                        x: data.pos[0],
                        y: data.pos[1],
                        text: node,
                        size: 10 + data.degree * 2,
                        colour: data.colour,
                        hovertext: `${{node}}<br>Mentions: ${{data.mentions}}<br>Avg Sentiment: ${{data.sentiment.toFixed(2)}}<br>Connections: ${{data.degree}}`
                    }};
                }});
            }} else {{
                // Show only selected node and its neighbors
                const centerNode = nodeData[filterNode];
                const neighbors = centerNode.neighbors;
                
                // Create edges in circular layout
                const radius = 0.4;
                const angleStep = (2 * Math.PI) / neighbors.length;
                
                neighbors.forEach((neighbor, idx) => {{
                    const angle = idx * angleStep;
                    const x = radius * Math.cos(angle);
                    const y = radius * Math.sin(angle);
                    
                    const key = filterNode + "|||" + neighbor;
                    const weight = edgeWeights[key] || 1;
                    
                    edges.push({{
                        x: [0, x, null],
                        y: [0, y, null],
                        mode: 'lines',
                        line: {{width: weight * 0.5, color: 'rgba(136, 136, 136, 0.5)'}},
                        hoverinfo: 'none',
                        showlegend: false
                    }});
                }});
                
                // Create center node
                nodes.push({{
                    x: 0,
                    y: 0,
                    text: filterNode,
                    size: 25,
                    colour: centerNode.colour,
                    hovertext: `${{filterNode}}<br>Mentions: ${{centerNode.mentions}}<br>Avg Sentiment: ${{centerNode.sentiment.toFixed(2)}}<br>Connections: ${{centerNode.degree}}`
                }});
                
                // Create neighbor nodes in circle
                neighbors.forEach((neighbor, idx) => {{
                    const angle = idx * angleStep;
                    const x = radius * Math.cos(angle);
                    const y = radius * Math.sin(angle);
                    const data = nodeData[neighbor];
                    
                    nodes.push({{
                        x: x,
                        y: y,
                        text: neighbor,
                        size: 10 + data.degree * 2,
                        colour: data.colour,
                        hovertext: `${{neighbor}}<br>Mentions: ${{data.mentions}}<br>Avg Sentiment: ${{data.sentiment.toFixed(2)}}<br>Connections: ${{data.degree}}`
                    }});
                }});
            }}
            
            // Create node trace
            const nodeTrace = {{
                x: nodes.map(n => n.x),
                y: nodes.map(n => n.y),
                mode: 'markers+text',
                text: nodes.map(n => n.text),
                textposition: 'top center',
                textfont: {{size: filterNode ? 12 : 8}},
                hovertext: nodes.map(n => n.hovertext),
                hoverinfo: 'text',
                marker: {{
                    size: nodes.map(n => n.size),
                    color: nodes.map(n => n.colour),
                    line: {{width: 2, color: 'white'}}
                }},
                customdata: nodes.map(n => n.text)
            }};
            
            const layout = {{
                title: filterNode ? `Connections for: ${{filterNode}}` : 'Entity Network (Click node to filter)',
                showlegend: false,
                hovermode: 'closest',
                xaxis: {{showgrid: false, zeroline: false, showticklabels: false}},
                yaxis: {{showgrid: false, zeroline: false, showticklabels: false}},
                height: 800,
                plot_bgcolor: 'white'
            }};
            
            Plotly.newPlot('graph', [...edges, nodeTrace], layout);
            
            // Add click event
            document.getElementById('graph').on('plotly_click', function(data) {{
                const point = data.points[0];
                const clickedNode = point.customdata;
                currentFilter = clickedNode;
                document.getElementById('status').textContent = `Viewing connections for: ${{clickedNode}}`;
                createGraph(clickedNode);
            }});
        }}
        
        // Reset button
        document.getElementById('reset').onclick = function() {{
            currentFilter = null;
            document.getElementById('status').textContent = 'Click on a node to view its connections';
            createGraph(null);
        }};
        
        // Initial render
        createGraph();
    </script>
</body>
</html>
"""

# Save the HTML file
with open(DATA_DIR / 'visuals' / 'network.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"Network has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
print(f"Found {len(communities)} communities")
print("Interactive network saved! Click any node to filter to its connections.")