import streamlit as st
import pandas as pd
import networkx as nx
import plotly.graph_objects as go
import sys
import yaml
from pathlib import Path

# Add project root to sys.path to allow module imports
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

# Change working directory to project root for relative paths
import os
os.chdir(root_dir)

from src.main import main as run_pipeline
from src.ingestion.load_data import load_transactions, load_refunds
from src.preprocessing.normalize import preprocess
from src.graph.builder import build_graph

# Configure Page
st.set_page_config(
    page_title="Refund Laundering Detection",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for "Bank Premium" Feel
st.markdown("""
<style>
    .reportview-container {
        background: #0e1117;
    }
    .main .block-container {
        padding-top: 2rem;
    }
    h1 {
        color: #ffffff;
        font-family: 'Helvetica Neue', sans-serif;
    }
    h2, h3 {
        color: #a0a0a0;
    }
    .stButton>button {
        color: white;
        background-color: #0046ad; /* Citi Blue */
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
    }
    .stDataFrame {
        border: 1px solid #333;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Helper Functions

@st.cache_data
def load_config():
    config_path = root_dir / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

@st.cache_data
def load_processed_data():
    """Load results if available, else None"""
    path = root_dir / "data" / "processed" / "risk_scores.csv"
    if path.exists():
        return pd.read_csv(path)
    return None

def build_subgraph(graph, card_id, depth=2):
    """Extract ego graph for visualization"""
    if not graph.has_node(card_id):
        return None
    return nx.ego_graph(graph, card_id, radius=depth)

def plot_graph(subgraph, selected_node=None):
    """Plotly Network Visualization"""
    if subgraph is None:
        return None
        
    try:
        pos = nx.spring_layout(subgraph, seed=42)
        
        edge_x = []
        edge_y = []
        for edge in subgraph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])

        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines'
        )

        node_x = []
        node_y = []
        node_text = []
        node_color = []
        
        # Determine colors
        for node in subgraph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            # Safe attribute access
            ntype = subgraph.nodes[node].get('type', 'Unknown')
            
            # Color logic
            if node == selected_node:
                color = '#FFD700' # Gold for selected
            elif ntype == 'Account':
                color = '#EF553B' # Red
            elif ntype == 'Card':
                color = '#00CC96' # Green
            else:
                color = '#E2E2E2' # Grey
                
            node_color.append(color)
            node_text.append(f"{ntype}: {node}")

        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers',
            hoverinfo='text',
            marker=dict(
                showscale=False,
                color=node_color,
                size=20,
                line_width=2),
            text=node_text
        )

        fig = go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
            )
        )
        return fig
    except Exception as e:
        st.error(f"Graph plotting failed: {e}")
        return None

# Placeholder selected_card
selected_node = None

# -----------------------------------------------------------------------------
# Main Dashboard Logic

def main():
    st.sidebar.title("Dashboard Controls")
    
    # Run Pipeline Button
    if st.sidebar.button("Run Analysis Pipeline"):
        with st.spinner("Running detection algorithms..."):
            try:
                # We need to run main logic. It might be better to import functions or subprocess 
                # but importing is cleaner if structure allows.
                # However, main() prints to stdout. Let's redirect.
                run_pipeline()
                st.success("Pipeline Completed!")
                st.session_state['data_refresh'] = True
            except Exception as e:
                st.error(f"Pipeline Failed: {e}")

    # Load Data
    df = load_processed_data()
    
    st.title(" Refund Laundering Investigation")
    st.markdown("### Targeted Risk Analysis Dashboard")
    
    if df is None:
        st.warning("No analysis data found. Please run the pipeline from the sidebar.")
        return

    # metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Flagged Cards", len(df))
    col2.metric("Highest Risk Score", f"{df['risk_score'].max():.2f}")
    col3.metric("Avg Risk Score", f"{df['risk_score'].mean():.2f}")
    
    # -------------------------------------------------------------------------
    # Risk Table
    st.subheader("High Risk Entities")
    # Filter
    min_score = st.slider("Filter by Minimum Risk Score", 0.0, df['risk_score'].max(), 5.0)
    filtered_df = df[df['risk_score'] >= min_score]
    
    st.dataframe(
        filtered_df.style.background_gradient(subset=['risk_score'], cmap='Reds'),
        use_container_width=True
    )
    
    # -------------------------------------------------------------------------
    # Graph Investigation
    st.subheader("Network Graph Explorer")
    
    active_cards = filtered_df['card_id'].tolist()
    if active_cards:
        selected_card = st.selectbox("Select Card to Investigate", active_cards)
        st.session_state['selected_card'] = selected_card
        
        # Load Graph (Expensive, maybe cache globally?)
        # For demo, rebuild or load pickle. 
        # Rebuilding is safer given the modularity.
        config = load_config()
        tx = load_transactions(config['paths']['transactions'])
        rf = load_refunds(config['paths']['refunds'])
        graph_df = preprocess(tx, rf)
        G = build_graph(graph_df)
        
        subgraph = build_subgraph(G, selected_card)
        if subgraph:
            st.plotly_chart(plot_graph(subgraph), use_container_width=True)
            
            # Details
            st.write(f"**Card Details: {selected_card}**")
            details = filtered_df[filtered_df['card_id'] == selected_card].iloc[0]
            st.json(details.to_dict())
        else:
            st.error("Card not found in graph.")
    else:
        st.info("No cards meet the risk criteria.")

if __name__ == "__main__":
    main()
