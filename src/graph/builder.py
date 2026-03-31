import networkx as nx
import logging
from .schema import CARD, ACCOUNT

logger = logging.getLogger(__name__)

def build_graph(df):
    """
    Builds a MultiDiGraph where:
    - Nodes are Cards and Accounts
    - Edges represent money flow:
        - Payment: Card -> Account
        - Refund: Account -> Card
    """
    G = nx.MultiDiGraph()
    
    try:
        if df.empty:
            logger.warning("Empty dataframe provided for graph building")
            return G
            
        # Split by event type
        payments = df[df['event_type'] == 'PAYMENT']
        refunds = df[df['event_type'] == 'REFUND']
        
        logger.info(f"Building graph with {len(payments)} payments and {len(refunds)} refunds")
        
        # Add Payments (Card -> Account)
        if not payments.empty:
            # We can't use from_pandas_edgelist for multiple types at once efficiently if directions flip
            # So dealing with subset is correct
            g_pay = nx.from_pandas_edgelist(
                payments,
                source='card_id',
                target='account_id',
                edge_attr=['amount', 'timestamp', 'channel', 'event_type'],
                create_using=nx.MultiDiGraph()
            )
            G.add_nodes_from(g_pay.nodes(data=True))
            G.add_edges_from(g_pay.edges(keys=True, data=True))

        # Add Refunds (Account -> Card)
        if not refunds.empty:
            g_ref = nx.from_pandas_edgelist(
                refunds,
                source='account_id',
                target='card_id',
                edge_attr=['amount', 'timestamp', 'channel', 'event_type'],
                create_using=nx.MultiDiGraph()
            )
            G.add_nodes_from(g_ref.nodes(data=True))
            G.add_edges_from(g_ref.edges(keys=True, data=True))

        # Annotate Nodes with Types
        # Note: This assumes IDs are unique across entity types
        unique_cards = set(df['card_id'].unique())
        unique_accounts = set(df['account_id'].unique())
        
        # Check intersection
        intersection = unique_cards.intersection(unique_accounts)
        if intersection:
            logger.warning(f"ID Collision detected: {len(intersection)} IDs appear as both Card and Account. {list(intersection)[:5]}...")
        
        # Batch set attributes
        # If collision, last one wins. Logic: usually Account is final destination? 
        # But honestly, graph nodes should carry attributes.
        
        # Efficient way
        attrs = {}
        for n in G.nodes():
            if n in unique_cards:
                attrs[n] = CARD
            elif n in unique_accounts:
                attrs[n] = ACCOUNT
            else:
                attrs[n] = 'Unknown'
                
        nx.set_node_attributes(G, attrs, 'type')
        
        logger.info(f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        return G
        
    except Exception as e:
        logger.error(f"Graph building failed: {str(e)}")
        raise
