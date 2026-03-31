import logging

logger = logging.getLogger(__name__)

def detect_refund_hubs(G, threshold=5):
    """
    Identify accounts (Merchants) that issue an excessive number of refunds.
    Returns a dict of Account -> Refund Count
    """
    hubs = {}
    try:
        # Accounts issuing refunds have Out-Edges to Cards
        # We should only count 'REFUND' edges
        
        for n, d in G.nodes(data=True):
            if d.get('type') == 'Account':
                # Count refund edges
                refund_count = 0
                for _, _, data in G.out_edges(n, data=True):
                    if data.get('event_type') == 'REFUND':
                        refund_count += 1
                
                if refund_count >= threshold:
                    hubs[n] = refund_count
                    
        logger.info(f"Detected {len(hubs)} refund hubs (threshold={threshold})")
        return hubs
        
    except Exception as e:
        logger.error(f"Refund hub detection failed: {str(e)}")
        return {}
