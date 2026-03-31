from datetime import timedelta
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def temporal_clustering(G, window_days=7):
    """
    Detect cards performing high-frequency transactions within a short window.
    Returns a dict of Card -> Cluster Count (pairs of tx within window)
    """
    scores = {}
    window = timedelta(days=window_days)
    
    try:
        for n, d in G.nodes(data=True):
            if d.get('type') == 'Card':
                # Get timestamps of outgoing payments
                # Filter for PAYMENT? Or just all out-edges (which are payments for Cards)
                # Safeguard: only look at edges with 'timestamp'
                timestamps = []
                for _, _, data in G.out_edges(n, data=True):
                    ts = data.get('timestamp')
                    if ts and not pd.isna(ts):
                        timestamps.append(ts)
                
                if not timestamps:
                    continue
                    
                timestamps.sort()
                
                # Count pairs within window efficiently
                count = 0
                n_tx = len(timestamps)
                
                # Using two pointers or just optimized inner loop
                # Since window is fixed, this is efficient
                for i in range(n_tx):
                    for j in range(i + 1, n_tx):
                        if timestamps[j] - timestamps[i] <= window:
                            count += 1
                        else:
                            # Since sorted, all subsequent j will be outside window
                            break
                            
                if count > 0:
                    scores[n] = count
                    
        logger.info(f"Temporal analysis complete for {len(scores)} active cards")
        return scores
        
    except Exception as e:
        logger.error(f"Temporal clustering failed: {str(e)}")
        return {}


