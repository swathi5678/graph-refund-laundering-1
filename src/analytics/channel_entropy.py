import math
from collections import Counter
import logging

logger = logging.getLogger(__name__)

def channel_entropy(G):
    """
    Calculate entropy of payment channels for each Card.
    High entropy means the card uses many different channels (Web, Mobile, POS, etc.)
    which might indicate bot activity or stolen credentials testing.
    """
    scores = {}
    try:
        for n, d in G.nodes(data=True):
            if d.get('type') == 'Card':
                # Extract channels from payment edges
                # Filter for PAYMENT type just in case we have mixed edges
                channels = [
                    data.get('channel', 'UNKNOWN') 
                    for _, _, data in G.out_edges(n, data=True)
                    if data.get('event_type') == 'PAYMENT'
                ]
                
                if not channels:
                    scores[n] = 0.0
                    continue
                    
                counts = Counter(channels)
                total = len(channels)
                
                # Shannon Entropy
                entropy = -sum((c/total)*math.log2(c/total) for c in counts.values())
                scores[n] = entropy
                
        logger.info(f"Calculated channel entropy for {len(scores)} cards")
        return scores
        
    except Exception as e:
        logger.error(f"Channel entropy calculation failed: {str(e)}")
        return {}
