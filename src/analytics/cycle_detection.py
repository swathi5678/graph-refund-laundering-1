import networkx as nx
import logging

logger = logging.getLogger(__name__)

def detect_cycles(G, min_len=3, max_cycles=1000):
    """
    Detect suspicious cycles (money loops) involving Cards and Accounts.
    
    Warning: Cycle detection is computationally expensive (NP-Hard).
    This implementation limits the search to avoid hanging.
    """
    valid_cycles = []
    count = 0
    
    try:
        # Iterate over simple cycles
        # Note: simplistic approach. For huge graphs, use specific algorithms or ego-nets.
        for cycle in nx.simple_cycles(G):
            if len(cycle) >= min_len:
                valid_cycles.append(cycle)
            
            count += 1
            if count >= max_cycles:
                logger.warning(f"Cycle detection stopped early after checking {max_cycles} candidates")
                break
                
        logger.info(f"Found {len(valid_cycles)} cycles of length >= {min_len}")
        return valid_cycles
        
    except Exception as e:
        logger.error(f"Cycle detection failed: {str(e)}")
        return []
