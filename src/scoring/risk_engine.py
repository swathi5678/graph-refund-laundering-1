import logging

logger = logging.getLogger(__name__)

def calculate_card_risk(card_nodes, G, hubs, entropy, cycles, temporal, weights):
    """
    Aggregates risk factors for each card.
    
    Factors:
    - Interaction with Refund Hubs (Merchants)
    - Channel Entropy (High entropy = suspicious)
    - Involvement in Cycles (Money loops)
    - Temporal Clustering (Rapid transactions)
    """
    risk_scores = {}
    
    try:
        # Pre-process cycles for fast lookup
        nodes_in_cycles = {}
        for i, cycle in enumerate(cycles):
            for node in cycle:
                if node not in nodes_in_cycles:
                    nodes_in_cycles[node] = 0
                nodes_in_cycles[node] += 1
        
        for card in card_nodes:
            # 1. Hub Interaction
            # Check if Card paid to a Hub Account
            hub_interactions = 0
            # Iterate out edges (Payments)
            if G.has_node(card):
                for _, target, _ in G.out_edges(card, data=True):
                    if target in hubs:
                        hub_interactions += 1
            
            # 2. Entropy
            ent_val = entropy.get(card, 0)
            
            # 3. Cycles
            cyc_count = nodes_in_cycles.get(card, 0)
            
            # 4. Temporal
            temp_val = temporal.get(card, 0)
            
            # Calculate Score
            # Note: inputs are not normalized (e.g. hub_interactions can be 0, 1, 10...)
            # We assume weights handle the scale or we just sum raw linear combination.
            # In a real model, we'd limit/sigmoid these.
            
            score = (
                weights.get('refund_hub', 0.35) * hub_interactions +
                weights.get('channel_entropy', 0.25) * ent_val +
                weights.get('cycle', 0.20) * cyc_count +
                weights.get('temporal', 0.20) * temp_val
            )
            
            if score > 0:
                risk_scores[card] = {
                    'risk_score': round(score, 4),
                    'breakdown': {
                        'hub_interactions': hub_interactions,
                        'channel_entropy': round(ent_val, 4),
                        'cycles_involved': cyc_count,
                        'temporal_bursts': temp_val
                    }
                }
                
        return risk_scores

    except Exception as e:
        logger.error(f"Risk calculation failed: {str(e)}")
        return {}
