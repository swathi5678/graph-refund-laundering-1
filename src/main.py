import sys
import logging
import pandas as pd
import networkx as nx
from pathlib import Path

# Fix path to allow imports from src
sys.path.append(str(Path(__file__).parent))

from ingestion.load_data import load_transactions, load_refunds
from preprocessing.normalize import preprocess
from graph.builder import build_graph
from analytics.refund_hubs import detect_refund_hubs
from analytics.channel_entropy import channel_entropy
from analytics.cycle_detection import detect_cycles
from analytics.temporal_patterns import temporal_clustering
from scoring.risk_engine import calculate_card_risk
from utils.logger import setup_logger
from utils.config import load_config

def main():
    # 1. Setup Environment
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        print(f"Config not found at {config_path}")
        return

    config = load_config(config_path)
    
    # Setup Logger
    logger = setup_logger(
        "RefundLaundering", 
        log_file=Path(config['paths']['logs_dir']) / "app.log",
        level=logging.INFO
    )
    
    logger.info("Starting Refund Laundering Detection Pipeline")
    
    try:
        # 2. Data Ingestion
        logger.info("Loading data...")
        tx_path = config['paths']['transactions']
        rf_path = config['paths']['refunds']
        
        tx = load_transactions(tx_path)
        rf = load_refunds(rf_path)
        
        # 3. Preprocessing
        logger.info("Preprocessing data...")
        df = preprocess(tx, rf)
        
        # 4. Graph Construction
        logger.info("Building Transaction Graph...")
        G = build_graph(df)
        
        if G.number_of_nodes() == 0:
            logger.warning("Graph has no nodes. Exiting.")
            return

        # 5. Analytics Execution
        logger.info("Running Analytics Modules...")
        
        # A. Refund Hubs
        hubs = detect_refund_hubs(
            G, 
            threshold=config['thresholds']['refund_hub_degree']
        )
        
        # B. Channel Entropy
        entropy = channel_entropy(G)
        
        # C. Cycle Detection
        cycles = detect_cycles(
            G, 
            min_len=config['thresholds']['min_cycle_length']
        )
        
        # D. Temporal Patterns
        temporal = temporal_clustering(
            G, 
            window_days=config['thresholds']['temporal_window_days']
        )
        
        # 6. Risk Scoring
        logger.info("Calculating Risk Scores...")
        cards = [n for n, d in G.nodes(data=True) if d.get('type') == 'Card']
        
        scores = calculate_card_risk(
            cards, 
            G, 
            hubs, 
            entropy, 
            cycles, 
            temporal, 
            config['risk_weights']
        )
        
        # 7. Output Results
        output_dir = Path(config['paths']['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Convert to DataFrame for easy viewing
        results = []
        for card, data in scores.items():
            row = {'card_id': card, 'risk_score': data['risk_score']}
            row.update(data['breakdown'])
            results.append(row)
            
        if results:
            res_df = pd.DataFrame(results)
            res_df.sort_values('risk_score', ascending=False, inplace=True)
            
            out_file = output_dir / "risk_scores.csv"
            res_df.to_csv(out_file, index=False)
            logger.info(f"Risk scores saved to {out_file}")
            print(f"Top 5 Risky Cards:\n{res_df.head()}")
        else:
            logger.info("No risky cards detected.")
            
        logger.info("Pipeline completed successfully.")
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
