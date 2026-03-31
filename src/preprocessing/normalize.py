import pandas as pd
import logging

logger = logging.getLogger(__name__)

def preprocess(transactions, refunds):
    """Normalize and merge transaction and refund dataframes."""
    try:
        # Clone to avoid Side Effects
        tx = transactions.copy()
        rf = refunds.copy()
        
        # Convert Timestamps
        tx['timestamp'] = pd.to_datetime(tx['timestamp'], errors='coerce')
        rf['timestamp'] = pd.to_datetime(rf['timestamp'], errors='coerce')
        
        # Drop invalid timestamps
        if tx['timestamp'].isnull().any():
            logger.warning(f"Dropping {tx['timestamp'].isnull().sum()} invalid timestamps in transactions")
            tx = tx.dropna(subset=['timestamp'])
            
        if rf['timestamp'].isnull().any():
            logger.warning(f"Dropping {rf['timestamp'].isnull().sum()} invalid timestamps in refunds")
            rf = rf.dropna(subset=['timestamp'])

        # Align Columns
        rf = rf.rename(columns={'refund_account':'account_id'})
        
        # Type Enforcement
        tx['event_type'] = 'PAYMENT'
        rf['event_type'] = 'REFUND'
        
        # Ensure Channel compatibility
        if 'channel' not in rf.columns:
            rf['channel'] = 'UNKNOWN'
            
        common_cols = ['card_id', 'account_id', 'amount', 'timestamp', 'event_type', 'channel']
        
        # check missing in tx (already validated in load_data but good for safety)
        missing_tx = [c for c in common_cols if c not in tx.columns]
        if missing_tx:
            logger.error(f"Transactions missing columns for merge: {missing_tx}")
            raise ValueError(f"Transactions missing columns: {missing_tx}")

        # Concatenate
        df = pd.concat([tx[common_cols], rf[common_cols]], ignore_index=True)
        
        logger.info(f"Preprocessing complete. Total records: {len(df)}")
        return df

    except Exception as e:
        logger.error(f"Preprocessing failed: {str(e)}")
        raise
