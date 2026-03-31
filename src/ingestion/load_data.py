import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    'transactions': ['card_id', 'account_id', 'amount', 'timestamp', 'channel'],
    'refunds': ['card_id', 'refund_account', 'amount', 'timestamp']
}

def validate_schema(df, dataset_name):
    """Validate that the dataframe contains required columns"""
    required = REQUIRED_COLUMNS.get(dataset_name, [])
    missing = [col for col in required if col not in df.columns]
    
    if missing:
        error_msg = f"Missing required columns in {dataset_name}: {missing}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"Schema validation passed for {dataset_name}")
    return True

def load_csv(path, name):
    """Generic csv loader with validation"""
    if not os.path.exists(path):
        logger.error(f"File not found: {path}")
        raise FileNotFoundError(f"File not found: {path}")
        
    try:
        df = pd.read_csv(path)
        validate_schema(df, name)
        logger.info(f"Successfully loaded {len(df)} rows from {path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load {name}: {str(e)}")
        raise

def load_transactions(path):
    return load_csv(path, 'transactions')

def load_refunds(path):
    return load_csv(path, 'refunds')
