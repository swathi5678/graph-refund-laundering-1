import pandas as pd
import random
from datetime import datetime, timedelta
import os

def generate_data(num_cards=100, num_merchants=20, num_tx=1000):
    print("Generating sample data...")
    
    # Create directories
    os.makedirs('data/raw', exist_ok=True)
    
    cards = [f'C{i:04d}' for i in range(num_cards)]
    merchants = [f'M{i:03d}' for i in range(num_merchants)]
    channels = ['POS', 'WEB', 'MOBILE', 'ATM']
    
    # 1. Normal Transactions
    data = []
    start_date = datetime.now() - timedelta(days=30)
    
    for _ in range(num_tx):
        card = random.choice(cards)
        merchant = random.choice(merchants)
        amount = round(random.uniform(10, 500), 2)
        timestamp = start_date + timedelta(minutes=random.randint(0, 43200))
        channel = random.choice(channels)
        
        data.append({
            'card_id': card,
            'account_id': merchant,
            'amount': amount,
            'timestamp': timestamp,
            'channel': channel,
            'event_type': 'PAYMENT'
        })
        
    # 2. Inject Fraud Patterns
    
    # A. Refund Hub (Merchant M001 is a laundry service)
    bad_merchant = 'M001'
    bad_cards = cards[:10] # First 10 cards are mules
    
    for card in bad_cards:
        # Payment
        ts = start_date + timedelta(days=25)
        data.append({
            'card_id': card,
            'account_id': bad_merchant,
            'amount': 1000.00,
            'timestamp': ts,
            'channel': 'WEB',
            'event_type': 'PAYMENT'
        })
        # Refund
        data.append({
            'card_id': card,
            'account_id': bad_merchant, # In refund csv this is 'refund_account'
            'amount': 1000.00,
            'timestamp': ts + timedelta(hours=2),
            'channel': 'WEB', # Refund might not have channel but for simplicity
            'event_type': 'REFUND'
        })

    # B. Cycle (C099 -> M002 -> C099 via refund logic? No, cycle is typically money movement)
    # Since our validation separates Payment (C->M) and Refund (M->C), a cycle is C->M->C
    # Let's create a cycle: C090 -> M005 -> C090 (Refund) -> M005 ...
    cycle_card = 'C090'
    cycle_merchant = 'M005'
    ts = start_date + timedelta(days=28)
    for _ in range(5):
        data.append({
            'card_id': cycle_card,
            'account_id': cycle_merchant,
            'amount': 500,
            'timestamp': ts,
            'channel': 'POS',
            'event_type': 'PAYMENT'
        })
        data.append({
            'card_id': cycle_card,
            'account_id': cycle_merchant,
            'amount': 500,
            'timestamp': ts + timedelta(minutes=30),
            'channel': 'POS', # This will go to refund file
            'event_type': 'REFUND'
        })
        ts += timedelta(hours=1)
        
    # Convert to DataFrames
    df = pd.DataFrame(data)
    
    # Split
    payments = df[df['event_type'] == 'PAYMENT'][['card_id', 'account_id', 'amount', 'timestamp', 'channel']]
    refunds = df[df['event_type'] == 'REFUND'][['card_id', 'account_id', 'amount', 'timestamp']]
    refunds.rename(columns={'account_id': 'refund_account'}, inplace=True)
    
    # Save
    payments.to_csv('data/raw/transactions.csv', index=False)
    refunds.to_csv('data/raw/refunds.csv', index=False)
    
    print(f"Generated {len(payments)} payments and {len(refunds)} refunds.")

if __name__ == '__main__':
    generate_data()
