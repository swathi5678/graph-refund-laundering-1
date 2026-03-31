# Graph-Based Detection of Coordinated Credit Card Overpayment & Refund Laundering

## Overview
This is an enterprise-grade graph analytics pipeline designed to detect "Refund Laundering" schemes where bad actors use coordinated refunds to launder money or manipulate credit.

The system constructs a directed graph of money flow (Payments & Refunds) and applies advanced network algorithms to identify suspicious entities.

## Architecture
- **Data Ingestion**: Robust loading of Transaction and Refund data with schema validation.
- **Graph Construction**: Vectorized build of a Multi-Directed Graph representing financial flows:
  - **Payment**: `Card -> Account (Merchant)`
  - **Refund**: `Account -> Card`
- **Analytics Engine**:
  1. **Refund Hubs**: Identifying merchants with excessive refund volume (High Out-Degree).
  2. **Channel Entropy**: Detecting cards using an abnormal variety of payment channels (High Entropy).
  3. **Cycle Detection**: Uncovering complex money loops (e.g., Circular Transactions).
  4. **Temporal Clustering**: Flagging rapid-fire transaction bursts.
- **Risk Scoring**: Weighted aggregation of all risk factors into a single `Risk Score`.

## Setup & usage

### Prerequisites
- Python 3.8+
- Dependencies: `pip install -r requirements.txt`

### Quick Start (Step-by-Step)

#### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

#### Step 2: Run the Pipeline
Execute the main analysis script to process transactions, build the graph, and calculate risk scores:
```bash
python src/main.py
```
This will:
- Load transaction and refund data from `data/raw/`
- Build the transaction graph
- Run all analytics modules (refund hubs, channel entropy, cycle detection, temporal patterns)
- Generate risk scores for all cards
- Output results to `data/processed/risk_scores.csv`

#### Step 3: View Results in Dashboard (Optional)
Launch the interactive Streamlit dashboard to explore risk patterns:
```bash
streamlit run src/dashboard.py
```
The dashboard will open in your browser at `http://localhost:8501` and provide:
- Interactive visualizations of the transaction graph
- Risk score rankings
- Detailed breakdowns by risk factor
- Real-time analytics

### Configuration
Adjust thresholds and weights in `config/config.yaml`:
- `config/config.yaml`: Main configuration (paths, thresholds, risk weights)
- `config/risk_weights.yaml`: Risk scoring weights for each factor

### Default Data Inputs
- `data/raw/transactions.csv`: Payment transaction records (card_id, account_id, amount, timestamp, channel)
- `data/raw/refunds.csv`: Refund records (card_id, refund_account, amount, timestamp)

### Outputs
- **Logs**: `logs/app.log` (detailed execution logs)
- **Risk Scores**: `data/processed/risk_scores.csv` (card risk rankings and breakdowns)
