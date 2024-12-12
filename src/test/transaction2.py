import pandas as pd
import random
from datetime import datetime, timedelta

# Function to generate random transaction data
def generate_transaction_data(num_transactions):
    account_numbers = [f'ACC{random.randint(100000, 999999)}' for _ in range(num_transactions)]
    pay_modes = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash']
    timestamps = [datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23),
                        minutes=random.randint(0, 59), seconds=random.randint(0, 59)) for _ in range(num_transactions)]
    amounts = [round(random.uniform(10.00, 500.00), 2) for _ in range(num_transactions)]
    transaction_ids = [f'TID{1000 + i}' for i in range(num_transactions)]

    # Creating the DataFrame
    transaction_data = pd.DataFrame({
        'Transaction ID': transaction_ids,
        'Account Number': account_numbers,
        'Pay Mode': [random.choice(pay_modes) for _ in range(num_transactions)],
        'Timestamp': timestamps,
        'Amount': amounts
    })

    return transaction_data

# Generate 1000 transactions
num_records = 1000
transaction_df = generate_transaction_data(num_records)

# Save the DataFrame to a CSV file
csv_file_path = 'transaction_data5.csv'
transaction_df.to_csv(csv_file_path, index=False)

print(f"CSV file with {num_records} records created: {csv_file_path}")
