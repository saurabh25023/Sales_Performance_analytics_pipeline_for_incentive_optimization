import pandas as pd
import random
from datetime import datetime, timedelta


# Function to generate random transaction data
def generate_transaction_data(num_records):
    # Sample data
    customers = [f'Customer_{i}' for i in range(1, 101)]
    products = [f'Product_{i}' for i in range(1, 21)]

    data = []

    for _ in range(num_records):
        transaction_date = datetime.now() - timedelta(days=random.randint(1, 365))
        customer = random.choice(customers)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        price = round(random.uniform(10.0, 100.0), 2)
        total = round(price * quantity, 2)

        data.append({
            'TransactionDate': transaction_date.strftime('%Y-%m-%d'),
            'Customer': customer,
            'Product': product,
            'Quantity': quantity,
            'Price': price,
            'Total': total
        })

    return pd.DataFrame(data)


# Generate the data
num_records = 1000  # Change this to generate more or fewer records
transaction_data = generate_transaction_data(num_records)

# Save to CSV
transaction_data.to_csv('transaction_data.csv', index=False)

print('Transaction data generated and saved to transaction_data.csv')
