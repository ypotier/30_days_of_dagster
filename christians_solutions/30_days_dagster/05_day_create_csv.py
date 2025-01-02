# Script to create sample data (save as create_data.py in parent directory)
import pandas as pd
import os

# Create data directory in parent folder
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(parent_dir, 'data')
os.makedirs(data_dir, exist_ok=True)

# Sample coffee shop orders
orders_data = {
    'order_id': range(1, 101),
    'drink': ['Latte', 'Espresso', 'Cappuccino', 'Americano', 'Mocha'] * 20,
    'size': ['Extra Small', 'Small', 'Medium', 'Large', 'Extra Large'] * 20,
    'price': [4.50, 3.00, 4.00, 3.50, 4.75] * 20
}

# Save to parent directory's data folder
df = pd.DataFrame(orders_data)
df.to_csv(os.path.join(data_dir, 'orders.csv'), index=False)