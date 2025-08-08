import requests
import pandas as pd
import random
import datetime

def get_products():
    response = requests.get('https://fakestoreapi.com/products')
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Failed to fetch product data")

def simulate_inventory(products, days=30):
    inventory_history = []
    start_date = datetime.datetime.now() - datetime.timedelta(days=days)

    for product in products:
        base_inventory = random.randint(20, 200)
        daily_demand = random.randint(1, 10)

        for day in range(days):
            current_date = start_date + datetime.timedelta(days=day)
            daily_sales = min(random.randint(0, daily_demand), base_inventory)
            base_inventory -= daily_sales

            if day % 7 == 0:
                restock_amount = random.randint(30, 100)
                base_inventory += restock_amount

            inventory_history.append({
                'product_id': product['id'],
                'product_name': product['title'],
                'date': current_date.strftime('%Y-%m-%d'),
                'inventory_level': base_inventory,
                'category': product['category'],
                'price': product['price'],
                'restock': 1 if day % 7 == 0 else 0
            })

    return pd.DataFrame(inventory_history)
