import pandas as pd

file_path = './data/Global Superstore.xls'

orders = pd.read_excel(file_path, sheet_name='Orders')
returns = pd.read_excel(file_path, sheet_name='Returns')
people = pd.read_excel(file_path, sheet_name='People')





print(f" Total orders: {len(orders)}")
print(f" Date range: {orders['Order Date'].min().date()} to {orders['Order Date'].max().date()}")
print(f" Product categories: {orders['Category'].unique()}")
print(f" Return rate: {len(returns) / len(orders):.2%}")
