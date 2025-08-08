
import superstoredata 
from fakestoreapi import get_products, simulate_inventory




def run_pipeline():
   
   
    order=superstoredata.orders
    returns=superstoredata.returns
    people=superstoredata.people


    products = get_products()
    inventory_df = simulate_inventory(products)

    order.to_csv("./data/orders.csv",index=False)
    returns.to_csv("./data/returns.csv",index=False)
    people.to_csv("./data/people.csv",index=False)
    inventory_df.to_csv("./data/inventory.csv",index=False)
   

    print("Pipeline execution completed.")

if __name__ == "__main__":
    run_pipeline()
