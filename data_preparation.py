import os
import pandas as pd
from datetime import datetime
from random import randint
def data_prepare():
    input_file = "/Users/macos/Documents/Python/Project-CV/Application_of_sales_analysis/Datasource"
    output_file = "/Users/macos/Documents/Python/Project-CV/Application_of_sales_analysis/Prepared_Data"

# Extract: Merge data from multiple external sources
    print("1. merge data")
    all_files = os.listdir(f"{input_file}/")
    os.makedirs(output_file, exist_ok=True)
    list_file = [file for file in all_files]
    dataframe = [pd.read_csv(os.path.join(input_file,file)) for file in list_file]
    merge_data_df = pd.concat(dataframe,ignore_index=True)
    print("...merged successfull...")


# Transform: Clean data
    print("2. cleaning data")
    cleaned_data_df = merge_data_df.dropna(how="all").drop_duplicates().reset_index(drop=True)
    cleaned_data_df = cleaned_data_df[cleaned_data_df['Order ID'] != 'Order ID'].reset_index(drop=True) #check any header is contained in data.
    print("...cleaned successfull...")



    print("3. formatting the data")
    formatting_data_df = cleaned_data_df.drop("Purchase Address",axis=1)
    formatting_data_df = formatting_data_df.rename(columns = {
            "Product": "product",
            "Order ID": "sale_id",
            "Quantity Ordered": "quantity_sold",
            "Price Each": "each_price",
            "Order Date": "sale_date"
    })
    formatting_data_df[['sale_id','quantity_sold','each_price']] = formatting_data_df[['sale_id','quantity_sold','each_price']].astype({'sale_id':'int64','quantity_sold':'int64','each_price':'float64'})
    formatting_data_df['sale_date'] = pd.to_datetime(formatting_data_df['sale_date'])
    formatting_data_df['sales']= formatting_data_df['each_price'] * formatting_data_df['quantity_sold']
    formatting_data_df = formatting_data_df.sort_values(by='sale_date').reset_index(drop=True)
    print('...success formatting...')
    pd.set_option('display.max_columns', None)
    print(formatting_data_df.head(10).to_string())


# Load: saving to sinks
    print("4. Saving to csv")
    formatting_data_df.to_csv(f"{output_file}/processed_file.csv",index=False)
    print("--Saved to csv--")

    print("5. computing and setting stock quantity")
    stock_df = formatting_data_df.drop(['sale_id','each_price','sale_date','sales'],axis =1)
    stocks_df = stock_df.groupby(['product'], as_index=False)['quantity_sold'].sum()
    stocks_df['stock_quantity'] = randint(30000,35000)
    stocks_df.to_csv(f"{output_file}/stock_quantity.csv",index=False)
    print(stocks_df)

if __name__ == "__main__":
    data_prepare()