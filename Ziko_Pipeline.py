#importing Necessary Libraries
import pandas as pd
import os
import io
from azure.storage.blob import BlobServiceClient,BlobClient
from dotenv import load_dotenv

#Extraction Layer
ziko_df = pd.read_csv('ziko_logistics_data.csv')

#Data Cleaning and Transformation
ziko_df.fillna({
    'Unit_Price': ziko_df['Unit_Price'].mean(),
    'Total_Cost': ziko_df['Total_Cost'].mean(),
    'Discount_Rate': 0.0,
    'Return_Reason': 'Unknown'
}, inplace= True)

#Changing datatype of date from object to datetime
ziko_df['Date'] = pd.to_datetime(ziko_df['Date'])

#Customer table
customers = ziko_df[['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates().reset_index(drop= True)
customers.head()

#Product Table
products = ziko_df[['Product_ID','Quantity','Unit_Price', 'Discount_Rate','Product_List_Title']].copy().drop_duplicates().reset_index(drop= True)
products.head()

#Transaction fact table
transaction_fact = ziko_df.merge(customers,on =['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address'],how ='left')\
                          .merge(products, on=['Product_ID','Quantity','Unit_Price', 'Discount_Rate','Product_List_Title'],how ='left')\
                          [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID','Total_Cost','Sales_Channel','Order_Priority', 'Warehouse_Code', \
                            'Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Payment_Type', 'Taxable', 'Region', 'Country']]


#Temporal Loaading as csv
customers.to_csv('dataset//customers.csv', index= False)
products.to_csv('dataset//products.csv', index= False)
transaction_fact.to_csv('dataset//transaction_fact.csv', index= False)

print('Files loaded temporaly into local machine')

#Setting up Azure blob connection using .env file
load_dotenv()
connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

container_name = os.getenv('CONTAINER_NAME')
container_client = blob_service_client.get_container_client(container_name)


#create a function to load into Azure blob storage as parquet file
def upload_df_to_blob_as_parquet(df,container_client,blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index = False)
    buffer.seek(0)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer,blob_type = "BlockBlob", overwrite = True)
    print(f'{blob_name} uploaded to  Blob Storage succesfully')

upload_df_to_blob_as_parquet(customers,container_client, 'rawdata/customers.parquet')
upload_df_to_blob_as_parquet(products,container_client, 'rawdata/products.parquet')
upload_df_to_blob_as_parquet(transaction_fact,container_client, 'rawdata/transaction_fact.parquet')
