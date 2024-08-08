#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('geolocation.csv', 'geolocation'),
    ('order_items.csv', 'order_items'),
    ('products.csv', 'products'),
    ('orders.csv', 'orders'),
    ('payments.csv', 'payments'),
    ('sellers.csv', 'sellers')# Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Manoj123@',
    database='ecommerce'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'C:/Users/HP/OneDrive/AppData/Desktop/excel project'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()


# In[84]:


import pandas as pd
import matplotlib.pyplot as plt 
import seaborn as sns
import mysql.connector
import numpy as np

db = mysql.connector.connect(host ="localhost",
                            username = "root",
                            password = "Manoj123@",
                            database = "ecommerce")

cur = db.cursor()


# # List all unique cities where customers are located.
# 

# In[39]:


query = """ select distinct customer_city from customers"""

cur.execute(query)

data = cur.fetchall()

data


# # Count the number of orders placed in 2017.

# In[11]:


query =""" select count(order_id) from orders where year(order_purchase_timestamp) = 2017"""

cur.execute(query)

data = cur.fetchall()

"Total count of orders are : " , data[0][0]


# # Find the total sales per category.

# In[53]:


query =""" select products.product_category , round(sum(payments.payment_value)) 
from products 
join  order_items  
on products.product_id = order_items.product_id
join payments 
on order_items.order_id = payments.order_id group by product_category"""

cur.execute(query)

data = cur.fetchall()
df=pd.DataFrame(data)

df


# # 4. Calculate the percentage of orders that were paid in installments.

# In[40]:


query = """ select (sum(case when payment_installments >=1 then 1 else 0 end))
/ count(order_id)*100 from payments"""

cur.execute(query)

data = cur.fetchall()

data


# # 5. Count the number of customers from each state. 

# In[41]:


# Close the current cursor
cur.close()

# Create a new cursor
cur = conn.cursor()

# Execute your new query
query = """SELECT customer_state, COUNT(customer_id) FROM customers 
           GROUP BY customer_state"""
cur.execute(query)
data = cur.fetchall()


# In[65]:



query = """SELECT customer_state, COUNT(customer_id) FROM customers 
           GROUP BY customer_state"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data)
df


# In[66]:


df = pd.DataFrame(data , columns = ["state", "customer_count" ])
df = df.sort_values(by="customer_count", ascending=False)
plt.figure(figsize =(8,3))

plt.bar(df["state"], df["customer_count"])
plt.xticks(rotation = 90)
plt.show()


# # Intermediate Queries
# # 1. Calculate the number of orders per month in 2018.

# In[35]:


query = """ select 
count(order_id) as num_of_orders, monthname(order_purchase_timestamp)as Months 
from orders where year(order_purchase_timestamp) = 2018 group by Months order by num_of_orders desc ;"""

cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data , columns = ["num_of_orders" , "Months"])
plt.figure(figsize =(8,5))
plt.xticks(rotation=45)

ax=sns.barplot(x=df["Months"], y=df["num_of_orders"])
ax.bar_label(ax.containers[0])
plt.title("Numbers of Orders in a Month")
plt.show()


# # 2. Find the average number of products per order, grouped by customer city.

# In[78]:


query = """ with count_per_order as (select orders.order_id , orders.customer_id  , count(orders.order_id) as count from orders join 
order_items on orders.order_id = order_items.order_id group by orders.customer_id , orders.order_id) 
select customers.customer_city, round(avg( count_per_order.count) ,2) average_orders from customers
join count_per_order
on customers.customer_id = count_per_order.customer_id group by customers.customer_city"""

cur.execute(query)
data = cur.fetchall()
df=pd.DataFrame(data , columns = ["customer city" , "Average"])
df


# # 3. Calculate the percentage of total revenue contributed by each product category.

# In[81]:


query = """ select (products.product_category) category , round((sum(payments.payment_value)/(select sum(payment_value) from payments ))*100,2 ) sales_percentage 
from products 
join  order_items  
on products.product_id = order_items.product_id
join payments 
on order_items.order_id = payments.order_id group by product_category order by sales_percentage desc;"""

cur.execute(query)
data = cur.fetchall()
df=pd.DataFrame(data , columns = ["Category" , "sales Percentage"])
df.head(20)


# # 4. Identify the correlation between product price and the number of times a product has been purchased.

# In[90]:


query = """select products.product_category , count(order_items.order_id) , avg(order_items.price)  from products  
join order_items on products.product_id = order_items.product_id group by products.product_category  ; """

cur.execute(query)
data = cur.fetchall()
df=pd.DataFrame(data  , columns =[" Products ", "count of orders" , "Average"]) 

arr1 = df["count of orders"]
arr2 = df["Average"]

a= np.corrcoef([arr1,arr2])
print(" The correlation between product price and the number of times a product has been purchased is" , a[0,1])


# # 5. Calculate the total revenue generated by each seller, and rank them by revenue.

# In[109]:


query = """select * , dense_rank() over (order by Revenue desc) rnk from ( select (order_items.seller_id) a , sum(payments.payment_value) as Revenue 
from order_items join payments on order_items.order_id = payments.order_id group by a) as b """

cur.execute(query)
data = cur.fetchall()
df =pd.DataFrame(data, columns = ["Seller_id" , "Revenue" , "Rank"])
df.head(3)

sns.barplot(x = "Rank" , y = "Revenue" , data = df)

plt.show()


# In[ ]:




