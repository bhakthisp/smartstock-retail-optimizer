import mysql.connector
from datasets import load_dataset

# Loading dataset
print("Loading FreshRetailNet-50K dataset...")
dataset = load_dataset("Dingdong-Inc/FreshRetailNet-50K")
print("Dataset loaded!")

# Connecting to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",                 # MySQL username
    password="Bhakthi@13",        # MySQL password
    database="retail_inventory_db"  # schema name
)
cursor = conn.cursor()
print("Connected to MySQL.")

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS fresh_retail (
    city_id INT,
    store_id INT,
    management_group_id INT,
    first_category_id INT,
    second_category_id INT,
    third_category_id INT,
    product_id INT,
    dt DATETIME,
    sale_amount DOUBLE,
    hours_sale TEXT,
    stock_hour6_22_cnt INT,
    hours_stock_status TEXT,
    discount DOUBLE,
    holiday_flag INT,
    activity_flag INT,
    precpt DOUBLE,
    avg_temperature DOUBLE,
    avg_humidity DOUBLE,
    avg_wind_level DOUBLE
)
""")
print("Table ready.")

# Function to insert dataset in chunks
def insert_split(split_name, chunk_size=10000):
    total_rows = len(dataset[split_name])
    print(f"Inserting {split_name} split with {total_rows} rows...")

    for start in range(0, total_rows, chunk_size):
        chunk = dataset[split_name][start:start+chunk_size]  # slice dataset directly
        records = []
        for row in chunk:
            records.append((
                int(row["city_id"]), int(row["store_id"]), int(row["management_group_id"]),
                int(row["first_category_id"]), int(row["second_category_id"]),
                int(row["third_category_id"]), int(row["product_id"]), row["dt"],
                float(row["sale_amount"]), str(row["hours_sale"]),
                int(row["stock_hour6_22_cnt"]), str(row["hours_stock_status"]),
                float(row["discount"]), int(row["holiday_flag"]), int(row["activity_flag"]),
                float(row["precpt"]), float(row["avg_temperature"]),
                float(row["avg_humidity"]), float(row["avg_wind_level"])
            ))
        cursor.executemany("""
            INSERT INTO fresh_retail (
                city_id, store_id, management_group_id, first_category_id,
                second_category_id, third_category_id, product_id, dt,
                sale_amount, hours_sale, stock_hour6_22_cnt, hours_stock_status,
                discount, holiday_flag, activity_flag, precpt,
                avg_temperature, avg_humidity, avg_wind_level
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, records)
        conn.commit()
        print(f"Inserted rows {start+1} to {start+len(chunk)} from {split_name} split.")

# Insert train and eval splits
insert_split("train", chunk_size=10000)  # test with 10,000 rows per chunk
insert_split("eval", chunk_size=10000)

# Close connection
cursor.close()
conn.close()
print("All data inserted successfully!")

