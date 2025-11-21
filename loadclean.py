import mysql.connector
import pandas as pd

print("Connecting to MySQL...")

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Bhakthi@13",
    database="smartstock"
)

query = "SELECT * FROM retail_data"
df = pd.read_sql(query, conn)

conn.close()
print("Data loaded from MySQL:", df.shape)

# Convert date column
df["dt"] = pd.to_datetime(df["dt"], errors="coerce")

# Drop rows with invalid dates
df = df.dropna(subset=["dt"])

# Feature: extract year, month, day
df["year"] = df["dt"].dt.year
df["month"] = df["dt"].dt.month
df["day"] = df["dt"].dt.day
df["day_of_week"] = df["dt"].dt.dayofweek
df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

# Feature: previous day's sales (lag)
df = df.sort_values(["store_id", "product_id", "dt"])
df["sales_lag_1"] = df.groupby(["store_id", "product_id"])["sale_amount"].shift(1)

# Feature: 7-day moving average
df["sales_ma_7"] = df.groupby(
    ["store_id", "product_id"]
)["sale_amount"].transform(lambda x: x.rolling(7).mean())

# FIX: Drop NaN ONLY for the new feature columns
df = df.dropna(subset=["sales_lag_1", "sales_ma_7"])

# Save cleaned file
df.to_csv("cleaned_retail_data.csv", index=False)

print("Preprocessing complete!")
print("Saved: cleaned_retail_data.csv")
