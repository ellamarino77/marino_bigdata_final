import os
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import json
from dotenv import load_dotenv

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["bigdata_project"]

def prepare_and_load_data():
    df = pd.read_csv("data/Online Retail.csv", encoding="ISO-8859-1")
    print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns.")

    data = df.to_dict(orient="records")
    with open("data/raw_data.json", "w") as f:
        json.dump(data, f, indent=4)
    print("Saved raw_data.json for reference.")

    db.raw_data.delete_many({})  
    db.raw_data.insert_many(data)
    print(f"Inserted {db.raw_data.count_documents({})} documents into MongoDB.")


def show_bronze_stats():
    count = db.raw_data.count_documents({})
    sample = db.raw_data.find_one()
    print(f"Rows: {count}")
    print(f"Columns: {len(sample.keys())}")


def clean_data():
    pipeline = [
        {"$match": {"UnitPrice": {"$ne": None}, "Quantity": {"$ne": None}}},
        {"$out": "clean_data"}
    ]
    db.raw_data.aggregate(pipeline)
    print("Clean data saved to clean_data collection.")


def aggregate_top_countries():
    pipeline = [
        {"$group": {"_id": "$Country", "total_sales": {"$sum": {"$multiply": ["$UnitPrice", "$Quantity"]}}}},
        {"$sort": {"total_sales": -1}},
        {"$limit": 10},
        {"$out": "gold_top_countries"}
    ]
    db.clean_data.aggregate(pipeline)
    print("Aggregated data saved to gold_top_countries collection.")

def aggregate_top_products():
    pipeline = [
        {"$group": {"_id": "$Description", "total_quantity": {"$sum": "$Quantity"}}},
        {"$sort": {"total_quantity": -1}},
        {"$limit": 10},
        {"$out": "gold_top_products"}
    ]
    db.clean_data.aggregate(pipeline)
    print("Aggregated data saved to gold_top_products collection.")

def aggregate_sales_by_month():
    pipeline = [
        {
            "$addFields": {
                "invoice_month": {
                    "$dateToString": {"format": "%Y-%m", "date": {"$toDate": "$InvoiceDate"}}
                }
            }
        },
        {
            "$group": {
                "_id": "$invoice_month",
                "total_sales": {"$sum": {"$multiply": ["$UnitPrice", "$Quantity"]}}
            }
        },
        {"$sort": {"_id": 1}},
        {"$out": "gold_sales_by_month"}
    ]
    db.clean_data.aggregate(pipeline)
    print("Aggregated data saved to gold_sales_by_month collection.")


def plot_top_countries():
    data = list(db.gold_top_countries.find())
    df = pd.DataFrame(data)
    plt.figure(figsize=(10,6))
    plt.bar(df['_id'], df['total_sales'])
    plt.xlabel("Country")
    plt.ylabel("Total Sales")
    plt.title("Top 10 Countries by Sales")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_top_products():
    data = list(db.gold_top_products.find())
    df = pd.DataFrame(data)
    plt.figure(figsize=(10,6))
    plt.bar(df['_id'], df['total_quantity'])
    plt.xlabel("Product")
    plt.ylabel("Total Quantity Sold")
    plt.title("Top 10 Products by Quantity Sold")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_sales_by_month():
    data = list(db.gold_sales_by_month.find())
    df = pd.DataFrame(data)

    plt.figure(figsize=(12,6))
    plt.plot(df['_id'], df['total_sales'], marker='o')
    plt.xlabel("Month")
    plt.ylabel("Total Sales")
    plt.title("Sales Trend by Month")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Uncomment to load raw data (run once only)
    # prepare_and_load_data()

    show_bronze_stats()
    clean_data()
    aggregate_top_countries()
    aggregate_top_products()
    aggregate_sales_by_month()

    plot_top_countries()
    plot_top_products()
    plot_sales_by_month()