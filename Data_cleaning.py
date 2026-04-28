#|data cleaning strategies                              | Explanation                                                                                        |
#| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------|
#|   1. Remove duplicate sales documents                | Detects duplicate sales transactions by `DocumentNo`. You can then delete duplicates manually.     |
#|   2. Standardize date format                         | Converts inconsistent date strings into MongoDB ISODate objects for accurate time-based reporting. |
#|   3. Trim and uppercase codes (customers/products)   | Ensures product codes are consistently formatted (e.g., “lor124” → “LOR124”).                      |
#|   4. Handle missing values                           | Adds placeholders for missing fields to avoid null errors during aggregation.                      |
#|   5. Validate numeric fields                         | Identifies invalid or text-based numeric values.                                                   |
#|   6. Join reference data (brands/categories)         | Cleans relational-style inconsistencies by embedding or referencing brand/category details.        |
#|   7. Remove outliers or negative totals              | Removes invalid negative totals (e.g., data entry errors).                                         |
#|   8. Flatten nested structures                       | Prepares nested arrays for Power BI import.                                                        |

#Use your cmd or poweshell and connect to the database (Mongodb atlas)

from pymongo import MongoClient

# --- CONNECT TO DATABASE ---
#client = MongoClient("mongodb+srv://<username>:<password>@cluster0.mongodb.net/")
db = client["clearvue"]

# =====================================================
# 1) REMOVE DUPLICATES SAFELY
# =====================================================
def remove_duplicates(collection_name, unique_field):
    collection = db[collection_name]

    pipeline = [
        {"$match": {unique_field: {"$exists": True, "$ne": None}}},
        {"$group": {
            "_id": f"${unique_field}",
            "ids": {"$push": "$_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ]

    duplicates = list(collection.aggregate(pipeline))

    deleted_count = 0

    for doc in duplicates:
        ids = doc["ids"]
        ids.pop(0)   # keep first record
        result = collection.delete_many({"_id": {"$in": ids}})
        deleted_count += result.deleted_count

    print(f"Removed {deleted_count} duplicate records from {collection_name}")

# =====================================================
# 2) FIND NULL / MISSING VALUES
# =====================================================
def find_missing_values(collection_name, field):
    collection = db[collection_name]

    missing = list(collection.find({
        "$or": [
            {field: None},
            {field: {"$exists": False}}
        ]
    }))

    print(f"Found {len(missing)} missing values in {collection_name}.{field}")
    return missing;
  
# =====================================================
# 3) STANDARDIZE TEXT (UPPERCASE)
# =====================================================
def standardize_text(collection_name, field):
    collection = db[collection_name]

    docs = collection.find({field: {"$exists": True}})

    updated = 0
    for doc in docs:
        value = doc.get(field)
        if isinstance(value, str):
            new_value = value.strip().upper()
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {field: new_value}}
            )
            updated += 1

    print(f"Standardized {updated} records in {collection_name}.{field}")

# =====================================================
# 4) REMOVE NEGATIVE VALUES
# =====================================================
def remove_negative_values(collection_name, field):
    collection = db[collection_name]

    result = collection.delete_many({field: {"$lt": 0}})

    print(f"Removed {result.deleted_count} invalid negative records from {collection_name}")


# =====================================================
# 5) TRIM EXTRA SPACES
# =====================================================
def trim_spaces(collection_name, field):
    collection = db[collection_name]

    docs = collection.find({field: {"$exists": True}})
    updated = 0

    for doc in docs:
        value = doc.get(field)
        if isinstance(value, str):
            cleaned = value.strip()
            collection.update_one(
                {"_id": doc["_id"]},
                {"$set": {field: cleaned}}
            )
            updated += 1

    print(f"Trimmed spaces in {updated} records of {collection_name}.{field}")

# =====================================================
# RUN FUNCTIONS
# =====================================================

# remove_duplicates("sales_line", "DocumentNo")
# find_missing_values("customers", "CustomerID")
# standardize_text("products", "Category")
# remove_negative_values("sales_header", "Amount")
# trim_spaces("customers", "CustomerName")
