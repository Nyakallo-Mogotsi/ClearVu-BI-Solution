|   Report                                         |   Purpose                                                              |
| ------------------------------------------------ | ---------------------------------------------------------------------- |
|   1. Daily Sales Report                          | Aggregates total sales per region per day.                             |
|   2. Monthly Sales by Financial Period           | Supports ClearVue’s custom financial-month reporting.                  |
|   3. Quarterly Trend Analysis                    | Summarizes quarterly totals for Power BI visualization.                |
|   4. Top Products by Category                    | Finds best-selling product categories.                                 |
|   5. Customer Performance (B2B vs B2C)           | Segments revenue by customer type.                                     |
|   6. Late Payments Analysis (for Kafka alerts)   | Identifies overdue payments; Kafka can stream these into alert topics. |
|   7. Supplier-ready structure (future use)       | Builds a hierarchical supplier → products view.                        |
|   8. Real-time Payment Stream (Kafka producer)   | Python/Kafka example:  `python producer.send("payments_stream", value=json.dumps(payment_doc)) `| Streams new/updated payments into Kafka for live dashboards.           |
|   9. BI Integration for Power BI                 | Use the MongoDB connector or export aggregation pipeline to a collection:  `js db.sales.aggregate([... , { $out: "bi_sales_summary" }]) `| Creates a Power BI-ready summarized dataset.                           |
                                                                                                                            
from pymongo import MongoClient
from datetime import datetime

# --- CONNECT TO DATABASE ---
#client = MongoClient("mongodb+srv://<username>:<password>@cluster0.mongodb.net/")
db = client["clearvue"]

# =====================================================
# 1) TOTAL SALES BY PRODUCT
# =====================================================
def total_sales_by_product():
    pipeline = [
        {
            "$group": {
                "_id": "$ProductID",
                "TotalSales": {"$sum": "$LineAmount"},
                "UnitsSold": {"$sum": "$Quantity"}
            }
        },
        {"$sort": {"TotalSales": -1}}
    ]

    result = list(db["sales_line"].aggregate(pipeline))
    print("\n1) TOTAL SALES BY PRODUCT")
    for r in result:
        print(r)


# =====================================================
# 2) MONTHLY SALES TREND
# =====================================================
def monthly_sales_trend():
    pipeline = [
        {
            "$group": {
                "_id": {"$substr": ["$PostingDate", 0, 7]},
                "MonthlySales": {"$sum": "$Amount"}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    result = list(db["sales_header"].aggregate(pipeline))
    print("\n2) MONTHLY SALES TREND")
    for r in result:
        print(r)


# =====================================================
# 3) TOP 10 CUSTOMERS
# =====================================================
def top_customers():
    pipeline = [
        {
            "$group": {
                "_id": "$CustomerID",
                "TotalSpent": {"$sum": "$Amount"}
            }
        },
        {"$sort": {"TotalSpent": -1}},
        {"$limit": 10}
    ]

    result = list(db["sales_header"].aggregate(pipeline))
    print("\n3) TOP 10 CUSTOMERS")
    for r in result:
        print(r)


# =====================================================
# 4) LATE PAYMENTS REPORT
# =====================================================
def late_payments_report():
    result = list(db["late_payments"].find({}))

    print("\n4) LATE PAYMENTS REPORT")
    for r in result:
        print(r)


# =====================================================
# 5) CUSTOMER PERFORMANCE REPORT
# =====================================================
def customer_performance():
    result = list(db["customer_performance"].find({}))

    print("\n5) CUSTOMER PERFORMANCE REPORT")
    for r in result:
        print(r)


# =====================================================
# 6) PRODUCT PERFORMANCE REPORT
# =====================================================
def product_performance():
    pipeline = [
        {
            "$group": {
                "_id": "$ProductID",
                "Revenue": {"$sum": "$LineAmount"},
                "UnitsSold": {"$sum": "$Quantity"}
            }
        },
        {"$sort": {"Revenue": -1}}
    ]

    result = list(db["sales_line"].aggregate(pipeline))
    print("\n6) PRODUCT PERFORMANCE REPORT")
    for r in result:
        print(r)


# =====================================================
# 7) PAYMENT STATUS SUMMARY
# =====================================================
def payment_status_summary():
    pipeline = [
        {
            "$group": {
                "_id": "$Status",
                "Count": {"$sum": 1},
                "TotalAmount": {"$sum": "$Amount"}
            }
        }
    ]

    result = list(db["payment_lines"].aggregate(pipeline))
    print("\n7) PAYMENT STATUS SUMMARY")
    for r in result:
        print(r)


# =====================================================
# 8) SALES BY REGION / CUSTOMER GROUP
# =====================================================
def sales_by_region():
    pipeline = [
        {
            "$group": {
                "_id": "$Region",
                "Sales": {"$sum": "$Amount"}
            }
        },
        {"$sort": {"Sales": -1}}
    ]

    result = list(db["sales_header"].aggregate(pipeline))
    print("\n8) SALES BY REGION")
    for r in result:
        print(r)


# =====================================================
# 9) ANOMALY DETECTION REPORT
# =====================================================
def anomaly_detection():
    result = list(db["sales_header"].find({
        "$or": [
            {"Amount": {"$lt": 0}},
            {"Amount": {"$gt": 1000000}}
        ]
    }))

    print("\n9) ANOMALY DETECTION REPORT")
    for r in result:
        print(r)


# =====================================================
# RUN ALL REPORTS
# =====================================================
if __name__ == "__main__":
    total_sales_by_product()
    monthly_sales_trend()
    top_customers()
    late_payments_report()
    customer_performance()
    product_performance()
    payment_status_summary()
    sales_by_region()
    anomaly_detection()                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
                                                                                                                             
