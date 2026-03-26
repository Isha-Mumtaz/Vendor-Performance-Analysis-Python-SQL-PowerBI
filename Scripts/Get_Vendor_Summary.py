import os
import pandas as pd
import sqlite3
import logging

os.chdir(r"C:\Users\AB\Documents\projects")

if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    query = """
    WITH PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber, p.VendorName, p.Brand,
            p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo AS VendorNumber,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNumber
        AND ps.Brand = ss.Brand
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    logging.info("Running vendor summary SQL...")
    df = pd.read_sql(query, conn)
    logging.info(f"Vendor summary generated: {df.shape[0]} rows")
    return df

def clean_data(df):
    df = df.copy()
    df['Volume'] = df['Volume'].astype(float)
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    logging.info("Data cleaning and KPI calculation done.")
    return df

def ingest_db(df, table_name, conn):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    logging.info(f"Table '{table_name}' saved to database.")

if __name__ == "__main__":
    conn = sqlite3.connect("inventory.db")

    logging.info("Creating vendor summary...")
    summary_df = create_vendor_summary(conn)

    logging.info("Cleaning data...")
    clean_df = clean_data(summary_df)

    # <-- SAVE CSV HERE
    csv_path = os.path.join(os.getcwd(), "vendor_summary.csv")
    clean_df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved at: {csv_path}")

    logging.info("Saving to database...")
    ingest_db(clean_df, "vendor_sales_summary", conn)

    logging.info("✅ Completed successfully")
    conn.close()
