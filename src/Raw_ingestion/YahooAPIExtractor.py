import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import adlfs
from azure.storage.blob import BlobServiceClient

container_name = "stockstoragebg1"
connection_string = 0
# Load stock symbols from a file (one symbol per line)
def load_symbols(file_path):
    with open(file_path, 'r') as f:
        symbols = [line.strip() for line in f if line.strip()]
    return symbols


def fetch_snapshots(symbols,flag):
    snapshots = []
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            if flag == "fund":
                info = dict(ticker.info)  # info is a quick snapshot dict
            elif flag == "pric":
                fi = ticker.fast_info
                info = {"symbol": symbol,
                "current_price": fi.get("lastPrice"),
                "open": fi.get("open"),
                "day_high": fi.get("dayHigh"),
                "day_low": fi.get("dayLow"),
                "previous_close": fi.get("previousClose"),
                "volume": fi.get("volume"),
                "market_cap": fi.get("marketCap")
                }
            else:
                raise ValueError("wrong flag")
            info['symbol'] = symbol
            snapshots.append(info)
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
    snapshots = pd.DataFrame(snapshots)
    snapshots['extract_time'] = datetime.utcnow().isoformat()
    snapshots['extract_date'] = pd.to_datetime(datetime.utcnow().date())
    snapshots['extract_time'] = (datetime.utcnow() + timedelta(days=2)).isoformat()
    snapshots['extract_date'] = pd.to_datetime(datetime.utcnow().date() + timedelta(days=2))


    snapshots['year'] = snapshots['extract_date'].dt.year
    snapshots['month'] = snapshots['extract_date'].dt.month
    snapshots['day'] = snapshots['extract_date'].dt.day

    snapshots['extract_date'] = snapshots['extract_date'].dt.date.astype(str)

    return snapshots

def sort_for_fundamentals(df):
    desired_cols = [
        "symbol", "shortName", "sector", "industry", "country", "currency", "fullTimeEmployees",
        "marketCap", "enterpriseValue", "totalRevenue", "netIncomeToCommon", "profitMargins",
        "revenueGrowth", "ebitda", "enterpriseToRevenue", "enterpriseToEbitda", "bookValue",
        "priceToBook", "trailingPE", "forwardPE", "trailingEps", "forwardEps", "returnOnAssets",
        "returnOnEquity", "earningsQuarterlyGrowth", "ipoExpectedDate", "extract_time", "extract_date", "year",
        "month", "day"
    ]

    existing_desired_cols = [col for col in desired_cols if col in df.columns]

    other_cols = [col for col in df.columns if col not in existing_desired_cols]

    final_col_order = existing_desired_cols + other_cols

    snapshots = df[final_col_order].reset_index(drop=True)

    return snapshots

def write_data_to_ADLS(df_stock,output_name,):
    try:
        fs = adlfs.AzureBlobFileSystem(
            connection_string=connection_string
        )

        with fs.open(f'{container_name}/dev/raw-data/{output_name}', 'w') as f:
            df_stock.to_csv(f,index=False)

        print("Data uploaded to ADLS Gen2!")

    except Exception as e:
        print(f"An error occurred: {e}")



if __name__ == "__main__":
    symbols_file = "Symbols.txt"   # Your file with list of symbols

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    if not container_client.exists():
        container_client.create_container()
        print(f"Created container '{container_name}'")

    flag = "pric"


    if flag == "pric":
        symbols = load_symbols(symbols_file)
        snapshots = fetch_snapshots(symbols,flag)
        output_name_pric = f"hourly_prices/{snapshots['year'].max()}/{snapshots['month'].max()}/prices_{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')}.csv"
        write_data_to_ADLS(snapshots, output_name_pric)
    elif flag == "fund":
        symbols = load_symbols(symbols_file)
        snapshots = fetch_snapshots(symbols,flag)
        snapshots = sort_for_fundamentals(snapshots)
        output_name_fund = f"daily_fundamentals/{snapshots['year'].max()}/{snapshots['month'].max()}/fundamentals_{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')}.csv"
        write_data_to_ADLS(snapshots,output_name_fund)


