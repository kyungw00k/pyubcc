# pyubcc
> Upbit Candle Collector for Python

## Description
Upbit Candle Collector is a Python script that collects historical candle data from the Upbit API and saves it to a SQLite3 DB or CSV file. It allows you to specify the market, time interval, and date range for the data collection.

## Quick Start
```
$ pip install pyubcc
$ ubcc BTC --timeframe day --days 30             

Starting data collection for BTC...
Period: 30 days, Timeframe: day

KRW-BTC: 30 candles [00:00, 116.88 candles/s]                                                                       
No missing candles found.

=== BTC Data Collection Results ===
Collection Period: 2025-01-18 09:00 ~ 2025-02-17 00:00
Timeframe: day (1440 minutes)
Collected Candles: 30
Data Gaps: 0
Timestamp Order Mismatches: 0

$ sqlite3 db/KRW-BTC_day.db "SELECT COUNT(*) FROM ohlcv;"
30
```

## Usage

### CLI
```
usage: ubcc [-h]
              [--timeframe {minute1,minute3,minute5,minute10,minute15,minute30,minute60,minute240,day,week,month}]
              [--days DAYS] [--db-path DB_PATH] [--export-csv] [--verbose]
              coin

Upbit Candle Collector

positional arguments:
  coin                  Coin symbol (e.g., BTC, ETH, DOGE) or full ticker (e.g., KRW-
                        BTC, USDT-BTC)

options:
  -h, --help            show this help message and exit
  --timeframe {minute1,minute3,minute5,minute10,minute15,minute30,minute60,minute240,day,week,month}
                        Time interval (default: day)
  --days DAYS           Collection period in days (default: 30)
  --db-path DB_PATH     DB file path (default: db/{coin}_{timeframe}.db)
  --export-csv          Export data to CSV file
  --verbose             Enable detailed logging
```

### Module
```python
from pyubcc import UpbitCandleCollector

# Initialize collector for BTC/KRW daily candles
collector = UpbitCandleCollector(
    coin='BTC',           # Coin symbol (e.g., BTC, ETH, DOGE)
    timeframe='day',      # Time interval (minute1 to month)
    fiat='KRW',          # Base currency (default: KRW)
    verbose=True         # Enable detailed logging
)

# Check database status
collector.check_db_status()

# Collect last 30 days of data
from datetime import datetime, timedelta
end_date = datetime.now()
start_date = end_date - timedelta(days=30)
results = collector.collect(start_date=start_date, end_date=end_date)

# Export collected data to CSV
collector.export_to_csv()

# Get data as pandas DataFrame
df = collector.get_ohlcv_data(start_date=start_date, end_date=end_date, filter_gaps=True)
print(df.head())
```

### Return Values
The `collect()` method returns a tuple containing:
- `total_count`: Number of collected candles
- `expected_candles`: Expected number of candles for the period
- `timestamp_order_mismatches`: Number of timestamp order mismatches
- `gaps`: List of gaps in the data

### Data Structure
The collected data includes:
- `timestamp`: Candle timestamp
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Trading volume

## Public API

### Constructor
```python
UpbitCandleCollector(coin, timeframe, fiat="KRW", db_path=None, verbose=False, show_progress=False)
```
- `coin` (str): Coin symbol (e.g., 'BTC', 'ETH', 'DOGE')
- `timeframe` (str): Time interval (minute1, minute3, minute5, minute10, minute15, minute30, minute60, minute240, day, week, month)
- `fiat` (str): Base currency (KRW, BTC, USDT, default: KRW)
- `db_path` (str, optional): DB file path (default: db/{coin}_{timeframe}_{fiat}.db)
- `verbose` (bool): Enable detailed logging
- `show_progress` (bool): Show progress bar (default: False)

### Methods

#### check_db_status()
Checks the database status and returns information about the first and last timestamps.
- Returns: bool - True if database contains data, False if empty

#### collect(start_date=None, end_date=None)
Collects historical data for the specified period.
- Parameters:
  - `start_date` (datetime, optional): Start date for data collection
  - `end_date` (datetime, optional): End date for data collection (default: current time)
- Returns: tuple (total_count, expected_candles, timestamp_order_mismatches, gaps)

#### get_ohlcv_data(start_date=None, end_date=None, filter_gaps=True)
Retrieves stored OHLCV data as a pandas DataFrame.
- Parameters:
  - `start_date` (datetime, optional): Start date for data retrieval
  - `end_date` (datetime, optional): End date for data retrieval
  - `filter_gaps` (bool): Whether to filter data gaps (default: True)
- Returns: pandas.DataFrame with OHLCV data

#### export_to_csv(start_date=None, end_date=None)
Exports OHLCV data to a CSV file.
- Parameters:
  - `start_date` (datetime, optional): Start date for data export
  - `end_date` (datetime, optional): End date for data export
- Returns: str - Path to the exported CSV file, or None if no data to export

#### analyze_gaps(start_date=None, end_date=None)
Analyzes gaps in candle data from database.
- Parameters:
  - `start_date` (datetime, optional): Start date for gap analysis
  - `end_date` (datetime, optional): End date for gap analysis
- Returns: list of dictionaries containing gap information

