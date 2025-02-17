import sqlite3
import pyupbit
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import logging
import os
import argparse
from tqdm import tqdm

# SQLite datetime adapter registration for Python 3.12 compatibility
def adapt_datetime(val):
    return val.isoformat()

def convert_datetime(val):
    return datetime.fromisoformat(val)

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class UpbitCandleCollector:
    # 시간 단위별 간격(분) 설정
    timeframe_minutes = {
        'minute1': 1,
        'minute3': 3,
        'minute5': 5,
        'minute10': 10,
        'minute15': 15,
        'minute30': 30,
        'minute60': 60,
        'minute240': 240,
        'day': 1440,
        'week': 10080,
        'month': 43200
    }

    def __init__(self, coin, timeframe, fiat="KRW", db_path=None, verbose=False, show_progress=False):
        """
        Parameters:
        - coin: Coin symbol (e.g., 'BTC', 'ETH', 'DOGE')
        - timeframe: Time interval (minute1, minute3, minute5, minute10, minute15, minute30, minute60, minute240, day, week, month)
        - fiat: Base currency (KRW, BTC, USDT, default: KRW)
        - db_path: DB file path (default: db/{coin}_{timeframe}_{fiat}.db)
        - verbose: Enable detailed logging
        - show_progress: Show progress bar (default: False)
        """
        self.coin = coin.upper()
        self.fiat = fiat.upper()
        self.timeframe = timeframe
        self.ticker = f"{self.fiat}-{self.coin}"
        self.verbose = verbose
        self.show_progress = show_progress and not verbose  # Progress bar is not shown in verbose mode
        
        # Initialize interval
        self.interval = self.timeframe_minutes.get(self.timeframe, 1)
        
        # Set DB file path
        if db_path:
            self.db_path = db_path
        else:
            os.makedirs('db', exist_ok=True)  # Create db directory
            self.db_path = os.path.join('db', f"{self.ticker}_{timeframe}.db")
        
        # Initialize logging (first priority)
        self._setup_logging()
                
        # Initialize DB
        self.initialize_db()
    
    def _setup_logging(self):
        """Configure logging settings"""
        self.logger = logging.getLogger(f"pyubcc.{self.ticker}")
        self.logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

    def initialize_db(self):
        """Initialize the database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ohlcv (
                    timestamp DATETIME PRIMARY KEY,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL
                ) WITHOUT ROWID
            ''')
            conn.commit()

    def _get_last_timestamp(self):
        """Query the timestamp of the last saved data"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM ohlcv')
            min_date, max_date = cursor.fetchone()
            return min_date, max_date

    def check_db_status(self):
        """Check database status"""
        min_date, max_date = self._get_last_timestamp()
        
        if min_date and max_date:
            self.logger.info("Database Status:")
            self.logger.info(f"- Start Date: {min_date}")
            self.logger.info(f"- End Date: {max_date}")
            return True
        else:
            self.logger.info("Database is empty.")
            return False

    def collect(self, start_date=None, end_date=None):
        """Collect historical data and perform data analysis
        
        Parameters:
        - start_date: Start date for data collection
        - end_date: End date for data collection (default: current time)
        
        Returns:
        - tuple: (total_count, expected_candles, timestamp_order_mismatches, gaps)
            - total_count: Number of collected candles
            - expected_candles: Expected number of candles for the period
            - timestamp_order_mismatches: Number of timestamp order mismatches
            - gaps: List of gaps in the data
        """
        self.end_date = end_date if end_date else datetime.now()
        
        # Set start_date to 09:00 KST for the specified day if not provided
        if start_date:
            self.start_date = start_date
        else:
            self.start_date = (self.end_date - timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        if self.verbose:
            self.logger.info(f"Starting data collection: {self.start_date.strftime('%Y-%m-%d')} ~ {self.end_date.strftime('%Y-%m-%d')}")
        
        # Check existing data
        min_date, max_date = self._get_last_timestamp()
        if min_date and max_date:
            if self.verbose:
                self.logger.debug(f"Found existing data: {min_date} ~ {max_date}")
            
            # If existing data starts later than requested start date, collect from requested start date
            if pd.Timestamp(min_date) > pd.Timestamp(self.start_date):
                if self.verbose:
                    self.logger.info(f"Existing data start date ({min_date}) is later than requested start date ({self.start_date}).")
                    self.logger.info("Collecting data from the requested start date.")
            else:
                # Collect only from the last saved timestamp
                self.start_date = pd.Timestamp(max_date)
                if self.verbose:
                    self.logger.info(f"Resuming collection from last saved timestamp ({max_date}).")
        elif self.verbose:
            self.logger.info("Starting new data collection.")
        
        # Calculate expected number of candles
        total_candles = self._calculate_total_candles(self.start_date, self.end_date)
        collected_candles = 0
        
        # 시작일과 종료일을 timeframe 간격에 맞춰 보정
        adjusted_start = self.start_date
        adjusted_end = self.end_date
        
        # 시작일 보정 (09:00 이전이면 09:00으로 조정)
        if adjusted_start.hour < 9:
            adjusted_start = adjusted_start.replace(hour=9, minute=0, second=0, microsecond=0)
        
        # 시작일을 timeframe 간격에 맞춰 보정
        minutes_from_nine = (adjusted_start.hour - 9) * 60 + adjusted_start.minute
        if minutes_from_nine % self.interval != 0:
            # 이전 간격으로 내림 조정
            minutes_to_subtract = minutes_from_nine % self.interval
            adjusted_start = adjusted_start - timedelta(minutes=minutes_to_subtract)
        # 초와 마이크로초를 0으로 설정
        adjusted_start = adjusted_start.replace(second=0, microsecond=0)
        
        # 종료일을 timeframe 간격에 맞춰 보정 (이전 캔들 종료 시간으로)
        minutes_total = (adjusted_end.hour * 60 + adjusted_end.minute)
        if minutes_total % self.interval != 0:
            # 이전 간격으로 내림 조정
            minutes_to_subtract = minutes_total % self.interval
            adjusted_end = adjusted_end - timedelta(minutes=minutes_to_subtract)
        # 초와 마이크로초를 0으로 설정
        adjusted_end = adjusted_end.replace(second=0, microsecond=0)
        
        if self.verbose:
            self.logger.debug(f"Collection period (before adjustment): {self.start_date} ~ {self.end_date}")
            self.logger.debug(f"Collection period (after adjustment): {adjusted_start} ~ {adjusted_end}")
            self.logger.debug(f"Expected candles: {total_candles}")
        
        with sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as conn:
            current_date = self.end_date  # 최신 데이터부터 시작
            pbar = None

            while current_date > self.start_date:
                try:
                    # Calculate the number of candles needed based on time difference
                    time_diff_minutes = int((current_date - self.start_date).total_seconds() / 60)
                    needed_candles = min(200, time_diff_minutes // self.interval)
                    count = max(1, needed_candles)  # Ensure at least 1 candle is requested
                    
                    df = pyupbit.get_ohlcv(self.ticker, interval=self.timeframe,
                                          to=current_date - timedelta(hours=9),
                                          count=count)
                    
                    if df is not None and not df.empty:
                        # Sort DataFrame by timestamp to ensure sequential processing
                        df = df.sort_index()
                        
                        # Get the actual time range of received data
                        actual_start = df.index[0]
                        actual_end = df.index[-1]
                        
                        saved_count = self._save_dataframe_to_db(df, conn)
                        collected_candles += saved_count
                        
                        # Update current_date for the next iteration
                        current_date = actual_start
                        
                        # Initialize progress bar if not exists and show_progress is True
                        if pbar is None and self.show_progress:
                            pbar = tqdm(total=total_candles, desc=f"{self.ticker}", unit=" candles")
                            # Initialize collected_candles counter for gap detection
                            collected_candles = 0
                        
                        # Update progress bar if it exists
                        if pbar is not None:
                            # Update total if we detect gaps
                            if saved_count < count:
                                gap_size = count - saved_count
                                pbar.total -= gap_size
                                pbar.refresh()
                            pbar.update(saved_count)
                        
                        if self.verbose:
                            self.logger.debug(f"Collection period: {actual_start} ~ {actual_end}")
                    else:
                        # 데이터가 없는 경우 이전 구간으로 이동
                        current_date -= timedelta(minutes=200 * self.interval)
                    
                    time.sleep(0.1)  # API 호출 제한 방지
                    
                except Exception as e:
                    if self.verbose:
                        self.logger.error(f"Error occurred during data collection: {str(e)}")
                    raise
                
            if pbar is not None:
                pbar.close()
        
        if self.verbose:
            self.logger.info("Collection completed")
        
        # Perform data verification and gap analysis
        total_count, timestamp_order_mismatches = self.verify_data(start_date=adjusted_start, end_date=adjusted_end)
        gaps = self.analyze_gaps(start_date=adjusted_start, end_date=adjusted_end)
        expected_candles = self._calculate_total_candles(adjusted_start, adjusted_end)
        
        return total_count, expected_candles, timestamp_order_mismatches, gaps

    def calculate_minutes_between(self, start_date, end_date):
        """Calculate the exact number of minutes between two timestamps"""
        # Calculate time difference between start and end dates
        time_diff = end_date - start_date
        
        # Convert total seconds to minutes (considering microseconds)
        total_minutes = time_diff.total_seconds() / 60
        
        return total_minutes

    def _calculate_total_candles(self, start_date, end_date):
        """Calculate total number of candles needed"""
        # Adjust start time to 09:00 if earlier
        if start_date.hour < 9:
            start_date = start_date.replace(hour=9, minute=0, second=0, microsecond=0)
        
        # Adjust start date to match timeframe interval
        minutes_from_nine = (start_date.hour - 9) * 60 + start_date.minute
        if minutes_from_nine % self.interval != 0:
            # Adjust to next candle start time
            minutes_to_add = self.interval - (minutes_from_nine % self.interval)
            start_date = start_date + timedelta(minutes=minutes_to_add)
        
        # Adjust end date to match timeframe interval
        minutes_total = (end_date.hour * 60 + end_date.minute)
        if minutes_total % self.interval != 0:
            # Adjust to previous candle end time
            minutes_to_subtract = minutes_total % self.interval
            end_date = end_date - timedelta(minutes=minutes_to_subtract)
        
        # Calculate total minutes between start and end dates (24-hour continuous trading)
        total_minutes = int(self.calculate_minutes_between(start_date, end_date))
        
        # Calculate number of candles by dividing by interval
        return total_minutes // self.interval

    def _save_dataframe_to_db(self, df, conn):
        """Save DataFrame to SQLite database
        
        Parameters:
        - df: DataFrame containing OHLCV data
        - conn: SQLite connection object
        
        Returns:
        - int: Number of records saved
        """
        cursor = conn.cursor()
        saved_count = 0
        
        try:
            # Prepare data for insertion
            data = [
                (index.to_pydatetime(), row['open'], row['high'],
                 row['low'], row['close'], row['volume'])
                for index, row in df.iterrows()
            ]
            
            # Insert data using REPLACE to handle duplicates
            cursor.executemany(
                'INSERT OR REPLACE INTO ohlcv (timestamp, open, high, low, close, volume) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                data
            )
            
            # Commit changes
            conn.commit()
            saved_count = len(data)
            
            if self.verbose:
                self.logger.debug(f"Saved {saved_count} records to database")
                
        except Exception as e:
            if self.verbose:
                self.logger.error(f"Error saving data to database: {str(e)}")
            conn.rollback()
            raise
        
        return saved_count

    def verify_data(self, start_date=None, end_date=None):
        """Verify collected data
        
        Parameters:
        - start_date: Start date for verification (default: None, entire period)
        - end_date: End date for verification (default: None, entire period)
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create time conditions
            time_conditions = []
            if start_date:
                time_conditions.append(f"timestamp >= '{start_date}'")
            if end_date:
                time_conditions.append(f"timestamp <= '{end_date}'")
            time_where = f"WHERE {' AND '.join(time_conditions)}" if time_conditions else ""
            
            # Get total count
            cursor.execute(f'SELECT COUNT(*) FROM ohlcv {time_where}')
            total_count = cursor.fetchone()[0]
            
            # Verify timestamp order
            cursor.execute(f'''
                WITH ordered_timestamps AS (
                    SELECT timestamp, 
                           LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                    FROM ohlcv
                    {time_where}
                )
                SELECT COUNT(*) 
                FROM ordered_timestamps
                WHERE prev_timestamp >= timestamp
            ''')
            timestamp_order_mismatches = cursor.fetchone()[0]
            
            return total_count, timestamp_order_mismatches

    def get_ohlcv_data(self, start_date=None, end_date=None, filter_gaps=True):
        """Retrieve stored OHLCV data
        
        Parameters:
        - start_date: Start date for data retrieval
        - end_date: End date for data retrieval
        - filter_gaps: Whether to filter data gaps (default: True)
        """
        with sqlite3.connect(self.db_path) as conn:
            query = 'SELECT * FROM ohlcv'
            conditions = []
            
            if start_date:
                conditions.append(f"timestamp >= '{start_date}'")
            if end_date:
                conditions.append(f"timestamp <= '{end_date}'")
            
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
            
            query += ' ORDER BY timestamp'
            
            # Convert to DataFrame
            df = pd.read_sql_query(query, conn, index_col='timestamp', 
                                 parse_dates=['timestamp'])
            
            if filter_gaps and not df.empty:
                # Calculate time differences between consecutive timestamps
                time_diff = df.index.to_series().diff()
                expected_diff = pd.Timedelta(minutes=self.interval)
                
                # Filter only data with normal intervals
                valid_indices = time_diff == expected_diff
                # Set first record to True as diff calculation results in NaT
                valid_indices.iloc[0] = True
                
                # Filter data with gaps
                df = df[valid_indices]
            
            return df

    def export_to_csv(self, start_date=None, end_date=None):
        """Export OHLCV data to CSV file"""
        # Set CSV file path
        os.makedirs('csv', exist_ok=True)
        csv_filename = f"csv/{self.ticker}_{self.timeframe}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        # Retrieve data
        df = self.get_ohlcv_data(start_date, end_date)
        
        if df.empty:
            self.logger.warning("No data to export.")
            return None
            
        # Save to CSV file
        df.to_csv(csv_filename)
        self.logger.info(f"CSV file saved: {csv_filename}")
        return csv_filename

    def analyze_gaps(self, start_date=None, end_date=None):
        """Analyze gaps in candle data from database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get all timestamps in chronological order
            query = 'SELECT timestamp FROM ohlcv'
            conditions = []
            if start_date:
                conditions.append(f"timestamp >= '{start_date}'")
            if end_date:
                conditions.append(f"timestamp <= '{end_date}'")
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
            query += ' ORDER BY timestamp'
            
            cursor.execute(query)
            timestamps = [datetime.fromisoformat(row[0]) if isinstance(row[0], str) else row[0] for row in cursor.fetchall()]
            
            if not timestamps:
                self.logger.info("Database is empty.")
                return []
            
            gaps = []
            expected_interval = timedelta(minutes=self.interval)
            missing_timestamps = []
            
            for i in range(len(timestamps) - 1):
                current = timestamps[i]
                next_time = timestamps[i + 1]
                actual_interval = next_time - current
                
                # Treat as GAP if actual interval is larger than expected
                if actual_interval > expected_interval:
                    # Calculate number of missing candles
                    missing_count = int((actual_interval.total_seconds() / 60 / self.interval) - 1)
                    
                    # Calculate each missing timestamp
                    for j in range(missing_count):
                        missing_time = current + expected_interval * (j + 1)
                        missing_timestamps.append(missing_time)
                    
                    gaps.append({
                        'start': current,
                        'end': next_time,
                        'duration': str(actual_interval),
                        'missing_candles': missing_count
                    })
            
            # Print results
            if gaps:
                print(f"\nNumber of missing candles: {len(missing_timestamps)}")
                print("List of missing candles:")
                for timestamp in missing_timestamps:
                    print(f"- {timestamp}")
            else:
                print("No missing candles found.")
            
            return gaps
