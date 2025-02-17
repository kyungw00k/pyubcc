from datetime import datetime, timedelta, timezone
import time
import os
import argparse
import logging
from .ubcc import UpbitCandleCollector

def main():
    parser = argparse.ArgumentParser(description='Upbit Candle Collector')
    parser.add_argument('coin', help='Coin symbol (e.g., BTC, ETH, DOGE) or full ticker (e.g., KRW-BTC, USDT-BTC)')
    parser.add_argument('--timeframe', default='day', 
                      choices=['minute1', 'minute3', 'minute5', 'minute10', 
                              'minute15', 'minute30', 'minute60', 'minute240',
                              'day', 'week', 'month'],
                      help='Time interval (default: day)')
    parser.add_argument('--days', type=int, default=30,
                      help='Collection period in days (default: 30)')
    parser.add_argument('--db-path', help='DB file path (default: db/{coin}_{timeframe}.db)')
    parser.add_argument('--export-csv', action='store_true',
                      help='Export data to CSV file')
    parser.add_argument('--verbose', action='store_true',
                      help='Enable detailed logging')
    
    args = parser.parse_args()
        
    try:
        print(f"\nStarting data collection for {args.coin}...")
        print(f"Period: {args.days} days, Timeframe: {args.timeframe}\n")
        
        # Handle coin parameter: if it doesn't contain '-', assume KRW as fiat
        if '-' in args.coin:
            ticker = args.coin.upper()
            fiat, coin = ticker.split('-')
        else:
            fiat = 'KRW'
            coin = args.coin.upper()
            ticker = f"{fiat}-{coin}"

        collector = UpbitCandleCollector(
            coin=coin,
            timeframe=args.timeframe,
            fiat=fiat,
            db_path=args.db_path,
            verbose=args.verbose,
            show_progress=True  # Progress bar is enabled by default in CLI mode
        )
        
        end_date = datetime.now()

        # Adjust end_date to match timeframe interval
        minutes_total = (end_date.hour * 60 + end_date.minute)
        if minutes_total % collector.interval != 0:
            minutes_to_subtract = minutes_total % collector.interval
            end_date = end_date - timedelta(minutes=minutes_to_subtract)

        start_date = (end_date - timedelta(days=args.days)).replace(hour=9, minute=0, second=0, microsecond=0)
        
        # Collect data and get analysis results in a single call
        total_count, expected_candles, timestamp_order_mismatches, gaps = collector.collect(
            start_date=start_date,
            end_date=end_date
        )

        print(f"\n=== {args.coin} Data Collection Results ===")

        print(f"Collection Period: {start_date.strftime('%Y-%m-%d %H:%M')} ~ {end_date.strftime('%Y-%m-%d %H:%M')}")
        print(f"Timeframe: {args.timeframe} ({collector.interval} minutes)")
        print(f"Collected Candles: {total_count:,}")

        # Calculate total missing candles from gaps
        total_missing_candles = sum(gap['missing_candles'] for gap in gaps) if gaps else 0
        print(f"Data Gaps: {total_missing_candles}")
        print(f"Timestamp Order Mismatches: {timestamp_order_mismatches}")

        if gaps:
            print(f"\n=== GAP Analysis Results ===")
            print(f"Number of GAPs Found: {len(gaps)}")
            for gap in gaps:
                print(f"- GAP Period: {gap['start'].strftime('%Y-%m-%d %H:%M')} ~ {gap['end'].strftime('%Y-%m-%d %H:%M')}")
                print(f"  Duration: {gap['duration']}, Missing Candles: {gap['missing_candles']}")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

if __name__ == '__main__':
    main()