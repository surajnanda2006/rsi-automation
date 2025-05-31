# Install required libraries (only first time in Colab)
!pip install yfinance ta google-auth google-auth-oauthlib google-auth-httplib2 gspread oauth2client

# Import libraries
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Google Sheet Authentication
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name("/content/service_account.json", scope)
client = gspread.authorize(creds)

# OPEN your Google Sheet
sheet = client.open("Candian Stock Market ETF Trading")

# Process ETFs
def process_etfs():
    worksheet = sheet.worksheet("Technical indicators")
    etf_codes = worksheet.col_values(1)[1:]
    print("Tickers read from ETF sheet:", etf_codes)
    results = []

    for code in etf_codes:
        if code.startswith("TSE:"):
            code_yahoo = code.replace("TSE:", "") + ".TO"
        else:
            code_yahoo = code

        print(f"Processing {code_yahoo}...")

        daily_rsi, daily_bb, daily_macd, daily_supertrend = calculate_indicators(code_yahoo, "1d", "1y")
        weekly_rsi, weekly_bb, weekly_macd, weekly_supertrend = calculate_indicators(code_yahoo, "1wk", "1y")
        monthly_rsi, monthly_bb, monthly_macd, monthly_supertrend = calculate_indicators(code_yahoo, "1mo", "5y")

        result_row = [
            daily_rsi, weekly_rsi, monthly_rsi,
            daily_bb, weekly_bb, monthly_bb,
            daily_macd, weekly_macd, monthly_macd,
            daily_supertrend, weekly_supertrend, monthly_supertrend
        ]
        results.append(result_row)

    for idx, row in enumerate(results, start=2):
        worksheet.update(f'K{idx}', [row])

    print("✅ ✅ ✅ ETF SHEET FULLY UPDATED ✅ ✅ ✅")

# Process Stocks
def process_stocks():
    worksheet = sheet.worksheet("Stocks")
    stock_codes = worksheet.col_values(1)[1:]
    print("Tickers read from Stock sheet:", stock_codes)
    results = []

    for code in stock_codes:
        code_yahoo = code
        print(f"Processing {code_yahoo}...")

        daily_rsi, daily_bb, daily_macd, daily_supertrend = calculate_indicators(code_yahoo, "1d", "1y")
        weekly_rsi, weekly_bb, weekly_macd, weekly_supertrend = calculate_indicators(code_yahoo, "1wk", "1y")
        monthly_rsi, monthly_bb, monthly_macd, monthly_supertrend = calculate_indicators(code_yahoo, "1mo", "5y")

        result_row = [
            daily_rsi, weekly_rsi, monthly_rsi,
            daily_bb, weekly_bb, monthly_bb,
            daily_macd, weekly_macd, monthly_macd,
            daily_supertrend, weekly_supertrend, monthly_supertrend
        ]
        results.append(result_row)

    for idx, row in enumerate(results, start=2):
        worksheet.update(f'K{idx}', [row])

    print("✅ ✅ ✅ STOCK SHEET FULLY UPDATED ✅ ✅ ✅")

# Calculate Indicators (shared function)
def calculate_indicators(ticker, interval, period):
    try:
        data = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        data.dropna(inplace=True)

        close = data['Close'].squeeze()
        high = data['High'].squeeze()
        low = data['Low'].squeeze()

        rsi = ta.momentum.rsi(close, window=14)
        bb = ta.volatility.BollingerBands(close, window=20, window_dev=2)
        bb_pos = (close - bb.bollinger_lband()) / (bb.bollinger_hband() - bb.bollinger_lband())
        bb_position = []
        for val in bb_pos:
            if val < 0.33:
                bb_position.append("Lower Band")
            elif val < 0.66:
                bb_position.append("Middle Band")
            else:
                bb_position.append("Upper Band")

        macd = ta.trend.macd_diff(close, window_slow=26, window_fast=12, window_sign=9)
        macd_signal = "Bullish" if macd.iloc[-1] > 0 else "Bearish"

        atr = ta.volatility.average_true_range(high, low, close, window=10)
        supertrend_upper = ((high + low) / 2) + (3 * atr)
        supertrend_lower = ((high + low) / 2) - (3 * atr)
        last_close = close.iloc[-1]
        supertrend_signal = "Bullish" if last_close > supertrend_upper.iloc[-1] else "Bearish"

        return (
            safe_value(rsi.iloc[-1]), 
            bb_position[-1], 
            macd_signal, 
            supertrend_signal
        )
    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        return None, None, None, None

# Safe conversion
def safe_value(val):
    if val is None or pd.isna(val):
        return ""
    if isinstance(val, (np.float64, np.float32, float)) and (np.isnan(val) or np.isinf(val)):
        return ""
    return float(val)

# Run both functions
process_etfs()
process_stocks()
