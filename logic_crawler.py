import requests
import pandas as pd
import datetime
import streamlit as st
import urllib3

# Disable SSL warnings globally
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.nasdaq.com/',
    'Origin': 'https://www.nasdaq.com'
}

@st.cache_data(ttl=3600)
def get_earnings_calendar(date_str=None):
    """
    Fetch earnings calendar from Nasdaq API for a specific date (YYYY-MM-DD).
    If date_str is None, uses today.
    """
    if date_str is None:
        date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={date_str}"
    
    try:
        response = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and data['data'].get('rows'):
                rows = data['data']['rows']
                df = pd.DataFrame(rows)
                
                # Select and Rename columns
                # Available: lastYearRptDt, lastYearEPS, time, symbol, name, marketCap, fiscalQuarterEnding, epsForecast, noOfEsts
                cols_map = {
                    'symbol': 'Ticker',
                    'name': 'Company',
                    'time': 'Time', # time-pre-market, time-after-hours
                    'epsForecast': 'Est. EPS',
                    'marketCap': 'Market Cap'
                }
                
                # Filter columns that exist
                existing_cols = [c for c in cols_map.keys() if c in df.columns]
                df = df[existing_cols].rename(columns=cols_map)
                
                # Clean up 'Time' column
                def clean_time(t):
                    if 'pre-market' in str(t).lower(): return '☀️ Pre-Market'
                    if 'after-hours' in str(t).lower(): return '🌙 After-Hours'
                    return t
                
                if 'Time' in df.columns:
                    df['Time'] = df['Time'].apply(clean_time)
                
                return df
            else:
                return pd.DataFrame() # No data found
        else:
            st.error(f"Nasdaq API Error: {response.status_code}")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Crawling Error: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_historical_price(ticker):
    """
    Fetch 3-year daily historical price (Close) for a ticker from Nasdaq.
    Returns DataFrame with index 'Date' and column 'Stock'.
    """
    try:
        # User Agent is critical
        headers = HEADERS
        
        to_date = datetime.datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.datetime.now() - datetime.timedelta(days=365*3)).strftime("%Y-%m-%d")
        
        url = f"https://api.nasdaq.com/api/quote/{ticker}/historical?assetclass=stocks&fromdate={from_date}&todate={to_date}&limit=9999"
        
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and data.get('data') and data['data'].get('tradesTable') and data['data']['tradesTable'].get('rows'):
                rows = data['data']['tradesTable']['rows']
                df = pd.DataFrame(rows)
                
                # Cleaning
                # Columns: date, close, volume, open, high, low
                df = df.rename(columns={'date': 'Date', 'close': 'Stock'})
                df['Date'] = pd.to_datetime(df['Date'])
                
                # Clean price string ($ sign)
                df['Stock'] = df['Stock'].astype(str).str.replace('$', '').str.replace(',', '').astype(float)
                
                df.set_index('Date', inplace=True)
                df.sort_index(inplace=True)
                
                return df[['Stock']]
            else:
                return pd.DataFrame()
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Price Fetch Error: {e}")
        return pd.DataFrame()
