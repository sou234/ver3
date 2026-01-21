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
