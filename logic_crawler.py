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
def get_earnings_calendar(start_date_str=None, days=7):
    """
    Fetch earnings calendar from Nasdaq API for a range of dates.
    Default: 7 days from start_date (Weekly view).
    """
    if start_date_str is None:
        start_date = datetime.date.today()
    else:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        
    all_dfs = []
    
    # Progress bar usage inside a cached function is tricky/discouraged in Streamlit
    # but we can just loop cleanly.
    
    for i in range(days):
        target_date = start_date + datetime.timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        
        url = f"https://api.nasdaq.com/api/calendar/earnings?date={date_str}"
        
        try:
            # Short sleep to be polite to API?
            # time.sleep(0.1) 
            response = requests.get(url, headers=HEADERS, verify=False, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and data['data'].get('rows'):
                    rows = data['data']['rows']
                    df = pd.DataFrame(rows)
                    
                    # Columns Mapping
                    cols_map = {
                        'symbol': 'Ticker',
                        'name': 'Company',
                        'time': 'Time',
                        'epsForecast': 'Est. EPS',
                        'marketCap': 'Market Cap'
                    }
                    
                    existing_cols = [c for c in cols_map.keys() if c in df.columns]
                    df = df[existing_cols].rename(columns=cols_map)
                    
                    # Add Date Column
                    df['Date'] = date_str
                    
                    all_dfs.append(df)
        except:
             pass
             
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # Clean Time
        def clean_time(t):
            t_str = str(t).lower()
            if 'pre-market' in t_str: return '☀️ Pre-Market'
            if 'after-hours' in t_str: return '🌙 After-Hours'
            return t
            
        if 'Time' in final_df.columns:
            final_df['Time'] = final_df['Time'].apply(clean_time)
            
        return final_df
    else:
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
@st.cache_data(ttl=86400)
def fetch_historical_earnings_dates(ticker):
    """
    Fetch historical earnings dates.
    Priority: Nasdaq API > Yahoo Finance
    """
    dates_list = []
    
    # 1. Try Nasdaq API (Earnings Surprise Endpoint has history)
    try:
        url = f"https://api.nasdaq.com/api/company/{ticker}/earnings-surprise"
        # reuse HEADERS from top of file
        response = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and data['data'].get('earningsSurpriseTable') and data['data']['earningsSurpriseTable'].get('rows'):
                rows = data['data']['earningsSurpriseTable']['rows']
                # row: { 'dateReported': 'Sep 29, 2024', ... }
                for r in rows:
                    dr = r.get('dateReported')
                    if dr:
                         # Parse date "Sep 29, 2024"
                         try:
                             dt = pd.to_datetime(dr)
                             dates_list.append(dt)
                         except:
                             pass
    except Exception as e:
        print(f"Nasdaq Earnings Fetch Error ({ticker}): {e}")

    if dates_list:
        return dates_list

    # 2. Fallback to Yahoo Finance
    import yfinance as yf
    try:
        t = yf.Ticker(ticker)
        cal = t.earnings_dates
        if cal is not None and not cal.empty:
            dates = cal.index.sort_values(ascending=False)
            cutoff = pd.Timestamp.now() - pd.DateOffset(years=3)
            now = pd.Timestamp.now()
            valid_dates = dates[(dates >= cutoff) & (dates <= now)]
            return valid_dates.tolist()
    except Exception as e:
        print(f"Yahoo Earnings Fetch Error ({ticker}): {e}")
        
    return []
