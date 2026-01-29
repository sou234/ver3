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

    nasdaq_dates = []
    if dates_list: nasdaq_dates = dates_list
    
    # 2. Yahoo Finance (Manual Chart API - Robust to SSL)
    # yfinance library often fails with SSL/Crumb issues. We use direct Chart API.
    yahoo_dates = []
    try:
        # Range 4y to be safe
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?range=4y&interval=1d&events=earnings"
        # Use simple headers
        h = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=h, verify=False, timeout=5)
        
        if r.status_code == 200:
            d = r.json()
            # Navigate JSON: chart -> result -> [0] -> events -> earnings
            if d.get('chart') and d['chart'].get('result'):
                res = d['chart']['result'][0]
                if 'events' in res and 'earnings' in res['events']:
                    earnings_dict = res['events']['earnings']
                    # earnings_dict is dict of timestamp -> data
                    for ts_key in earnings_dict:
                        # Timestamp is usually unix epoch
                        dt = pd.to_datetime(int(ts_key), unit='s')
                        yahoo_dates.append(dt)
        
    except Exception as e:
        print(f"Yahoo Direct API Error ({ticker}): {e}")

    # 3. Fallback to yfinance lib (Just in case)
    if not yahoo_dates:
        import yfinance as yf
        try:
            t = yf.Ticker(ticker)
            cal = t.earnings_dates
            if cal is not None and not cal.empty:
                 # ... (existing logic)
                 pass # Skip for now to assume Direct API works better
        except: pass
        
    # Merge and Deduplicate
    all_dates = set()
    for d in nasdaq_dates: all_dates.add(pd.Timestamp(d).normalize())
    for d in yahoo_dates: all_dates.add(pd.Timestamp(d).normalize())
    
    # 4. Manual Fallback (For Demo / Resilience against API Limits)
    # Nasdaq/Yahoo often block history > 1y. We augment major stocks manually.
    MANUAL_EARNINGS_DATES = {
        'TSLA': [
            '2024-10-23', '2024-07-23', '2024-04-23', '2024-01-24',
            '2023-10-18', '2023-07-19', '2023-04-19', '2023-01-25',
            '2022-10-19', '2022-07-20', '2022-04-20', '2022-01-26'
        ],
        'NVDA': [
            '2024-11-20', '2024-08-28', '2024-05-22', '2024-02-21',
            '2023-11-21', '2023-08-23', '2023-05-24', '2023-02-22',
            '2022-11-16', '2022-08-24', '2022-05-25', '2022-02-16'
        ],
        'AAPL': [
            '2024-10-31', '2024-08-01', '2024-05-02', '2024-02-01',
            '2023-11-02', '2023-08-03', '2023-05-04', '2023-02-02',
            '2022-10-27', '2022-07-28', '2022-04-28', '2022-01-27'
        ]
    }
    
    
    if ticker in MANUAL_EARNINGS_DATES:
        for d_str in MANUAL_EARNINGS_DATES[ticker]:
            all_dates.add(pd.Timestamp(d_str).normalize())
    
    sorted_dates = sorted(list(all_dates))
    
    return sorted_dates

@st.cache_data(ttl=3600)
def fetch_earnings_history_rich(ticker):
    """
    Fetch comprehensive earnings history from Nasdaq (EPS, Surprise).
    Returns DataFrame with columns: ['Date', 'Period', 'Est EPS', 'Act EPS', 'Surprise(%)']
    """
    history_data = []
    
    # 1. Nasdaq API
    try:
        url = f"https://api.nasdaq.com/api/company/{ticker}/earnings-surprise"
        response = requests.get(url, headers=HEADERS, verify=False, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and data['data'].get('earningsSurpriseTable') and data['data']['earningsSurpriseTable'].get('rows'):
                rows = data['data']['earningsSurpriseTable']['rows']
                
                for r in rows:
                    try:
                        d_str = r.get('dateReported')
                        if not d_str: continue
                        
                        dt = pd.to_datetime(d_str)
                        period = r.get('fiscalQuarter', 'N/A')
                        
                        # Clean values (Handle floats or strings with $)
                        def clean_val(val):
                            if val is None: return 0.0
                            if isinstance(val, (int, float)): return float(val)
                            # String cleanup
                            val_str = str(val).replace('$', '').replace(',', '').replace('%', '')
                            try:
                                return float(val_str)
                            except:
                                return 0.0

                        eps_act = clean_val(r.get('eps'))
                        eps_est = clean_val(r.get('consensusForecast'))
                        surprise = clean_val(r.get('percentageSurprise'))
                        
                        history_data.append({
                            'Date': dt,
                            'Period': period,
                            'Est EPS': eps_est,
                            'Act EPS': eps_act,
                            'Surprise(%)': surprise
                        })
                    except:
                        continue
    except Exception as e:
        pass
        
    if history_data:
        df = pd.DataFrame(history_data)
        df.sort_values(by='Date', ascending=False, inplace=True)
        return df
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_analyst_consensus(ticker):
    """
    Fetch Analyst Consensus using Yahoo Finance (yfinance) with Scraping Fallback.
    Returns dict with keys: targetMean, targetHigh, targetLow, recommendMean, recommendKey, analystCount
    """
    import yfinance as yf
    from bs4 import BeautifulSoup
    
    result = {
        'targetMean': None,
        'targetHigh': None,
        'targetLow': None,
        'recommendMean': None, # 1.0 (Strong Buy) ~ 5.0 (Sell)
        'recommendKey': None, 
        'analystCount': 0
    }
    
    # Method 1: yfinance API (Standard)
    try:
        session = requests.Session()
        session.verify = False
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        
        t = yf.Ticker(ticker, session=session)
        info = t.info
        
        if info and 'targetMeanPrice' in info:
            result['targetMean'] = info.get('targetMeanPrice')
            result['targetHigh'] = info.get('targetHighPrice')
            result['targetLow'] = info.get('targetLowPrice')
            result['recommendMean'] = info.get('recommendationMean')
            result['recommendKey'] = info.get('recommendationKey')
            result['analystCount'] = info.get('numberOfAnalystOpinions', 0)
            return result
            
    except Exception:
        pass
        
    # Method 2: Scraping Fallback (Yahoo Finance Quote Page)
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}"
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=5)
        
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # Target Price: Look for data-test="ONE_YEAR_TARGET_PRICE-value"
            # Note: Class names change, data-attributes are more stable
            # Or regex search
            
            # Simple check for "1y Target Est"
            # The structure is often: <td ... data-test="ONE_YEAR_TARGET_PRICE-value">123.45</td>
            target_tag = soup.find(attrs={"data-test": "ONE_YEAR_TARGET_PRICE-value"})
            if target_tag:
                 val = target_tag.text.strip().replace(',', '')
                 if val and val != 'N/A':
                      result['targetMean'] = float(val)
            
            # Analyst Rating (e.g. 1.8) is harder to scrape reliably from summary page.
            # It's usually in the "Analysis" tab: https://finance.yahoo.com/quote/{ticker}/analysis
            
            # If we got Target Price, that's partial success.
            # Let's try Analysis page for more if needed, but keeping it simple for now.
            
    except Exception:
        pass

    return result

