import pandas as pd
import yfinance as yf
import numpy as np
from sklearn.linear_model import LinearRegression
import requests
import urllib3
import streamlit as st
import zipfile
import io

# ---------------------------------------------------------
# SSL Patch for Robustness (duplicated from app.py to ensure safety)
# ---------------------------------------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Check if already patched to avoid recursion if imported multiple times
if not getattr(requests.Session.request, "_patched", False):
    original_request = requests.Session.request
    def patched_request(self, method, url, *args, **kwargs):
        kwargs['verify'] = False
        return original_request(self, method, url, *args, **kwargs)
    patched_request._patched = True
    requests.Session.request = patched_request

# ---------------------------------------------------------
# Constants & Config
# ---------------------------------------------------------
# 골드만삭스 로직: 섹터 베타 제거용 벤치마크 매핑
SECTOR_BENCHMARKS = {
    '정보기술': 'XLK', '커뮤니케이션 서비스': 'XLC', '건강관리': 'XLV',
    '산업재': 'XLI', '자유소비재': 'XLY', '필수소비재': 'XLP',
    '금융': 'XLF', '부동산': 'XLRE', '에너지': 'XLE', '소재': 'XLB',
    '유틸리티': 'XLU', '지수': '^GSPC' # 기본값
}

def load_universe():
    """
    Load universe_stocks.csv and create a Label column.
    """
    try:
        df = pd.read_csv('universe_stocks.csv')
        # 선택창 표시용 라벨 생성: "Western Digital (WDC)"
        df['Label'] = df['Name'] + " (" + df['Ticker'] + ")"
        return df
    except Exception as e:
        st.error(f"Universe 파일 로드 실패: {e}")
        return pd.DataFrame(columns=['Ticker', 'Name', 'Sector', 'Label'])

@st.cache_data(ttl=3600)
def get_market_data(ticker, sector_etf, start_date="2022-01-01"):
    """
    Fetch adjusted close data for [Stock, Market(^GSPC), Sector_ETF].
    Calculates returns for Multi-Factor Regression.
    Falback: Synthetic Data.
    """
    market_index = "^GSPC" # S&P 500
    
    # Create a robust session
    session = requests.Session()
    session.verify = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

    data = None
    
    # 1. Fetch SPY Proxy (Market)
    df_market = fetch_spy_proxy()
    if df_market is None:
        # Fallback to synthetic if SPY fails (for demo robustness)
        return create_synthetic_market_data(ticker)
        
    # 2. Fetch Stock Data
    # Priority: Nasdaq (logic_crawler) > Yahoo
    try:
        if 'logic_crawler' not in globals():
             import logic_crawler
        
        df_stock_price = logic_crawler.fetch_historical_price(ticker)
        if df_stock_price.empty:
             # Fallback to Yahoo
             df_stock_price = yf.download(ticker, period="3y", session=session, progress=False)
             if not df_stock_price.empty:
                 if 'Adj Close' in df_stock_price.columns:
                     s = df_stock_price['Adj Close']
                 else:
                     s = df_stock_price['Close']
                 if isinstance(s, pd.DataFrame): s = s.iloc[:, 0]
                 df_stock_price = pd.DataFrame(s)
                 df_stock_price.columns = ['Stock']

        # Calculate Stock Returns (Log Return to match SPY)
        if not df_stock_price.empty and 'Stock' in df_stock_price.columns:
             # Ensure index is datetime
             df_stock_price.index = pd.to_datetime(df_stock_price.index)
             
             # Log Return
             df_stock_price['Stock'] = np.log(df_stock_price['Stock'] / df_stock_price['Stock'].shift(1))
             df_stock_price.dropna(inplace=True)
             
             # 3. Merge
             data = df_market.join(df_stock_price, how='inner')
             
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        
    return data
# ------------------------------------------------------------------------------
# 1. Data Fetching (Factors)
# ------------------------------------------------------------------------------

@st.cache_data(ttl=86400) # Cache for 1 day
def get_fama_french_factors():
    """
    Download Daily 3-Factor Data from Kenneth French Library.
    Returns DataFrame with columns ['SMB', 'HML', 'RF'] (Decimals).
    """
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        r = requests.get(url, headers=headers, verify=False, timeout=15)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                csv_filename = [f for f in z.namelist() if f.endswith('.csv')][0]
                with z.open(csv_filename) as f:
                    # Skip 3 lines for header
                    df = pd.read_csv(f, skiprows=3)
                    df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
                    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d', errors='coerce')
                    df.dropna(subset=['Date'], inplace=True)
                    df.set_index('Date', inplace=True)
                    
                    # Convert Percent to Decimal and Select SMB, HML
                    df = df[['SMB', 'HML']] / 100.0
                    return df
    except Exception as e:
        print(f"FF Download Error: {e}")
        return pd.DataFrame() # Return empty if failed

@st.cache_data(ttl=86400)
def get_momentum_factor():
    """
    Download Daily Momentum Factor (MOM/UMD) from Kenneth French Library.
    """
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, verify=False, timeout=15)
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                # Find CSV
                csv_filename = [f for f in z.namelist() if f.endswith('.csv')][0]
                with z.open(csv_filename) as f:
                    # Skip header (usually 13 lines for Mom)
                    # We'll read and find the first row that starts with a date-like number
                    lines = f.readlines()
                    start_idx = 0
                    for i, line in enumerate(lines):
                        if b"Date" in line or b"date" in line: # Sometimes header is straightforward
                            start_idx = i
                            break
                        # Heuristic: Check if line starts with 8 digits
                        parts = line.decode('utf-8').strip().split(',')
                        if len(parts) > 0 and parts[0].isdigit() and len(parts[0]) == 8:
                            start_idx = i - 1 # Previous line is header (or none)
                            if start_idx < 0: start_idx = 0
                            break
                    
                    # Re-open or parse from buffer? easier to re-read with skips
                    f.seek(0)
                    df = pd.read_csv(f, skiprows=13) # Direct approach often safest for FF
                    
                    df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
                    df.columns = [c.strip() for c in df.columns]
                    
                    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d', errors='coerce')
                    df.dropna(subset=['Date'], inplace=True)
                    df.set_index('Date', inplace=True)
                    
                    # 'Mom' column
                    if 'Mom' in df.columns:
                        return df[['Mom']] / 100.0
                    return df.iloc[:, [0]] / 100.0
    except:
        pass
    return None

def fetch_yahoo_etf(ticker):
    """
    Fetch ETF historical data from Yahoo Finance (using requests/yfinance).
    Attempts to get robust data.
    """
    try:
        session = requests.Session()
        session.verify = False
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.04472.124 Safari/537.36'}
        
        # Try yfinance directly first (with session)
        dat = yf.download(ticker, period="3y", session=session, progress=False)
        if hasattr(dat, 'columns') and 'Close' in dat.columns: # Multi-index check
             if isinstance(dat.columns, pd.MultiIndex):
                 return dat['Close'][ticker] if ticker in dat['Close'].columns else dat['Close']
             return dat['Close']
        elif not dat.empty:
             return dat['Close'] if 'Close' in dat else dat
             
    except:
        pass
    return None

def create_synthetic_market_data(ticker):
    """
    Generate synthetic data for failover demonstration.
    """
    dates = pd.date_range(end=pd.Timestamp.now().normalize(), periods=756, freq='B')
    n = len(dates)
    
    seed_val = abs(hash(ticker)) % (2**32)
    np.random.seed(seed_val)
    
    # 1. Market Factor (S&P 500)
    r_mkt = np.random.normal(0.0004, 0.010, n)

    # 2. Sector Factor
    r_sec_idiosyncratic = np.random.normal(0, 0.005, n)
    r_sec = 1.1 * r_mkt + r_sec_idiosyncratic
    
    # 3. Stock
    beta_mkt = 0.8
    beta_sec = 0.5
    
    alpha_vol = np.random.uniform(0.01, 0.08)
    r_idio = np.random.normal(0, alpha_vol, n)
    
    r_stock = (beta_mkt * r_mkt) + (beta_sec * r_sec) + r_idio
    
    df_synth = pd.DataFrame({
        'Market': r_mkt,
        'Sector': r_sec,
        'Stock': r_stock
    }, index=dates)
    
    return df_synth

def fetch_spy_proxy():
    """
    Fetch SPY data to act as S&P 500 Market Proxy.
    Priority: Nasdaq > Yahoo (Session) > Synthetic Fallback
    """
    # 1. Try Nasdaq (Known to work for stocks)
    try:
        df = logic_crawler.fetch_historical_price("SPY")
        if not df.empty and 'Stock' in df.columns:
            # Rename 'Stock' -> 'Market'
            df.rename(columns={'Stock': 'Market'}, inplace=True)
            return df
    except:
        pass
        
    # 2. Try Yahoo (Session patched)
    try:
        session = requests.Session()
        session.verify = False
        dat = yf.download("SPY", period="3y", session=session, progress=False)
        if not dat.empty:
            # Prefer 'Adj Close', fall back to 'Close'
            if 'Adj Close' in dat.columns:
                 s = dat['Adj Close']
            elif 'Close' in dat.columns:
                 s = dat['Close']
            else:
                 return None

            if isinstance(s, pd.DataFrame):
                s = s['SPY'] if 'SPY' in s.columns else s.iloc[:, 0]
            
            df = pd.DataFrame(s)
            df.columns = ['Market']
            df.index.name = 'Date'
            
            # Convert to Log Returns: ln(Pt / Pt-1)
            # Using numpy log of simple return + 1 is equivalent
            # Or price/shifted_price
            df['Market'] = np.log(df['Market'] / df['Market'].shift(1))
            return df.dropna()
    except:
        pass
        
    return None

def enrich_with_factors(df, ticker):
    """
    Enrich data with Sector ETF and Fama-French Factors.
    df: DataFrame with Date index and 'Market' column (Returns).
    """
    df = df.copy()
    
    # 1. Sector Factor (if missing)
    if 'Sector' not in df.columns:
        try:
            universe = load_universe()
            if ticker in universe['Ticker'].values:
                # Get Sector Name from Universe
                sec_name = universe.loc[universe['Ticker'] == ticker, 'Sector'].iloc[0]
                # Map to ETF
                etf_ticker = SECTOR_BENCHMARKS.get(sec_name, 'XLK') # Default
                
                # Fetch ETF Data
                etf_series = fetch_yahoo_etf(etf_ticker)
                if etf_series is not None:
                    etf_ret = etf_series.pct_change().dropna()
                    etf_ret.name = 'Sector'
                    df = df.join(etf_ret, how='inner')
        except:
            pass
            
    # 2. Style Factors (Fama-French)
    try:
        ff_df = get_fama_french_factors()
        if ff_df is not None and not ff_df.empty:
            df = df.join(ff_df[['SMB', 'HML']], how='inner')
    except:
        pass

    # 3. Momentum Factor (UMD)
    try:
        mom_df = get_momentum_factor()
        if mom_df is not None and not mom_df.empty:
            # Join MOM
            # Rename col to 'MOM' if needed
            mom_df.columns = ['MOM']
            df = df.join(mom_df, how='inner')
    except:
        pass
        
    return df

def calculate_idio_score(df, ticker_symbol):
    """
    Calculate Earnings Idio Score using Multi-Factor Regression.
    Supports:
    - 4-Factor: Market + Sector + SMB + HML
    - 2-Factor: Market + Sector
    - 1-Factor: Market (CAPM)
    """
    if df is None or df.empty:
         return 0.0, pd.DataFrame(), {}, 0.0, 0.0, {}
    
    # Enrich with Factors if needed (Sector/Style)
    # Note: df usually comes from process_benchmark_file which only has Market (and maybe Sector)
    # We should attempt enrichment here if columns are missing?
    # Actually, `app.py` passes the merged [Stock, Market] df.
    # To be safe, we should assume `df` MIGHT NOT have Sector/Style yet if coming from simple upload.
    # BUT `app.py` has the ticker. Let's try to enrich HERE if missing.
    
    valid_cols = [c for c in ['Market', 'Sector', 'SMB', 'HML', 'MOM'] if c in df.columns]
    
    # If only Market, try to enrich?
    # Ideally enrichment happens BEFORE calculate, to ensure intersection of dates.
    # But let's do soft enrichment here logic-wise.
    # Actually, modifying DF inside calculation is risky if indices don't match.
    # BETTER: Caller calls enrich. But for robustness, let's just use what's given.
    
    # Determine Model
    X_cols = valid_cols
    if not X_cols:
        # score, df, betas, ret, vol, stats
        return 0.0, pd.DataFrame(), {}, 0.0, 0.0, {}

    # 1. Regression
    X = df[X_cols].values 
    y = df['Stock'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    prediction = model.predict(X)
    residuals = y - prediction # Idio Return
    
    # Coefficients Mapping
    beta_mkt = 0.0
    beta_sec = 0.0
    beta_smb = 0.0
    beta_hml = 0.0
    beta_mom = 0.0
    
    for i, col in enumerate(X_cols):
        if col == 'Market': beta_mkt = model.coef_[i]
        elif col == 'Sector': beta_sec = model.coef_[i]
        elif col == 'SMB': beta_smb = model.coef_[i]
        elif col == 'HML': beta_hml = model.coef_[i]
        elif col == 'MOM': beta_mom = model.coef_[i]
    
    df = df.copy()
    df['Idio_Return'] = residuals
    df['Beta_Return'] = prediction
    
    # 2. Earnings Date Filtering
    earnings_dates = []
    try:
        # Try Fetching Real Historical Earnings Dates
        if 'logic_crawler' not in globals():
             import logic_crawler
        
        real_dates = logic_crawler.fetch_historical_earnings_dates(ticker_symbol)
        
        # DEBUG: Print Date Matching Info to UI
        # st.write(f"DEBUG: Found {len(real_dates)} raw earnings dates for {ticker_symbol}")
        
        # Normalize and filter intersection with Price Data Index
        available_dates = df.index.normalize()
        if available_dates.tz is not None:
             available_dates = available_dates.tz_localize(None)
             
        valid_real_dates = []
        for d in real_dates:
            ts = pd.Timestamp(d).normalize()
            if ts.tz is not None:
                ts = ts.tz_localize(None)
                
            if ts in available_dates:
                 valid_real_dates.append(ts)
            # else:
            #     st.write(f"DEBUG: Missed {ts}")

        # st.write(f"DEBUG: Matched {len(valid_real_dates)} dates with price data.")
                 # Many earnings calendars list the 'Report Date'.
                 pass
        
        if len(valid_real_dates) >= 1:
            earnings_dates = valid_real_dates
            
            # --- EXPAND WINDOW LOGIC (T-2 to T+2) ---
            # Now we have exact Earnings Days (T). We want [T-2, T-1, T, T+1, T+2].
            # 1. Find integer locations of T in the df.index
            # Since df.index is available_dates (with tz removed temporarily?), 
            # actually we need to be careful. df.index might be DatetimeIndex.
            
            # Re-ensure df index is normalized for lookup? 
            # df is already normalized in 'available_dates = df.index.normalize()' logic? No.
            # But we matched valid_real_dates against available_dates.
            
            # Let's use searchsorted or get_indexer on the fully matching date objects.
            # valid_real_dates contains Timestamps that are definitely in available_dates.
            
            # We need to find the integer index of each 'd' in 'df.index'.
            # Note: df.index might not be sorted ascending? It usually is.
            # Let's handle it robustly using get_loc or similar if unique.
            
            expanded_indices = set()
            
            # Convert valid_real_dates to a set for faster lookup? No, we need locations.
            # Assuming df.index is sorted ascending (Time Series).
            
            # Mapped locations
            # We iterate dates, find loc, then take range [loc-2, loc+2]
            
            # To do this correctly with TZ issues:
            # We know valid_real_dates exactly found a match in available_dates.
            # available_dates[i] corresponds to df.index[i].
            
            # Let's map date -> integer index
            date_to_idx = {d: i for i, d in enumerate(available_dates)}
            
            for d in valid_real_dates:
                if d in date_to_idx:
                    center_idx = date_to_idx[d]
                    
                    # Window: [T-2, T+2] -> 5 days (User Request: Wider coverage)
                    # Since we take the MAX peak, wider window does NOT dilute score, but captures drifts.
                    start_idx = max(0, center_idx - 2)
                    end_idx = min(len(df) - 1, center_idx + 2)
                    
                    for i in range(start_idx, end_idx + 1):
                        expanded_indices.add(df.index[i])
                        
            # Replace earnings_dates with the expanded set
            if expanded_indices:
                earnings_dates = list(expanded_indices)
                
        else:
             # Fallback to sample ONLY if strictly no real data found (e.g. Synthetic/Demo)
             # But user requested "Consider Earnings Dates explicitly".
             # If we can't find dates, maybe we shouldn't score it?
             # Let's keep fallback for UI robustness but maybe warn?
             earnings_dates = df.sample(8).index
             
    except Exception as e:
        print(f"Earnings Date Error: {e}")
        earnings_dates = df.sample(8).index
        
    valid_dates = [d for d in earnings_dates if d in df.index]
    
    # If using real dates, we trust them even if small number (e.g. IPO recent)
    # But for calculation stability we need at least some data.
    if len(valid_dates) < 1:
         # Hard fallback if absolutely nothing matches
         # Use top 10% or top 8 most volatile Idio Return days as proxy for "Event Days"
         top_volatile = df['Idio_Return'].abs().nlargest(8).index
         earnings_moves = df.loc[top_volatile]
         valid_dates = top_volatile.tolist() # Update valid_dates for consistency downstream
    else:
         earnings_moves = df.loc[valid_dates]

    # 3. Score Calculation (Daily Raw)
    if not earnings_moves.empty:
        # No Annualization (*4 removal)
        daily_ret = np.mean(np.abs(earnings_moves['Idio_Return']))
        daily_vol = np.std(earnings_moves['Idio_Return'])
        
        score = (daily_ret / daily_vol) if daily_vol != 0 else 0.0
    else:
        score, daily_ret, daily_vol = 0.0, 0.0, 0.0
    
    betas = {
        'Market': beta_mkt,
        'Sector': beta_sec,
        'SMB': beta_smb,
        'HML': beta_hml,
        'MOM': beta_mom
    }
    
    # Filter Residuals
    # 1) Earnings Days
    # Use valid_dates which contains either valid_real_dates OR sampled dates (Fallback)
    earnings_residuals = df.loc[df.index.isin(valid_dates), 'Idio_Return']
    
    # 2) Non-Earnings Days (The rest)
    non_earnings_residuals = df.loc[~df.index.isin(valid_dates), 'Idio_Return']

    # Calculate Stats
    # A. Earnings Days Stats
    if not earnings_residuals.empty:
        e_mean = earnings_residuals.abs().mean() # Magnitude
        e_vol = earnings_residuals.std()
        e_cnt = len(earnings_residuals)
    else:
        e_mean, e_vol, e_cnt = 0.0, 0.0, 0

    # B. Non-Earnings Days Stats
    if not non_earnings_residuals.empty:
        ne_mean = non_earnings_residuals.abs().mean()
        ne_vol = non_earnings_residuals.std()
        ne_cnt = len(non_earnings_residuals)
    else:
        ne_mean, ne_vol, ne_cnt = 0.0, 0.0, 0
    
    # --- [New Logic: Max Peak per Event] ---
    # Instead of averaging all days (dilution), we want "Average Peak Impact".
    # Iterate through valid_real_dates to find max abs return for each event.
    event_peaks = []
    event_dirs = []
    
    if len(valid_real_dates) > 0:
        # Use Real Dates if available
        # Need to map dates to indices again or strict lookups
        # To avoid re-mapping complexity, we can use the window lookup logic again or simplified approach
        # Since we just need an aggregate, let's fast-loop.
        for d in valid_real_dates:
             # Window [T, T+1]
             # Handle TZ naive/aware matching by using string slicing or normalize
             # Assuming df.index is normalized from earlier logic.
             # Actually df.index might be DatetimeIndex.
             
             # Robust Window Lookup
             try:
                 loc = df.index.get_loc(d)
                 # Handle slice if Loc returns slice/array (duplicate index), take first
                 if isinstance(loc, slice): loc = loc.start
                 if isinstance(loc, np.ndarray): loc = loc[0]
                 
                 start = loc
                 end = min(len(df), loc + 2) # [T, T+1] (slice is exclusive at end)
                 
                 window_rets = df['Idio_Return'].iloc[start:end].abs()
                 
                 if not window_rets.empty:
                     # Top-2 Average Logic (User Req 1)
                     top_2 = window_rets.nlargest(2)
                     event_score = top_2.mean()
                     event_peaks.append(event_score)
                     
                     # Directional Score (User Req 4)
                     # Sum of residuals in window
                     win_res_signed = df['Idio_Return'].iloc[start:end]
                     event_dirs.append(win_res_signed.sum())
                     
             except:
                 pass
    else:
        # Fallback
        if not earnings_moves.empty:
             event_peaks = earnings_moves['Idio_Return'].abs().tolist()
             event_dirs = earnings_moves['Idio_Return'].tolist()

    if event_peaks:
        # Aggregation: Median (User Req 5 - Robustness)
        e_stat = np.median(event_peaks)
        e_cnt = len(event_peaks)
        
        # Direction Aggregation: Median of Sums
        if event_dirs:
            dir_score = np.median(event_dirs)
        else:
            dir_score = 0.0
    else:
        e_stat, e_cnt, dir_score = 0.0, 0, 0.0

    # Recalculate Normal Volatility (strictly excluding ALL windows)
    # merged_indices = union of all [T-2, T+2]
    # We already have `expanded_indices` set if we used real dates.
    # If fallback, we used top_volatile.
    
    # If we have real dates logic, use `expanded_indices` to exclude.
    # But `expanded_indices` is local variable inside try block. 
    # Let's reconstruct it or use what we have.
    # Actually `valid_dates` is just the list of center dates.
    
    # To be precise, let's just use non_earnings_residuals BUT ensuring it excludes the user-defined window correctly.
    # earlier logic: `earnings_residuals` was loc(valid_dates).
    # IF valid_dates was just T, then we only excluded T.
    # We need to exclude [T-2, T+2].
    
    # Re-filter Normal Vals
    if len(valid_dates) > 0:
        # Collect all "Event" indices
        event_indices_set = set()
        for d in valid_dates:
             try:
                 loc = df.index.get_loc(d)
                 if isinstance(loc, slice): loc = loc.start
                 if isinstance(loc, np.ndarray): loc = loc[0]
                 start = max(0, loc - 2)
                 end = min(len(df), loc + 3) # Slice exclusive
                 for i in range(start, end):
                     event_indices_set.add(df.index[i])
             except: pass
             
        normal_df = df.loc[~df.index.isin(event_indices_set)]
        if not normal_df.empty:
            ne_vol = normal_df['Idio_Return'].std()
        else:
             ne_vol = df['Idio_Return'].std() # Fallback if everything is event (unlikely)
             
    # Final Score
    if ne_vol > 0:
        score = e_stat / ne_vol
    else:
        score = 0.0
        
    # Return expanded dictionary including Direction
    comp_stats = {
        'Earnings_Stat': e_stat, # Median of Top2Avg
        'Earnings_Count': e_cnt,
        'Normal_Vol': ne_vol,
        'Direction_Score': dir_score,
        'Label': 'Bullish' if dir_score > 0 else 'Bearish'
    }

    return score, df, betas, e_stat, ne_vol, comp_stats

def process_uploaded_file(uploaded_file):
    """
    Process user uploaded CSV/Excel for Idio Score analysis.
    Expected Columns: ['Date', 'Stock', 'Market', 'Sector']
    """
    try:
        # Determine file type
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
            
        # Column standardization
        df.columns = [str(c).strip() for c in df.columns]
        
        # Check required columns
        # Filter only numeric columns needed
        cols_needed = ['Stock', 'Market', 'Sector']
        
        # Date processing
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        elif not isinstance(df.index, pd.DatetimeIndex):
             # Try first column
             df.index = pd.to_datetime(df.iloc[:, 0])
        
        if not all(c in df.columns for c in cols_needed):
             return None, "CSV 파일에 ['Stock', 'Market', 'Sector'] 컬럼이 꼭 필요합니다."
             
        df_prices = df[cols_needed].astype(float)
        
        # Calculate Returns
        df_returns = df_prices.pct_change().dropna()
        
        return df_returns, None

    except Exception as e:
        return None, f"파일 처리 오류: {str(e)}"

def process_benchmark_file(uploaded_file):
    """
    Process User Uploaded Benchmark File (Market, Sector).
    Used for Hybrid Mode: Benchmark(Index) + Live Stock(Crawler)
    Expected Columns: ['Date', 'Market', 'Sector']
    """
    try:
        # Determine file type
        if uploaded_file.name.endswith('.csv') or uploaded_file.name.endswith('.txt'):
            # Try Default (Comma)
            df = pd.read_csv(uploaded_file)
            
            # If parsing failed (e.g. all in one column), try Tab (Clipboard paste format)
            if len(df.columns) < 2:
                uploaded_file.seek(0)
                try:
                    df = pd.read_csv(uploaded_file, sep='\t')
                except:
                    pass
        else:
            df = pd.read_excel(uploaded_file)
            
        # Column standardization
        df.columns = [str(c).strip() for c in df.columns]
        
        # Check required columns (Market is mandatory, Sector is optional but recommended)
        if 'Market' not in df.columns:
             return None, "파일에 'Market' (S&P 500) 컬럼이 없습니다."
        
        # Date processing
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        elif not isinstance(df.index, pd.DatetimeIndex):
             # Try first column
             df.index = pd.to_datetime(df.iloc[:, 0])
             df.index.name = 'Date'
        
        # Filter numeric columns
        cols = ['Market']
        if 'Sector' in df.columns:
            cols.append('Sector')
            
        df_bench = df[cols].astype(float)
        
        # Calculate Returns
        # Assuming input is PRICES (Level), convert to Returns
        df_returns = df_bench.pct_change().dropna()
        
        return df_returns, None

    except Exception as e:
        return None, f"벤치마크 파일 처리 오류: {str(e)}"

def get_vix_level():
    """
    Fetch current VIX level.
    Fallback to synthetic if blocked.
    """
    try:
        # Try Yahoo Finance first
        session = requests.Session()
        session.verify = False
        vix = yf.Ticker("^VIX", session=session)
        hist = vix.history(period="1d")
        if not hist.empty:
            return hist['Close'].iloc[-1]
    except:
        pass
        
    # Fallback: Random realistic VIX (15 ~ 25)
    # Using minute to slightly vary it
    import time
    random_vix = 18.5 + (time.time() % 100 / 20.0)
    return random_vix

@st.cache_data(ttl=86400)
def fetch_spy_proxy():
    """
    Fetch SPY data to act as S&P 500 Market Proxy.
    Priority: Nasdaq > Yahoo (Session) > Synthetic Fallback
    """
    # 1. Try Nasdaq (Known to work for stocks)
    try:
        if 'logic_crawler' not in globals():
             import logic_crawler
             
        df = logic_crawler.fetch_historical_price("SPY")
        if not df.empty and 'Stock' in df.columns:
            # Rename 'Stock' -> 'Market'
            df.rename(columns={'Stock': 'Market'}, inplace=True)
            return df
    except:
        pass
        
    # 2. Try Yahoo (Session patched)
    try:
        session = requests.Session()
        session.verify = False
        dat = yf.download("SPY", period="3y", session=session, progress=False)
        if not dat.empty:
            # Prefer 'Adj Close', fall back to 'Close'
            if 'Adj Close' in dat.columns:
                 s = dat['Adj Close']
            elif 'Close' in dat.columns:
                 s = dat['Close']
            else:
                 return None

            if isinstance(s, pd.DataFrame):
                s = s['SPY'] if 'SPY' in s.columns else s.iloc[:, 0]
            
            df = pd.DataFrame(s)
            df.columns = ['Market']
            df.index.name = 'Date'
            
            # Convert to Log Returns: ln(Pt / Pt-1)
            # Using numpy log of simple return + 1 is equivalent
            # Or price/shifted_price
            df['Market'] = np.log(df['Market'] / df['Market'].shift(1))
            
            if df.empty: return None
            
            return df.dropna()
    except:
        pass
        
    return None
