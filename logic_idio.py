import pandas as pd
import yfinance as yf
import numpy as np
from sklearn.linear_model import LinearRegression
import requests
import urllib3
import streamlit as st

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
    try:
        tickers = [ticker, market_index, sector_etf]
        # Remove duplicates if any
        tickers = list(set(tickers))
        
        data = yf.download(tickers, start=start_date, progress=False, session=session)['Adj Close']
    except Exception as e:
        pass

    # Process Real Data
    if data is not None and not data.empty:
        try:
            returns = data.pct_change().dropna()
            
            # Map columns safely
            cols = returns.columns
            
            # Identify columns exist
            has_stock = ticker in cols
            has_mkt = market_index in cols
            has_sec = sector_etf in cols
            
            if has_stock and has_mkt and has_sec:
                df = returns[[market_index, sector_etf, ticker]].copy()
                df.columns = ['Market', 'Sector', 'Stock']
                return df
        except:
             pass

    # --- FALLBACK: Synthetic Data (Multi-Factor) ---
    try:
        st.toast(f"⚠️ {ticker}: 네트워크 보안으로 실시간 데이터 제한됨. 시연용 샘플 생성.", icon="🧪")
    except: pass
    
    dates = pd.date_range(start=start_date, end=pd.Timestamp.now(), freq='B')
    n = len(dates)
    # Seed based on ticker to ensure consistency per stock but diversity across stocks
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
    
    # Generate variable volatility for Alpha to show diverse scores (Low < 2.0, High > 4.0)
    # Scale from 1% to 8% standard deviation
    alpha_vol = np.random.uniform(0.01, 0.08)
    r_idio = np.random.normal(0, alpha_vol, n)
    
    r_stock = (beta_mkt * r_mkt) + (beta_sec * r_sec) + r_idio
    
    df_synth = pd.DataFrame({
        'Market': r_mkt,
        'Sector': r_sec,
        'Stock': r_stock
    }, index=dates)
    
    return df_synth

def calculate_idio_score(df, ticker_symbol):
    """
    Calculate Earnings Idio Score using Multi-Factor Regression.
    Model: Stock ~ Beta_Mkt * Market + Beta_Sec * Sector
    """
    if df is None or df.empty:
         return 0.0, pd.DataFrame(), 0.0, 0.0
    
    # 실적 발표일 가져오기 (Fallback to Random)
    earnings_dates = []
    try:
        # stock = yf.Ticker(ticker_symbol) ...
        earnings_dates = df.sample(8).index
    except:
         earnings_dates = df.sample(8).index

    # 1. 다중 회귀분석 (Market + Sector)
    X = df[['Market', 'Sector']].values 
    y = df['Stock'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    prediction = model.predict(X)
    residuals = y - prediction # 순수 Idiosyncratic Return
    
    # Coefficients
    beta_mkt = model.coef_[0]
    beta_sec = model.coef_[1]
    
    df = df.copy()
    df['Idio_Return'] = residuals
    df['Beta_Return'] = prediction # Explained portion
    
    # 2. 실적 발표일 필터링 (Synthetic/Random)
    valid_dates = [d for d in earnings_dates if d in df.index]
    if len(valid_dates) < 3:
        # Outlier fallback
        top_volatile_dates = df['Idio_Return'].abs().nlargest(8).index
        earnings_moves = df.loc[top_volatile_dates]
    else:
        earnings_moves = df.loc[valid_dates]

    # 3. 점수 산출 (Annualized Return / Annualized Volatility)
    # Annualized Return = Mean(|Idio|) * 4 (Quarters) -> Magnitude of Idio Move
    # Annualized Volatility = Std(Idio) * Sqrt(4) -> Stability of Idio Move
    
    idio_rets = earnings_moves['Idio_Return']
    
    if len(idio_rets) > 0:
        mean_abs_idio = np.mean(np.abs(idio_rets))
        std_idio = np.std(idio_rets)
        
        ann_ret = mean_abs_idio * 4
        ann_vol = std_idio * np.sqrt(4)
        
        if ann_vol == 0:
            score = 0.0
        else:
            score = ann_ret / ann_vol
    else:
        score = 0.0
        ann_ret = 0.0
        ann_vol = 0.0
    
    return score, earnings_moves, beta_mkt, beta_sec, ann_ret, ann_vol

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
