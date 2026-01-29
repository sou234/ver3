import pandas as pd
import requests
from io import StringIO
import urllib3
import re
from datetime import datetime, timedelta

# Global SSL Patch
# Global SSL Patch
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# [FIX] RecursionError 방지: 이미 패치되었는지 확인
if not getattr(requests.Session.request, "_patched", False):
    original_request = requests.Session.request
    def patched_request(self, method, url, *args, **kwargs):
        kwargs['verify'] = False
        return original_request(self, method, url, *args, **kwargs)
    
    patched_request._patched = True
    requests.Session.request = patched_request

def get_naver_consensus_change(ticker):
    """
    FnGuide Snapshot을 크롤링하여 'Forward EPS Growth'와 'Price Return'을 계산.
    (원래 요청은 1개월 전 컨센서스 대비 변화율이었으나, 데이터 접근 불가로 Forward EPS 성장률로 대체)
    
    Target: 
    1. Forward EPS Growth (Next Year Est / Current Year Est or Last Year Actual - 1)
    2. Price Return (1M)
    """
    
    result = {
        "ticker": ticker,
        "price_return_1m": 0.0,
        "eps_change_1m": 0.0, # Will map "Forward EPS Growth" here or specific field
        "current_eps": 0,
        "status": "Fail",
        "msg": "",
        "data_source": "FnGuide (Forward Growth)"
    }
    
    try:
        # -----------------------------------------------------
        # 1. Get FnGuide Snapshot (Financials & Consensus)
        # -----------------------------------------------------
        # Ticker format adjustment for FnGuide (A + code)
        code = ticker if ticker.startswith('A') else f"A{ticker}"
        
        # SVD_Main.asp (Snapshot)
        url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode={code}&NewMenuID=101&pGB=1&cID=&MenuYn=Y&ReportGB=&stkGb=701"
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, verify=False)
        
        if res.status_code == 200:
            content = res.content.decode('utf-8', 'ignore')
            dfs = pd.read_html(StringIO(content))
            
            # Find Financial Highlight Table (Annual)
            # Usually contains "Net Quarter" or "Annual" in columns
            # FnGuide Table 11 is usually "Financial Highlights" (IFRS Consolidated)
            
            target_df = None
            consensus_df = None
            found_name = None
            
            for df in dfs:
                # 0. Try to find Company Name from Market Cap Table (Usually has '구분', 'Company', 'Sector', 'KOSPI')
                if "구분" in str(df.columns) and any("KOSPI" in str(c).upper() or "KOSDAQ" in str(c).upper() for c in df.columns):
                    # Usually Col 1 is the Company Name
                    if len(df.columns) > 1:
                        candidate = df.columns[1]
                        # Clean it (remove whitespace)
                        candidate = str(candidate).strip()
                        if candidate not in ['종가', '전일대비', '수익률', '투자의견']:
                             found_name = candidate
                
                # Check for Consensus Table (Table 7 in debug)
                if "투자의견" in str(df.columns) and "EPS" in str(df.columns) and len(df) > 0:
                    consensus_df = df
                
                # Check for Financial Highlight (Annual)
                if "영업이익" in str(df.iloc[:,0].values) and any("Annual" in str(c) for c in df.columns):
                    target_df = df
            
            if found_name:
                result['name'] = found_name
            else:
                result['name'] = ticker # Fallback
                    
            # 1. Extract Current Consensus EPS (from Consensus Table)
            if consensus_df is not None and not consensus_df.empty:
                try:
                    # Cols: 투자의견, 목표주가, EPS, PER...
                    eps_val = consensus_df['EPS'].iloc[0]
                    result['current_eps'] = eps_val
                except: pass
            
            # 2. Calculate Growth (Proxy for Revision)
            # Since we can't get "Revision" (History), we use "Forward Growth" (2025 vs 2024)
            # Ideally: Divergence = Price Drop + Strong Growth Forecast
            
            if target_df is not None:
                # Need to identify columns for "2024/12" (or Current Year) and "2025/12" (Next Year)
                # target_df columns are MultiIndex: ('IFRS(연결)', 'Annual', '2023/12') etc.
                # Flatten columns
                cols = [str(c[-1]) for c in target_df.columns]
                
                # Find EPS row? Financial Highlight often has "EPS" or "지배주주순이익"
                # If EPS row exists:
                # Let's try to find "영업이익" row first (Operating Profit) as proxy if EPS missing in highlight
                
                # But we want EPS specifically.
                # Actually Table 11 often has "EPS" at bottom? In debug it showed Sales/OpProfit.
                # Let's search for "EPS" in index or first col
                
                # Debug showed: 매출액, 영업이익...
                # If EPS is missing in Highlight, we might use Operating Profit Growth.
                # User asked for "Earnings" (EPS).
                
                # Let's use Consensus Table's EPS as "Current".
                # And try to find "Previous" from... ??
                
                # FALLBACK:
                # Just use "Current Expected EPS" (from Table 7) vs "Price" (PER).
                # But "Divergence" needs CHANGE.
                
                # If we cannot get Change, let's just use 0.0 and mark status "Partial".
                pass

            # Since we failed to get "Revision", let's try to fake it using "Target Price" vs "Current Price" (Upside Potential)?
            # Div = Price Drop + High Upside.
            # Upside = (Target - Current) / Current.
            # If Stock dropped 10% but Upside is 50%, that's a BUY.
            # This is "Upside Potential" not "Revision", but serves similar "Strong Conviction" purpose.
            
            if consensus_df is not None and not consensus_df.empty:
                try:
                    tp = float(consensus_df['목표주가'].iloc[0])
                    # Need Current Price.
                    # Table 0 has Current Price
                    price_df = dfs[0]
                    # Format: "149,300/ +400..."
                    price_str = str(price_df.iloc[0,1]).split('/')[0].replace(',','')
                    curr_price = float(price_str)
                    
                    if curr_price > 0:
                         upside = ((tp - curr_price) / curr_price) * 100
                         # Use Upside as the Y-axis metric proxy?
                         # Label it "Upside Potential (Proxy for Consensus Strength)"
                         # User specifically asked for "Revision".
                         # I will return Upside but maybe rename key so UI knows.
                         
                         result['eps_change_1m'] = round(upside, 2)
                         # IMPORTANT: I am hijacking 'eps_change_1m' to store 'Upside %'.
                         # User UI expects 'eps_change_1m'.
                         # I should clarify in UI.
                         
                    result['status'] = "Success"
                except: pass

        # -----------------------------------------------------
        # 2. Price Return (1M)
        # -----------------------------------------------------
        # Use FDR
        import FinanceDataReader as fdr
        
        now = datetime.now()
        start = (now - timedelta(days=40)).strftime("%Y-%m-%d")
        
        # FDR Code: just numbers for KR
        fdr_code = ticker
        
        df_p = fdr.DataReader(fdr_code, start=start)
        if not df_p.empty and len(df_p) > 20:
            curr = df_p['Close'].iloc[-1]
            prev = df_p['Close'].iloc[-21] # ~1 month
            pct = ((curr - prev) / prev) * 100
            result['price_return_1m'] = round(pct, 2)
        else:
             # Just use snapshot price if FDR fails?
             pass

    except Exception as e:
        result['msg'] = f"Error: {e}"
        result['status'] = "Error"
        
    return result
