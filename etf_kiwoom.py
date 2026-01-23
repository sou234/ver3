
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import os
from typing import Dict, List, Optional
import pytz
import urllib3
import time

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class KiwoomETFMonitor:
    """
    Monitor for Kiwoom KOSEF Active ETF (US Growth 30)
    Target: 459790 (KOSEF 미국성장기업30 Active)
    Source: AJAX API (https://www.kiwoometf.com/service/etf/KO02010200MAjax4)
    """
    
    API_URL = "https://www.kiwoometf.com/service/etf/KO02010200MAjax4"
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://www.kiwoometf.com',
        'Referer': 'https://www.kiwoometf.com/service/etf/KO02010200M?gcode=459790'
    }
    KST = pytz.timezone('Asia/Seoul')
    
    def __init__(self, data_dir: str = "./data_kiwoom"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.etf_code = "459790" 
        self.etf_name = "KOSEF 미국성장기업30 Active"

    def fetch_data_from_api(self, date_str: str) -> pd.DataFrame:
        """
        Fetch portfolio data for a specific date (YYYY-MM-DD) via API.
        """
        # API expects YYYYMMDD
        date_api = date_str.replace("-", "")
        
        payload = {
            "schGubun1": self.etf_code,
            "startDate": date_api
        }
        
        try:
            resp = requests.post(self.API_URL, data=payload, headers=self.HEADERS, verify=False, timeout=10)
            if resp.status_code != 200:
                print(f"[Kiwoom] Status {resp.status_code} for {date_str}")
                return pd.DataFrame()
                
            js = resp.json()
            if 'pdfList' not in js or not js['pdfList']:
                return pd.DataFrame()
            
            data = []
            for item in js['pdfList']:
                # item: itemCode, itemTitle, volume(Shares), assessment(Amt), ratio(Weight)
                # 'gcode' seems to vary? Use itemCode.
                
                # Exclude Cash? ItemTitle '현금' or Code 'CASH...'
                # Timefolio excludes cash from 'Shares' analysis but keeps in Portfolio.
                # We'll keep it but handle in analysis.
                
                try:
                    vol_str = item.get('volume', '0').replace(',', '')
                    amt_str = item.get('assessment', '0').replace(',', '')
                    ratio_str = item.get('ratio', '0').replace('%', '')
                    
                    row = {
                        '종목명': item.get('itemTitle', ''),
                        '종목코드': item.get('itemCode', ''),
                        '보유수량': float(vol_str),
                        '평가금액': float(amt_str),
                        '비중': float(ratio_str),
                        '날짜': date_str
                    }
                    data.append(row)
                except Exception as e:
                    continue
                    
            return pd.DataFrame(data)
            
        except Exception as e:
            print(f"[Kiwoom] API Error: {e}")
            return pd.DataFrame()

    def get_portfolio_data(self, date: str) -> pd.DataFrame:
        """
        Public method to get data. Writes to file cache if new.
        """
        df = self.fetch_data_from_api(date)
        if not df.empty:
            self.save_data(df, date)
        return df

    def save_data(self, df: pd.DataFrame, date: str):
        filename = os.path.join(self.data_dir, f"portfolio_{date}.json")
        df.to_json(filename, orient='records', force_ascii=False, indent=2)
    
    def load_data(self, date: str) -> pd.DataFrame:
        # Check cache first
        filename = os.path.join(self.data_dir, f"portfolio_{date}.json")
        if os.path.exists(filename):
            return pd.read_json(filename)
            
        # If not in cache, try fetching
        df = self.fetch_data_from_api(date)
        if not df.empty:
            self.save_data(df, date)
            return df
        return None

    def get_previous_business_day(self, date_str: str, lookback_days: int = 7) -> Optional[str]:
        """
        Finds the nearest previous date with valid data.
        """
        curr = datetime.strptime(date_str, "%Y-%m-%d")
        
        for i in range(1, lookback_days + 1):
            prev = curr - timedelta(days=i)
            prev_str = prev.strftime("%Y-%m-%d")
            
            # Check if data exists (load or fetch)
            df = self.load_data(prev_str)
            if df is not None and not df.empty:
                return prev_str
                
        return None

    def analyze_rebalancing(self, df_today: pd.DataFrame, df_prev: pd.DataFrame) -> Dict:
        """
        Analyze changes based on SHARES (Trading) and Weights.
        """
        merged = pd.merge(
            df_today[['종목명', '종목코드', '보유수량', '비중']],
            df_prev[['종목명', '종목코드', '보유수량', '비중']],
            on='종목코드',
            how='outer',
            suffixes=('_today', '_prev')
        )
        
        merged['종목명'] = merged['종목명_today'].fillna(merged['종목명_prev'])
        
        # Fill NaNs
        for col in ['보유수량_today', '보유수량_prev', '비중_today', '비중_prev']:
            merged[col] = merged[col].fillna(0)

        # Ignore Cash
        merged = merged[~merged['종목코드'].astype(str).str.contains("CASH", case=False)]
        
        # Calc Deltas
        merged['수량변화'] = merged['보유수량_today'] - merged['보유수량_prev']
        merged['비중변화'] = merged['비중_today'] - merged['비중_prev']
        
        # Thresholds
        share_threshold = 1.0 # At least 1 share change
        
        # Categorize
        new_stocks = merged[(merged['보유수량_prev'] == 0) & (merged['보유수량_today'] > 0)]
        removed_stocks = merged[(merged['보유수량_today'] == 0) & (merged['보유수량_prev'] > 0)]
        
        increased = merged[(merged['수량변화'] >= share_threshold) & (merged['보유수량_prev'] > 0)]
        decreased = merged[(merged['수량변화'] <= -share_threshold) & (merged['보유수량_today'] > 0)]
        
        return {
            'new_stocks': new_stocks.to_dict('records'),
            'removed_stocks': removed_stocks.to_dict('records'),
            'increased_stocks': increased.to_dict('records'),
            'decreased_stocks': decreased.to_dict('records')
        }

if __name__ == "__main__":
    mon = KiwoomETFMonitor()
    today = datetime.now(mon.KST).strftime("%Y-%m-%d")
    print(f"Fetching {today}...")
    df = mon.get_portfolio_data(today)
    print(df.head())
    
    prev = mon.get_previous_business_day(today)
    if prev:
        print(f"Found Prev: {prev}")
        df_p = mon.load_data(prev)
        res = mon.analyze_rebalancing(df, df_p)
        print("New:", len(res['new_stocks']))
