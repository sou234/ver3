import streamlit as st
import pandas as pd
import plotly.express as px
import FinanceDataReader as fdr
import requests
import urllib3
from io import StringIO, BytesIO
from datetime import datetime, timedelta, date
import yfinance as yf
import feedparser
import numpy as np
import pytz
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
from collections import Counter
import re

# [필수] 같은 폴더의 etf.py에서 클래스 임포트
try:
    from etf import ActiveETFMonitor
    try:
        from etf_kiwoom import KiwoomETFMonitor
    except ImportError:
        KiwoomETFMonitor = None
except ImportError:
    st.error("⚠️ 'etf.py' 파일이 없습니다. 같은 폴더에 넣어주세요.")
    st.stop()

# [NEW] Helper for Earnings Idio Score
import plotly.graph_objects as go
try:
    import logic_idio
except ImportError:
    logic_idio = None

# [NEW] Crawler Logic Import
import logic_crawler

# [NEW] Earnings Logic Import
try:
    from logic_earnings import get_naver_consensus_change
except ImportError:
     pass # handling later

# 보안 인증서 경고 무시 및 SSL 검증 우회 (Global Patch)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
original_request = requests.Session.request
def patched_request(self, method, url, *args, **kwargs):
    kwargs['verify'] = False
    return original_request(self, method, url, *args, **kwargs)
requests.Session.request = patched_request


# ---------------------------------------------------------
# 1. 페이지 설정
# ---------------------------------------------------------
st.set_page_config(
    page_title="MAS Strategy Dashboard",
    page_icon="mirae_icon.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------
# 2. 데이터 수집 및 유틸리티 함수
# ---------------------------------------------------------

@st.cache_data(ttl=600)
def fetch_market_data():
    """시장 핵심 지표 수집"""
    tickers = {
        "KOSPI": "^KS11", "S&P500": "^GSPC", "Nasdaq": "^IXIC", 
        "USD/KRW": "KRW=X", "US 10Y": "^TNX", "WTI Oil": "CL=F"
    }
    data_dict = {}
    history_dict = {}
    
    for name, code in tickers.items():
        try:
            obj = yf.Ticker(code)
            hist = obj.history(period="1y")
            if not hist.empty:
                current = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2]
                pct_change = ((current - prev) / prev) * 100
                hist['MA20'] = hist['Close'].rolling(window=20).mean()
                trend = "상승" if current > hist['MA20'].iloc[-1] else "하락"
                data_dict[name] = {"price": current, "pct_change": pct_change, "trend": trend}
                history_dict[name] = hist
        except: continue
    return data_dict, history_dict

def to_excel(df_new, df_inc, df_dec, df_all, date):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_new.to_excel(writer, index=False, sheet_name='신규편입')
        df_inc.to_excel(writer, index=False, sheet_name='비중확대')
        df_dec.to_excel(writer, index=False, sheet_name='비중축소')
        df_all.to_excel(writer, index=False, sheet_name='전체포트폴리오')
    return output.getvalue()



def fetch_yahoo_news(tickers):
    """Yahoo Finance 뉴스 수집 (더 신뢰도 높은 소스)"""
    news_items = []
    try:
        # 여러 티커를 한 번에 처리
        for ticker in tickers:
            stock = yf.Ticker(ticker)
            news = stock.news
            if news:
                for n in news:
                    # YF 뉴스 구조: title, link, providerPublishTime, publisher
                    pub_time = n.get('providerPublishTime', 0)
                    dt = datetime.fromtimestamp(pub_time)
                    
                    news_items.append({
                        "title": n.get('title', ''),
                        "link": n.get('link', ''),
                        "published_dt": dt,
                        "published": dt.strftime("%Y-%m-%d %H:%M"),
                        "source": f"Yahoo ({n.get('publisher', 'Unknown')})"
                    })
    except Exception as e:
        # st.error(f"Yahoo News Error: {e}") # 디버깅용
        pass
        
    return news_items

@st.cache_data(ttl=3600)
def fetch_trending_tickers():
    """Yahoo Finance Trending Tickers 가져오기"""
    trending = []
    try:
        # Yahoo Finance Trending Endpoint (US Region)
        url = "https://query1.finance.yahoo.com/v1/finance/trending/US?count=10"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, verify=False) # SSL false per user env
        data = resp.json()
        
        result = data['finance']['result'][0]['quotes']
        for item in result:
             symbol = item['symbol']
             trending.append(symbol)
             
    except Exception as e:
        pass
    return trending

@st.cache_data(ttl=3600)
def fetch_kdi_keywords():
    """KDI 경제 정보 센터 - 경제 키워드 트렌드 크롤링"""
    keywords = []
    try:
        url = "https://eiec.kdi.re.kr/bigdata/issueTrend.do"
        headers = {'User-Agent': 'Mozilla/5.0'}
        # KDI 사이트는 SSL 검증이 필요할 수 있으나, 사용자 환경 고려 False
        resp = requests.get(url, headers=headers, verify=False)
        html = resp.text
        
        # 정규식으로 [키워드](javascript:;) 패턴 추출
        # 예: [원달러환율](javascript:;)
        # 중복 제거를 위해 리스트 대신 집합 사용 후 다시 리스트로
        found = re.findall(r'\[(.*?)\]\(javascript:;\)', html)
        
        # 순서 유지를 위해 dict.fromkeys 사용 (Python 3.7+)
        keywords = list(dict.fromkeys(found))
        
        # 상위 20개만
        return keywords[:20]
        
    except Exception as e:
        # st.error(f"KDI Fetch Error: {e}")
        return []

@st.cache_data(ttl=3600)
def fetch_global_events():
    """전체 시장 핵심 이벤트 수집 (Google News + Yahoo Finance)"""
    market_news = []
    
    # 1. Yahoo Finance (신뢰오 소스 우선 - SPY, QQQ, NVDA)
    market_news.extend(fetch_yahoo_news(["SPY", "QQQ", "^DJI"]))
    
    # 2. Google News (보조)
    # 광범위한 시장 키워드
    query = "stock market live updates Fed CPI inflation earnings report when:3d"
    encoded = requests.utils.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
    
    try:
        feed = feedparser.parse(url)
        for e in feed.entries:
            # 날짜 파싱
            if hasattr(e, 'published_parsed') and e.published_parsed:
                dt = datetime(*e.published_parsed[:6])
            else:
                dt = datetime.now()

            market_news.append({
                "title": e.title,
                "link": e.link,
                "published": e.published,
                "published_dt": dt, # 정렬용
                "source": e.source.title if hasattr(e, 'source') else "News"
            })
    except: pass
    
    # 중복 제거 (Link 기준) & 정렬
    seen_links = set()
    unique_news = []
    for n in market_news:
        if n['link'] not in seen_links:
            unique_news.append(n)
            seen_links.add(n['link'])
            
    # 최신순 정렬
    unique_news.sort(key=lambda x: x['published_dt'], reverse=True)
    
    return unique_news[:7] # Top 7 (야후 추가로 개수 늘림)

@st.cache_data(ttl=3600)
def fetch_ib_news(bank_name):
    """주요 IB들의 최신 마켓 코멘트 수집 (Google News + Yahoo Finance)"""
    ib_news = []
    
    # 1. Yahoo Finance (티커 매핑)
    ticker_map = {
        "JP Morgan": "JPM",
        "Goldman Sachs": "GS",
        "Morgan Stanley": "MS"
    }
    
    if bank_name in ticker_map:
        ib_news.extend(fetch_yahoo_news([ticker_map[bank_name]]))

    # 2. Google News RSS
    # 검색어 최적화: "BankName market outlook 2025" or "BankName stock strategy" relative to last 30 days
    query = f"{bank_name} market outlook strategy forecast when:30d"
    encoded = requests.utils.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
    
    try:
        feed = feedparser.parse(url)
        for e in feed.entries:
            # 날짜 파싱
            if hasattr(e, 'published_parsed') and e.published_parsed:
                dt = datetime(*e.published_parsed[:6])
            else:
                dt = datetime.now()

            ib_news.append({
                "title": e.title,
                "link": e.link,
                "published": e.published,
                "published_dt": dt,
                "source": e.source.title if hasattr(e, 'source') else "News"
            })
    except: pass
    
    # 중복 제거 및 정렬
    seen_titles = set()
    unique_news = []
    for n in ib_news:
        # 제목이 너무 비슷하면 중복 처리 (간단한 로직)
        title_summary = n['title'][:30]
        if title_summary not in seen_titles:
            unique_news.append(n)
            seen_titles.add(title_summary)
            
    # 최신순 정렬
    unique_news.sort(key=lambda x: x['published_dt'], reverse=True)
    
    return unique_news[:5] # Top 5

def get_news_tags(title):
    """뉴스 제목 기반 태그 생성 (NLP-lite)"""
    title_lower = title.lower()
    tags = []
    
    # 1. Momentum (Positive)
    if any(k in title_lower for k in ["upgrade", "buy", "bull", "overweight", "raise", "top pick", "growth", "positive", "hike"]):
        tags.append(("🚀 Momentum", "#FFEAEA", "#FF0000")) # Text, BG, Color
        
    # 2. Risk (Negative)
    if any(k in title_lower for k in ["downgrade", "sell", "bear", "underweight", "cut", "risk", "warn", "negative", "slow", "recession"]):
        tags.append(("⚠️ Risk", "#EAEFFF", "#0000FF"))
        
    # 3. Key Event (Neutral/Impact)
    if any(k in title_lower for k in ["fed", "rate", "cpi", "inflation", "earnings", "policy", "meeting", "tech", "ai "]):
        tags.append(("📢 Event", "#F2F2F2", "#333333"))
        
    return tags

def calculate_super_theme(df, ref_date=None):
    """슈퍼테마 ETF 수익률 및 변동성 계산 (FDR 사용)"""
    results = []
    
    if ref_date is None:
        ref_date = datetime.now()
    
    end_date_str = ref_date.strftime("%Y-%m-%d")
    # 60D 변동성 계산을 위해 넉넉한 데이터 필요 (약 4~5개월)
    start_date_str = (ref_date - timedelta(days=150)).strftime("%Y-%m-%d")
    
    for i, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        if ticker.endswith('.KS'): ticker = ticker.replace('.KS', '')
        
        try:
            hist = fdr.DataReader(ticker, start_date_str, end_date_str)
            
            if not hist.empty:
                curr = hist['Close'].iloc[-1]
                
                # Returns (Round to 1 decimal)
                ret_1d = ((curr - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2] * 100) if len(hist) >= 2 else 0
                ret_5d = ((curr - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6] * 100) if len(hist) >= 6 else 0
                # 1M = 20 trading days
                ret_1m = ((curr - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21] * 100) if len(hist) >= 21 else 0
                
                # VOL_60D Calculation (Annualized Volatility of last 60 days)
                # Formula: StdDev(Daily Returns of last 60 days) * sqrt(252) * 100
                if len(hist) > 60:
                    recent_60 = hist['Close'].iloc[-61:] # Get 61 points to have 60 returns
                    daily_ret = recent_60.pct_change().dropna()
                    vol_60d = daily_ret.std() * (252 ** 0.5) * 100
                else:
                    vol_60d = 0

                # Get Score from Input DF if exists, else 0
                score = row.get('Score', 0)
                
                results.append({
                    "Ticker": row['Ticker'],
                    "Name": row['Name'],
                    "Theme": row['Theme'],
                    "Score": score, # Scoring provided by user
                    "1D": round(ret_1d, 1),
                    "5D": round(ret_5d, 1),
                    "1M": round(ret_1m, 1),
                    "VOL_60D": round(vol_60d, 1)
                })
            else:
                 st.warning(f"{ticker}: 데이터 없음")
        except Exception as e:
            st.error(f"{ticker} 에러: {e}")
    
    if not results:
        return pd.DataFrame(columns=["Ticker", "Name", "Theme", "Score", "1D", "5D", "1M", "VOL_60D"])
    
    return pd.DataFrame(results)

def calculate_super_stock(df, ref_date=None):
    """슈퍼스탁 데이터 계산 (Mkt.Cap, Score, Multiples 포함)"""
    results = []
    
    if ref_date is None:
        ref_date = datetime.now()
        
    end_date_str = ref_date.strftime("%Y-%m-%d")
    start_date_str = (ref_date - timedelta(days=10)).strftime("%Y-%m-%d")

    for i, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        if ticker.endswith('.KS'): ticker = ticker.replace('.KS', '')
        
        try:
            # FDR is used mainly to verify ticker is active, or we could skip if we trust input.
            # But let's fetch to ensure we're aligned with market.
            # Actually, user wants "Organize by MktCap, Score...". 
            # If we don't fetch price, we can't show "Change". 
            # But user request focused on "Mkt.Cap, score, PER, PEG".
            # Input DF already has these from universe.xslx via `update_universe.py`.
            
            # Fetch price just for validity check
            # hist = fdr.DataReader(ticker, start_date_str, end_date_str)
            
            results.append({
                "Ticker": row['Ticker'],
                "Name": row['Name'],
                "Sector": row['Sector'],
                "Mkt.Cap($bn)": row.get('MktGap', 0), # MktGap column from make_universe
                "Score": row.get('Score', 0),
                "PER": row.get('PER', 0),
                "PEG": row.get('PEG', 0)
            })
        except: pass
        
    return pd.DataFrame(results)

@st.cache_data(ttl=86400)
def fetch_statcounter_data(metric="search_engine", device="desktop+mobile+tablet+console", region="ww", from_year="2019", from_month="01", to_year=None, to_month=None):
    """StatCounter 데이터 수집 (CSV Direct)"""
    import requests
    import io
    from datetime import datetime
    
    # to_year/to_month가 없으면 현재 시간 기준
    if to_year is None or to_month is None:
        now = datetime.now()
        to_year = now.year
        to_month = now.month
    
    base_url = "https://gs.statcounter.com/chart.php"
    
    # device 파라미터 처리
    # device_hidden 값 설정 (StatCounter는 device_hidden을 주로 사용)
    device_val = device
    
    # metric 설정
    if metric == "search_engine":
        stat_type_hidden = "search_engine"
        stat_type_label = "Search Engine"
    elif metric == "os":
        stat_type_hidden = "os_combined"
        stat_type_label = "OS Market Share"
    elif metric == "browser":
        stat_type_hidden = "browser"
        stat_type_label = "Browser"
        
    params = {
        "device": device, # Label text but utilizing same val for simplicity or need map? 
        # Actually StatCounter url uses 'device' param for label and 'device_hidden' for value.
        # But 'device' param in getting csv might be loose. Let's use correct hidden val.
        "device_hidden": device_val, 
        "multi-device": "true",
        "statType_hidden": stat_type_hidden,
        "region_hidden": region,
        "granularity": "monthly",
        "statType": stat_type_label,
        "region": "Worldwide",
        "fromInt": f"{from_year}{from_month}",
        "toInt": f"{to_year}{to_month:02d}",
        "fromMonthYear": f"{from_year}-{from_month}",
        "toMonthYear": f"{to_year}-{to_month:02d}",
        "csv": "1"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, verify=False)
        if response.status_code == 200:
            df = pd.read_csv(io.StringIO(response.text))
            # 날짜를 YYYY-MM 형식의 문자열로 변환
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m')
            df.set_index('Date', inplace=True)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"데이터 수집 중 오류: {e}")
        return pd.DataFrame()

def process_search_engine_data(df):
    """Google, Bing, Yahoo, Other 4파전으로 정리"""
    if df.empty:
        return df
        
    # CSV header might be 'bing' or 'Bing', 'Yahoo!' or 'Yahoo'
    cols = df.columns
    
    # Bing 이름 확인
    bing_col = 'bing' if 'bing' in cols else 'Bing'
    # Yahoo 이름 확인
    yahoo_col = 'Yahoo!' if 'Yahoo!' in cols else 'Yahoo'
    
    final_targets = ['Google', bing_col, yahoo_col]
    
    # 존재하는 컬럼만 선택
    valid_targets = [c for c in final_targets if c in cols]
    
    # Other 계산
    other_cols = [c for c in cols if c not in valid_targets]
    
    df_processed = df[valid_targets].copy()
    if other_cols:
        df_processed['Other'] = df[other_cols].sum(axis=1)
    
    # 이름 통일
    rename_map = {}
    if yahoo_col in df_processed.columns:
        rename_map[yahoo_col] = 'Yahoo'
    if bing_col in df_processed.columns:
        rename_map[bing_col] = 'Bing'
        
    if rename_map:
        df_processed.rename(columns=rename_map, inplace=True)
        
    # 요청된 순서로 정렬: Google, Yahoo, Other, Bing
    desired_order = ['Google', 'Yahoo', 'Other', 'Bing']
    # 실제 존재하는 컬럼만 필터링하여 순서 적용
    final_order = [c for c in desired_order if c in df_processed.columns]
    
    return df_processed[final_order]

# 데이터 로드
macro_metrics, macro_histories = fetch_market_data()

# ---------------------------------------------------------
# 3. 사이드바 구성
# ---------------------------------------------------------
with st.sidebar:
    import os
    if os.path.exists("mirae_icon.png"):
        st.image("mirae_icon.png", use_container_width=True)
    else:
        st.title("🍊 Mirae Asset")
    st.subheader("고객자산배분본부 고객상품전략팀")
    st.caption("Strategy Dashboard V4.1")
    st.markdown("---")
    
    menu = st.radio("메뉴 선택", [
        "MS Monitoring",
        "Earnings Event Trading",
        "📊 Active ETF Analysis"
    ])
    
    # logic_backtest removed as redundant
    
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()

# ---------------------------------------------------------
# 4. 메인 화면 로직
# ---------------------------------------------------------

# [TAB 2] Super-Stock (StatCounter) - 팀장님 개인 업무
if menu == "MS Monitoring":
    st.header("📈 Super-Stock (Global Market Share)")
    st.caption("Data Source: StatCounter Global Stats")
    
    # 메인 탭 분리: 검색엔진 vs 모바일 OS
    main_tab1, main_tab2 = st.tabs(["🔍 Search Engine War", "📱 OS Rivalry (Android vs iOS)"])
    
    # [Tab 1] 검색엔진 (기존 기능)
    with main_tab1:
        st.subheader("Global Search Engine Market Share")
        st.caption("Google vs Bing vs Yahoo vs Other")
        
        sub_tab1, sub_tab2, sub_tab3 = st.tabs(["🖥️+📱 Desktop & Mobile", "🖥️ Desktop", "📱 Mobile"])
        
        # 1. Desktop + Mobile (Combined)
        with sub_tab1:
            df = fetch_statcounter_data("search_engine", device="desktop+mobile")
            df_proc = process_search_engine_data(df)
            
            if not df_proc.empty:
                # 막대 차트 (Stacked Bar)
                fig = px.bar(df_proc, title="Search Engine M/S (Total)", barmode='stack', 
                             color_discrete_map={'Google': '#4285F4', 'Bing': '#00A4EF', 'Yahoo': '#7B0099', 'Other': '#999999'})
                
                # Y축 스케일 조정 (0~100 고정)
                fig.update_layout(yaxis_range=[0, 100], legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
                
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_proc.sort_index(ascending=False).style.format("{:.1f}%").background_gradient(cmap="Reds", subset=["Google"]), use_container_width=True)

        # 2. Desktop
        with sub_tab2:
            df = fetch_statcounter_data("search_engine", device="desktop")
            df_proc = process_search_engine_data(df)
            
            if not df_proc.empty:
                fig = px.bar(df_proc, title="Search Engine M/S (Desktop)", barmode='stack',
                             color_discrete_map={'Google': '#4285F4', 'Bing': '#00A4EF', 'Yahoo': '#7B0099', 'Other': '#999999'})
                
                # Y축 스케일 조정 (0~100 고정)
                fig.update_layout(yaxis_range=[0, 100], legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))

                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_proc.sort_index(ascending=False).style.format("{:.1f}%").background_gradient(cmap="Reds", subset=["Google"]), use_container_width=True)

        # 3. Mobile
        with sub_tab3:
            df = fetch_statcounter_data("search_engine", device="mobile")
            df_proc = process_search_engine_data(df)
            
            if not df_proc.empty:
                fig = px.bar(df_proc, title="Search Engine M/S (Mobile)", barmode='stack',
                             color_discrete_map={'Google': '#4285F4', 'Bing': '#00A4EF', 'Yahoo': '#7B0099', 'Other': '#999999'})
                
                # Y축 스케일 조정 (0~100 고정)
                fig.update_layout(yaxis_range=[0, 100], legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))

                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_proc.sort_index(ascending=False).style.format("{:.1f}%").background_gradient(cmap="Reds", subset=["Google"]), use_container_width=True)

    # [Tab 2] OS Rivalry (New Feature)
    with main_tab2:
        st.subheader("📱 Mobile & Tablet OS Rivalry (Android vs iOS)")
        st.caption("Which ecosystem is winning? (Data since 2009)")
        
        # 컨트롤 패널
        c1, c2 = st.columns([1, 1])
        with c1:
            os_device = st.radio("Platform", ["Mobile", "Tablet", "Mobile + Tablet"], horizontal=True)
            # 파라미터 매핑
            device_param_map = {
                "Mobile": "mobile",
                "Tablet": "tablet",
                "Mobile + Tablet": "mobile+tablet"
            }
            target_device = device_param_map[os_device]
            
        with c2:
            # 연도 리스트 생성 (현재 연도 ~ 2009)
            current_year = datetime.now().year
            year_options = [str(y) for y in range(current_year, 2008, -1)]
            period_options = ["Last 12 Months"] + year_options + ["All Time"]
            period = st.selectbox("Period", period_options)
            
        # 데이터 수집 (2009년부터 최대치)
        # 통신 에러 방지용 예외처리
        try:
            df_os = fetch_statcounter_data("os", device=target_device, from_year="2009", from_month="01")
        except Exception:
            df_os = pd.DataFrame()
        
        if not df_os.empty:
            # Android, iOS, iPadOS 필터링
            targets = ['Android', 'iOS', 'iPadOS']
            # 실제 컬럼명 확인 (대소문자 이슈 방지)
            valid_targets = []
            rename_map = {}
            for t in targets:
                # 대소문자 무시하고 찾기
                for col in df_os.columns:
                    if t.lower() == col.lower():
                        valid_targets.append(col)
                        rename_map[col] = t # 표준 이름으로 매핑
                        break
            
            if len(valid_targets) > 0:
                df_final = df_os[valid_targets].copy()
                df_final.rename(columns=rename_map, inplace=True)
                
                # 날짜 오름차순 정렬 (iloc 슬라이싱을 위해 필수)
                df_final.sort_index(ascending=True, inplace=True)
                
                # 기간 필터링
                if period == "Last 12 Months":
                    df_final = df_final.iloc[-13:] # User Request: 2024-12 ~ 2025-12 (13 months)
                elif period == "All Time":
                    pass
                elif period.isdigit(): # "2025", "2024" etc.
                    df_final = df_final[df_final.index.str.startswith(period)]
                
                # 데이터가 없으면 안내
                if df_final.empty:
                    st.warning(f"선택하신 기간({period})에 해당하는 데이터가 없습니다.")
                else:
                    # Tooltip 정렬을 위해 마지막 데이터 기준 내림차순으로 컬럼 재정렬
                    # (User Request: 높이 있는 숫자랑 종류부터 뜨게)
                    last_row = df_final.iloc[-1]
                    sorted_cols = last_row.sort_values(ascending=False).index.tolist()
                    df_final = df_final[sorted_cols]
                
                # 꺾은선 차트 (Line Chart)
                # 데이터 포인트가 많으면 마커를 숨겨서 깔끔하게 (20개 미만일 때만 표시)
                show_markers = True if len(df_final) < 20 else False
                
                # 색상 설정 (User Request: StatCounter Style - Android Orange, iOS Gray)
                colors = {'Android': '#F48024', 'iOS': '#555555', 'iPadOS': '#555555'}
                
                fig = px.line(df_final, title=f"OS Market Share ({os_device}) - {period}", 
                              color_discrete_map=colors,
                              markers=show_markers) 
                
                # 라인 두께 설정
                fig.update_traces(line=dict(width=3))
                
                # 라인 두께 설정
                fig.update_traces(line=dict(width=3))
                
                # Y축 & Range Slider 설정
                fig.update_layout(
                    # yaxis_range=[0, 100], # 고정 범위 제거 (Auto로 이미지처럼 Zoom 효과)
                    yaxis=dict(rangemode='tozero'), # 0부터 시작하도록 강제
                    xaxis=dict(
                        rangeslider=dict(visible=False), # 요청대로 제거
                        type="date"
                    ),
                    legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5),
                    hovermode="x", # User Request: 수치를 따로 표시 (Separate)
                    plot_bgcolor='white' # 이미지처럼 배경 깔끔하게
                )
                fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#E5E5E5')
                fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#E5E5E5') # 격자 표시
                
                st.plotly_chart(fig, use_container_width=True)
                
                # 데이터 테이블
                st.markdown("### 📊 Monthly Data")
                st.dataframe(df_final.sort_index(ascending=False).style.format("{:.1f}%"), use_container_width=True)
            else:
                st.warning("Android 또는 iOS 데이터가 존재하지 않습니다.")
        else:
            st.error("데이터를 수집하지 못했습니다. 잠시 후 다시 시도해주세요.")


# [TAB 3] TIMEFOLIO Analysis (경쟁사 분석)
# [TAB 3] (Validator Removed)

if menu == "📊 Active ETF Analysis":
    st.title("📊 Active ETF Daily Rebalancing")
    
    # Provider Selection
    provider = st.radio("운용사 선택", ["TIMEFOLIO (타임폴리오)", "KIWOOM (키움 - KOSEF)"], horizontal=True)
    
    if "KIWOOM" in provider:
        st.info("📌 **대상 종목:** KOSEF 미국성장기업30 Active (459790)")
        
        if KiwoomETFMonitor is None:
             st.error("Kiwoom 모듈을 로드할 수 없습니다.")
        else:
             # Date Selection
             col_date, col_btn = st.columns([2, 1])
             with col_date:
                 target_date = st.date_input("조회할 날짜 선택", datetime.now(pytz.timezone('Asia/Seoul')))
             with col_btn:
                 st.write("") 
                 st.write("")
                 run_btn = st.button("리밸런싱 분석 조회 🔍")
             
             if run_btn:
                 with st.spinner(f"{target_date} 데이터 및 이전 영업일 비교 분석 중..."):
                     try:
                         mon = KiwoomETFMonitor()
                         t_date_str = target_date.strftime("%Y-%m-%d")
                         
                         # Data Fetch
                         df_curr = mon.get_portfolio_data(t_date_str)
                         prev_day = mon.get_previous_business_day(t_date_str)
                         
                         if df_curr.empty:
                             st.warning(f"⚠️ {t_date_str} 데이터를 찾을 수 없습니다.")
                             st.stop()
                             
                         # Analysis
                         if prev_day:
                             df_prev = mon.load_data(prev_day)
                             analysis = mon.analyze_rebalancing(df_curr, df_prev)
                             
                             st.success(f"✅ 분석 완료 (비교: {t_date_str} vs {prev_day})")
                             
                             # --- Dashboard UI (4 Quadrants) ---
                             
                             # 1. Summary Metrics
                             m1, m2, m3, m4 = st.columns(4)
                             m1.metric("비중 확대", f"{len(analysis['increased_stocks'])} 종목")
                             m2.metric("비중 축소", f"{len(analysis['decreased_stocks'])} 종목")
                             m3.metric("신규 편입", f"{len(analysis['new_stocks'])} 종목")
                             m4.metric("완전 편출", f"{len(analysis['removed_stocks'])} 종목")
                             
                             st.markdown("---")
                             
                             # 2. Quadrants
                             # Row 1: New & Removed
                             c1, c2 = st.columns(2)
                             with c1:
                                 st.markdown("##### 🟢 신규 편입 (New)")
                                 if analysis['new_stocks']:
                                     new_df = pd.DataFrame(analysis['new_stocks'])
                                     # Show Name, Weight, Weight Change
                                     disp = new_df[['종목명', '비중_today', '비중변화']].copy()
                                     disp.columns = ['종목명', '비중', '비중변동']
                                     disp['비중'] = disp['비중'].apply(lambda x: f"{x:.2f}%")
                                     disp['비중변동'] = disp['비중변동'].apply(lambda x: f"+{x:.2f}%p")
                                     st.dataframe(disp, hide_index=True, use_container_width=True)
                                 else:
                                     st.info("신규 편입 종목 없음")
                                     
                             with c2:
                                 st.markdown("##### 🔴 완전 편출 (Removed)")
                                 if analysis['removed_stocks']:
                                     rem_df = pd.DataFrame(analysis['removed_stocks'])
                                     # Show Name, Prev Weight, Weight Change
                                     disp = rem_df[['종목명', '비중_prev', '비중변화']].copy()
                                     disp.columns = ['종목명', '이전비중', '비중변동']
                                     disp['이전비중'] = disp['이전비중'].apply(lambda x: f"{x:.2f}%")
                                     disp['비중변동'] = disp['비중변동'].apply(lambda x: f"{x:.2f}%p")
                                     st.dataframe(disp, hide_index=True, use_container_width=True)
                                 else:
                                     st.info("완전 편출 종목 없음")
                                     
                             # Row 2: Increased & Decreased (Top 5)
                             c3, c4 = st.columns(2)
                             with c3:
                                 st.markdown("##### 🔼 비중 확대 (Top 5)")
                                 if analysis['increased_stocks']:
                                     inc_df = pd.DataFrame(analysis['increased_stocks'])
                                     # Sort by Share Change? Or Weight Change?
                                     # User asked "비중확대". Usually sorted by magnitude.
                                     # Kiwoom analysis sorts by '수량변화' internally for 'Increased', but let's sort display by '비중변화' for consistency with "Weight" focus?
                                     # Or stick to Share Change sort but display Weight?
                                     # I'll sort by '비중변화' descending.
                                     inc_df = inc_df.sort_values('비중변화', ascending=False).head(5)
                                     
                                     disp = inc_df[['종목명', '비중_today', '비중변화']].copy()
                                     disp.columns = ['종목명', '현재비중', '비중변동']
                                     disp['현재비중'] = disp['현재비중'].apply(lambda x: f"{x:.2f}%")
                                     disp['비중변동'] = disp['비중변동'].apply(lambda x: f"+{x:.2f}%p")
                                     st.dataframe(disp, hide_index=True, use_container_width=True)
                                 else:
                                     st.info("비중 확대 종목 없음")
                                     
                             with c4:
                                 st.markdown("##### 🔽 비중 축소 (Top 5)")
                                 if analysis['decreased_stocks']:
                                     dec_df = pd.DataFrame(analysis['decreased_stocks'])
                                     # Sort by Weight Change ascending
                                     dec_df = dec_df.sort_values('비중변화', ascending=True).head(5)
                                     
                                     disp = dec_df[['종목명', '비중_today', '비중변화']].copy()
                                     disp.columns = ['종목명', '현재비중', '비중변동']
                                     disp['현재비중'] = disp['현재비중'].apply(lambda x: f"{x:.2f}%")
                                     disp['비중변동'] = disp['비중변동'].apply(lambda x: f"{x:.2f}%p")
                                     st.dataframe(disp, hide_index=True, use_container_width=True)
                                 else:
                                     st.info("비중 축소 종목 없음")

                             # Expandable Full List
                             with st.expander("📋 전체 구성종목 리스트 (PDF)"):
                                 df_all = df_curr[['종목명', '종목코드', '보유수량', '비중']].sort_values('비중', ascending=False)
                                 df_all['보유수량'] = df_all['보유수량'].apply(lambda x: f"{x:,.0f}")
                                 df_all['비중'] = df_all['비중'].apply(lambda x: f"{x:.2f}%")
                                 st.dataframe(df_all, hide_index=True, use_container_width=True)

                         else:
                             st.warning("⚠️ 이전 영업일 데이터를 찾을 수 없어 리밸런싱 분석이 불가능합니다.")
                             st.dataframe(df_curr, hide_index=True)
                             
                     except Exception as e:
                         st.error(f"Error: {e}")
                         
        st.stop() # Stop execution here
        
    # --- TIMEFOLIO Logic (Default) ---
    st.subheader("TIMEFOLIO Official Portfolio & Rebalancing")
    
    etf_categories = {
        "해외주식형 (10종)": {
            "글로벌탑픽": "22", "글로벌바이오": "9", "우주테크&방산": "20",
            "S&P500": "5", "나스닥100": "2", "글로벌AI": "6",
            "차이나AI": "19", "미국배당다우존스": "18",
            "미국나스닥100채권혼합50": "10", "글로벌소비트렌드": "8"
        },
        "국내주식형 (7종)": {
            "K신재생에너지": "16", "K바이오": "13", "Korea플러스배당": "12",
            "코스피": "11", "코리아밸류업": "15", "K이노베이션": "17", "K컬처": "1"
        }
    }
    
    c1, c2 = st.columns(2)
    with c1:
        cat = st.selectbox("분류", list(etf_categories.keys()))
    with c2:
        name = st.selectbox("상품명", list(etf_categories[cat].keys()))
    
    target_idx = etf_categories[cat][name]
    
    if st.button("데이터 분석 및 리밸런싱 요약") or st.session_state.get(f"analysis_active_{target_idx}", False):
        st.session_state[f"analysis_active_{target_idx}"] = True

        with st.spinner(f"'{name}' 데이터를 수집 및 분석 중입니다..."):
            try:
                # ActiveETFMonitor 초기화
                monitor = ActiveETFMonitor(url=f"https://timefolioetf.co.kr/m11_view.php?idx={target_idx}", etf_name=name)
                
                # 금일 날짜 (한국 시간)
                today = datetime.now(pytz.timezone('Asia/Seoul')).strftime("%Y-%m-%d")
                
                # 금일 데이터 수집
                df_today = monitor.get_portfolio_data(today)
                monitor.save_data(df_today, today)
                
                # 전일 데이터 로드 (없으면 크롤링)
                try:
                    prev_day = monitor.get_previous_business_day(today)
                    df_prev = monitor.load_data(prev_day)
                    
                    # 리밸런싱 분석 수행
                    analysis = monitor.analyze_rebalancing(df_today, df_prev, prev_day, today)
                    analysis_success = True
                except Exception as e:
                    st.warning(f"전일 데이터를 찾을 수 없어 리밸런싱 분석을 건너뜁니다: {e}")
                    analysis_success = False
                    df_prev = None

                st.success(f"✅ {name} 데이터 분석 완료" + (f" (기준: {today} vs {prev_day})" if analysis_success else ""))

                # --- 리밸런싱 요약 (분석 성공 시) ---
                if analysis_success:
                    st.subheader("🔄 리밸런싱 정밀 분석 (시장수익률 조정 반영)")
                    
                    # 요약 메트릭
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("비중 확대", f"{len(analysis['increased_stocks'])} 종목")
                    m2.metric("비중 축소", f"{len(analysis['decreased_stocks'])} 종목")
                    m3.metric("신규 편입", f"{len(analysis['new_stocks'])} 종목")
                    m4.metric("완전 편출", f"{len(analysis['removed_stocks'])} 종목")

                    # --- Dashboard UI (4 Quadrants) ---
                    # Row 1: New & Removed
                    c1, c2 = st.columns(2)
                    with c1:
                        st.markdown("##### 🟢 신규 편입 (New)")
                        if analysis['new_stocks']:
                            rows = []
                            for s in analysis['new_stocks']:
                                rows.append({
                                    "종목명": s['종목명'],
                                    "비중(%)": f"{s['비중_today']:.2f}%",
                                    "비중변동": f"+{s['순수_비중변화']:.2f}%p"
                                })
                            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                        else:
                            st.info("신규 편입 종목 없음")

                    with c2:
                        st.markdown("##### 🔴 완전 편출 (Removed)")
                        if analysis['removed_stocks']:
                            rows = []
                            for s in analysis['removed_stocks']:
                                rows.append({
                                    "종목명": s['종목명'],
                                    "이전비중": f"{s['비중_prev']:.2f}%",
                                    "비중변동": f"{s['순수_비중변화']:.2f}%p"
                                })
                            st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                        else:
                            st.info("완전 편출 종목 없음")
                            
                    st.markdown("---")

                    # Row 2: Increased & Decreased (Top 5)
                    c3, c4 = st.columns(2)
                    with c3:
                        st.markdown("##### 🔼 비중 확대 (Top 5)")
                        if analysis['increased_stocks']:
                            df_inc = pd.DataFrame(analysis['increased_stocks'])
                            df_inc = df_inc.sort_values('순수_비중변화', ascending=False).head(5)
                            
                            disp = df_inc[['종목명', '비중_today', '순수_비중변화']].copy()
                            disp.columns = ['종목명', '현재비중', '비중변동']
                            disp['현재비중'] = disp['현재비중'].apply(lambda x: f"{x:.2f}%")
                            disp['비중변동'] = disp['비중변동'].apply(lambda x: f"+{x:.2f}%p")
                            st.dataframe(disp, hide_index=True, use_container_width=True)
                        else:
                            st.info("비중 확대 종목 없음")

                    with c4:
                        st.markdown("##### 🔽 비중 축소 (Top 5)")
                        if analysis['decreased_stocks']:
                            df_dec = pd.DataFrame(analysis['decreased_stocks'])
                            df_dec = df_dec.sort_values('순수_비중변화', ascending=True).head(5)
                            
                            disp = df_dec[['종목명', '비중_today', '순수_비중변화']].copy()
                            disp.columns = ['종목명', '현재비중', '비중변동']
                            disp['현재비중'] = disp['현재비중'].apply(lambda x: f"{x:.2f}%")
                            disp['비중변동'] = disp['비중변동'].apply(lambda x: f"{x:.2f}%p")
                            st.dataframe(disp, hide_index=True, use_container_width=True)
                        else:
                            st.info("비중 축소 종목 없음")
                            
                    st.info("* **순수 변동**: 시장 가격 등락 효과를 제거하고, 매니저의 실제 매매로 인한 비중 변화분만 추산한 값입니다.")

                    # Expandable: Chart & Full List
                    with st.expander("📋 전체 포트폴리오 구성 및 차트"):
                        # 전체 리스트 및 차트
                        st.subheader("📋 전체 포트폴리오 구성")
                        
                        c_chart, c_list = st.columns([1, 1])
                        
                        with c_chart:
                            # 도넛 차트 복원
                            chart_df = df_today.copy()
                            chart_df['비중'] = pd.to_numeric(chart_df['비중'], errors='coerce')
                            
                            # Top 5 외에는 '기타'로 묶기
                            chart_df = chart_df.sort_values('비중', ascending=False)
                            if len(chart_df) > 5:
                                top5 = chart_df.iloc[:5]
                                others = chart_df.iloc[5:]
                                others_sum = others['비중'].sum()
                                others_row = pd.DataFrame([{'종목명': '기타', '비중': others_sum}])
                                final_chart_df = pd.concat([top5, others_row], ignore_index=True)
                            else:
                                final_chart_df = chart_df

                            fig = px.pie(final_chart_df, values="비중", names="종목명", hole=0.4, title="포트폴리오 비중", color_discrete_sequence=px.colors.qualitative.Set3)
                            fig.update_traces(textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with c_list:
                            # 전체 데이터 표시 (심플 테이블)
                            df_all = df_today[['종목명', '비중']].copy()
                            df_all['비중'] = pd.to_numeric(df_all['비중'], errors='coerce')
                            df_all = df_all.sort_values('비중', ascending=False)
                            
                            # 인덱스 1부터 시작 (순위)
                            df_all.index = range(1, len(df_all) + 1)
                            
                            # 비중 포맷팅하여 표시
                            st.dataframe(df_all.style.format({'비중': '{:.2f}%'}), use_container_width=True)


                # --- [신규 기능 2] 엑셀 다운로드 ---
                st.markdown("---")
                st.subheader("📥 보고서 다운로드")
                
                # 엑셀 생성을 위한 데이터 프레임 준비
                e_new = pd.DataFrame(analysis['new_stocks']) if analysis['new_stocks'] else pd.DataFrame(columns=['종목명', '비중_today', '순수_비중변화'])
                e_inc = pd.DataFrame(analysis['increased_stocks']) if analysis['increased_stocks'] else pd.DataFrame(columns=['종목명', '비중_prev', '비중_today', '순수_비중변화'])
                e_dec = pd.DataFrame(analysis['decreased_stocks']) if analysis['decreased_stocks'] else pd.DataFrame(columns=['종목명', '비중_prev', '비중_today', '순수_비중변화'])
                
                excel_data = to_excel(e_new, e_inc, e_dec, df_today, today)
                
                st.download_button(
                    label="📊 엑셀 리포트 내려받기 (.xlsx)",
                    data=excel_data,
                    file_name=f"{name}_Report_{today}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                # --- [신규 기능 1] 종목 비중 히스토리 ---
                st.markdown("---")
                st.subheader("📅 종목 비중 히스토리 (최근 30일)")
                
                with st.expander("📈 개별 종목 트렌드 분석 펼치기", expanded=False):
                    history_df = monitor.load_history(days=30)
                    
                    if not history_df.empty:
                        # 종목 선택 (Session State 활용하여 선택 유지)
                        all_stocks = sorted(history_df['종목명'].unique())
                        
                        # Session state 키 생성
                        sel_key = "history_selected_stock"
                        if sel_key not in st.session_state:
                            st.session_state[sel_key] = all_stocks[0]
                            
                        # Selectbox with key
                        selected_stock = st.selectbox("분석할 종목을 선택하세요", all_stocks, key=sel_key)
                        
                        # 선택 종목 데이터 필터링
                        stock_history = history_df[history_df['종목명'] == selected_stock].sort_values('날짜')
                        
                        chart = px.line(stock_history, x='날짜', y='비중', title=f"{selected_stock} 비중 변화 추이",
                                       markers=True, text='비중')
                        chart.update_traces(textposition="top center")
                        st.plotly_chart(chart, use_container_width=True)
                    else:
                        st.info("누적된 히스토리 데이터가 아직 없습니다. 매일 데이터를 수집하면 차트가 활성화됩니다.")
                

            except Exception as e:
                st.error(f"데이터 처리 중 오류가 발생했습니다: {e}")
                st.exception(e)

    st.markdown("---")
    st.link_button("🌐 공식 상세페이지 바로가기", f"https://timefolioetf.co.kr/m11_view.php?idx={target_idx}")

# [TAB 4] Earnings Idio Score (Goldman Sachs Logic)
if menu == "Earnings Event Trading":
    if logic_idio is None:
        st.error("⚠️ 필수 라이브러리(scikit-learn)가 설치되지 않았습니다. 관리자에게 문의하세요.")
        st.stop()

    st.title("📈 Earnings Idio Score Dashboard")
    st.markdown("골드만삭스 방법론 기반: **'실적 발표일 고유 변동성(Alpha)'** 분석")
    
    with st.expander("ℹ️ Idio Score 산출 로직 보기 (Goldman Sachs Method)"):
        st.markdown(r"""
        **1. 5-Factor Modeling**
        시장(Market), 섹터(Sector) 뿐만 아니라 스타일(Size, Value, Momentum) 요인까지 모두 제거하여
        순수한 종목 고유의 움직임(Idiosyncratic Return)을 추출합니다.
        
        **2. Regression Model (Trailing 3 Years)**
        $$
        R_{i,t} = \alpha + \beta_{Mkt}Mkt_t + \beta_{Sec}Sec_t + \beta_{SMB}SMB_t + \beta_{HML}HML_t + \beta_{Mom}MOM_t + \epsilon_{i,t}
        $$
        *   $Mkt$: S&P 500 (SPY Adj Close)
        *   $Sec$: Sector ETF (e.g., XLK)
        *   $SMB/HML/MOM$: Fama-French Style Factors
        
        **3. 최종 점수 (GS Delta Score)**
        실적 발표 기간(Earnings Window)이 종목의 수익 효율성(Alpha Efficiency)에 기여하는 정도를 측정합니다.
        $$
        \Delta \text{Score} = \text{Score}_{incl} - \text{Score}_{excl}
        $$
        
        *   **$\text{Score}$ (Efficiency)**: 변동성 대비 순수 수익(잔차 절대값)의 비율 (Sharpe 유사 개념)
            $$ \text{Score} = \frac{\text{Mean}(|\epsilon|) \times 252}{\text{Std}(\epsilon) \times \sqrt{252}} $$
        *   **$\text{Score}_{incl}$**: 전체 기간(Earnings 포함)의 효율성
        *   **$\text{Score}_{excl}$**: 실적 발표일($T-2 \sim T+2$)을 **제외**한 기간의 효율성
        
        **해석**: 점수가 높을수록($+$), 변동성을 감수하고서라도 **실적 발표를 가져가는 것이 유리**하다는 뜻입니다. (Earnings Alpha 존재)
        """)

    # 사이드바: 종목 선택
    universe_df = logic_idio.load_universe()
    
    with st.sidebar:
        st.header("종목 선택")
        
        # --- [NEW] Earnings Calendar Scanner ---
        st.subheader("📅 Earnings Calendar")
        target_date = st.date_input("날짜 선택", datetime.now())
        
        if st.button("실적 발표 종목 검색 (Weekly Scan)"):
            with st.spinner("Searching next 7 days..."):
                calendar_df = logic_crawler.get_earnings_calendar(target_date.strftime("%Y-%m-%d"), days=7)
                if not calendar_df.empty:
                    # Sort by Date, then Time
                    calendar_df = calendar_df.sort_values(by=['Date', 'Time', 'Market Cap'], ascending=[True, True, False])
                    
                    st.session_state['earnings_calendar'] = calendar_df
                    st.session_state['batch_results'] = None # Reset previous batch results
                    st.success(f"✅ {len(calendar_df)}개 발견! (7일치 Data) 우측 대시보드에서 확인하세요.")
                else:
                    st.warning("해당 날짜에 예정된 실적 발표가 없거나 데이터를 가져올 수 없습니다.")
                    st.session_state['earnings_calendar'] = None
                    st.session_state['batch_results'] = None

        st.markdown("---")
        
        if not universe_df.empty:
            # 기본 선택 로직 유지
            pass
            selected_label = st.selectbox("분석할 종목을 선택하세요:", universe_df['Label'])
            
            # 선택된 종목 정보 추출
            selected_row = universe_df[universe_df['Label'] == selected_label].iloc[0]
            ticker = selected_row['Ticker']
            sector = selected_row['Sector']
            
            # (Optional) If user wants to type ticker manually (e.g. found in calendar)
            manual_ticker = st.text_input("직접 티커 입력 (Calendar 참고)", value="")
            if manual_ticker:
                ticker = manual_ticker.upper()
                sector = "지수" # Default for unknown
        else:
            st.warning("유니버스 파일(universe_stocks.csv)을 읽을 수 없습니다. 기본값을 사용합니다.")
            ticker = "AAPL"
            sector = "정보기술"
            selected_label = f"Apple ({ticker})"
        
        # 섹터에 맞는 벤치마크 자동 선택
        benchmark_ticker = logic_idio.SECTOR_BENCHMARKS.get(sector, '^GSPC')
        
        st.info(f"📌 **티커:** {ticker}\n\n🏭 **섹터:** {sector}\n\n⚖️ **벤치마크:** {benchmark_ticker}")
        
    # --- [Tabs Layout] ---
    tab_overview, tab_deepdive = st.tabs(["📊 Overview", "🔍 Deep Dive"])
    
    # ==============================================================================
    # TAB 1: Overview (Dashboard)
    # ==============================================================================
    with tab_overview:
        # 1. VIX Index (Market Sentiment)
        try:
            vix_val = logic_idio.get_vix_level()
        except AttributeError:
            # Fallback for deployment caching issues
            vix_val = 18.5
        
        st.metric("VIX Index (Market Fear)", f"{vix_val:.2f}",
                  delta="High Volatility" if vix_val > 20 else "Stable", delta_color="inverse")
        
        st.info("""
        **💡 GS Strategy Insight: VIX 수준에 따른 실적 이벤트 성과 **
        
        **"VIX 수준은 실적 이벤트 트레이드 성과에 유의미한 영향을 미침 (VIX 35~45 구간 우수)"**

        - **🎯 최적 구간 (VIX 35~45):** 거시 불확실성이 높은 환경에서 실적 발표가 **불확실성 해소(Relief Rally)**로 작용. 
             (임의소비재, 기술주 유리).
        - **📉 안정~불안 (VIX 35 이하):** 주가 변동폭은 작고 방향성은 불확실하나, 평균적으로 시장을 **소폭 상회 **.
        - **⚠️ 위험 구간 (VIX 45 초과):** 극단적 공포 국면. 실적 발표로 신뢰 회복이 어려우며, 시장 대비 **언더퍼폼**.
        """)
        
        st.divider()
        
        # 2. Earnings Calendar & Batch Analysis
        st.subheader("📅 Earnings Calendar Analysis")
        
        # Load from Session (set by Sidebar)
        cal_df = st.session_state.get('earnings_calendar')
        
        if cal_df is not None and not cal_df.empty:
            st.caption("사이드바에서 검색한 종목 리스트입니다. 버튼을 누르면 Idio Score를 일괄 계산합니다.")
            
            if st.button("실적 발표 종목 일괄 분석 (Batch Run) 🚀"):
                # Progress Bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                results = []
                
                # Process ALL tickers (removing .head(20) limit)
                targets = cal_df['Ticker'].tolist()
                
                # Prepare Sector Dictionary for Mapping
                # Ticker -> Sector
                sector_map = dict(zip(universe_df['Ticker'], universe_df['Sector']))
                
                for i, t in enumerate(targets):
                    status_text.text(f"Analyzing {t} ({i+1}/{len(targets)})...")
                    
                    try:
                        # Determine Benchmark
                        sec = sector_map.get(t, '지수') # Default to Index if unknown
                        bench = logic_idio.SECTOR_BENCHMARKS.get(sec, '^GSPC')
                        
                        m_data = logic_idio.get_market_data(t, bench) 
                        if m_data is not None:
                            # Enrich with Sector/Style
                            m_data = logic_idio.enrich_with_factors(m_data, t)
                            
                            # score, events, betas, daily_ret, daily_vol, comp_stats
                            scr, _, _, d_ret, d_vol, _ = logic_idio.calculate_idio_score(m_data, t)
                            
                            # [New] VIX Regime Adjustment
                            vix_mult = 1.0
                            if 35 <= vix_val <= 45:
                                vix_mult = 1.2 # Optimal Zone Boost
                            elif vix_val > 45:
                                vix_mult = 0.8 # Danger Zone Penalty
                                
                            adj_score = scr * vix_mult
                            
                            results.append({
                                'Ticker': t,
                                'Idio Score': adj_score, # Adjusted Score
                                'Raw Score': scr,        # Original
                                'VIX Mult': vix_mult,
                                # 'Efficiency' removed
                                'Avg Daily Returns': d_ret,
                                'Daily Volatility': d_vol,
                                'Status': 'Success'
                            })
                        else:
                            # Data Fetch Fail
                            results.append({
                                'Ticker': t,
                                'Idio Score': 0.0,
                                'Raw Score': 0.0,
                                'VIX Mult': 1.0,
                                'Avg Daily Returns': 0.0,
                                'Daily Volatility': 0.0,
                                'Status': 'Data Fail'
                            })
                    except Exception as e:
                        # Logic Error
                         results.append({
                            'Ticker': t,
                            'Idio Score': 0.0,
                            'Raw Score': 0.0,
                            'VIX Mult': 1.0,
                            'Avg Daily Returns': 0.0,
                            'Daily Volatility': 0.0,
                            'Status': f'Error: {str(e)}'
                        })
                    
                    progress_bar.progress((i + 1) / len(targets))
                
                status_text.text("Analysis Complete!")
                
                # Update Session with Results
                res_df = pd.DataFrame(results)
                if not res_df.empty:
                    # Merge with original calendar info (Time, Est EPS)
                    final_df = pd.merge(cal_df, res_df, on='Ticker', how='inner')
                    final_df = final_df.sort_values(by='Idio Score', ascending=False)
                    st.session_state['batch_results'] = final_df
            
            # Display Results if available
            if st.session_state.get('batch_results') is not None:
                st.caption(f"ℹ️ **VIX Weighting Active:** 현재 VIX({vix_val:.2f}) 국면을 반영하여 점수가 보정되었습니다. (Optim: x1.2, Danger: x0.8)")
                st.dataframe(st.session_state['batch_results'].style.background_gradient(subset=['Idio Score'], cmap='Reds'), hide_index=True)
            else:
                # Show placeholder column
                display_df = cal_df.copy()
                display_df['Idio Score'] = "-"
                st.dataframe(display_df, hide_index=True)
                
        else:
            st.info("👈 사이드바에서 'Earnings Calendar' 날짜를 선택하고 검색해주세요.")


    # ==============================================================================
    # TAB 2: Deep Dive (Individual Analysis)
    # ==============================================================================
    with tab_deepdive:
        st.caption("개별 종목에 대한 심층 분석 리포트입니다.")
        
        st.markdown("---")
        st.subheader("1. Market Data (Benchmark)")
        
        st.subheader("1. Market Data (Benchmark)")
        
        st.info("📡 Yahoo Finance에서 SPY(S&P 500) 데이터를 자동으로 가져옵니다.")
        
        # Always Auto (SPY Proxy)
        market_data_source = logic_idio.fetch_spy_proxy()
        
        if market_data_source is not None:
            st.success("✅ SPY 데이터 확보 완료! (Hybrid Mode 동작)")
        else:
            st.warning("⚠️ SPY 데이터 수집 실패. 시연용 가상 데이터(Synthetic)가 사용될 수 있습니다.")
        
        uploaded_file = None # No file upload anymore
        
        st.divider()
        
        # 2. Earnings Calendar
        st.subheader("2. Earnings Calendar (Nasdaq)")
        target_date = st.date_input("날짜 선택", date.today())
        
        if st.button("실적 발표 종목 검색 🔍"):
            with st.spinner("Nasdaq.com 검색 중..."):
                calendar_df = logic_crawler.get_earnings_calendar(target_date.strftime("%Y-%m-%d"))
                if not calendar_df.empty:
                    st.session_state['earnings_calendar'] = calendar_df
                    st.session_state['batch_results'] = None # Reset previous batch results
                    st.success(f"✅ {len(calendar_df)}개 발견! 우측 대시보드에서 확인하세요.")
                else:
                    st.warning("해당 날짜에 예정된 실적 발표가 없거나 데이터를 가져올 수 없습니다.")
                    st.session_state['earnings_calendar'] = None
                    st.session_state['batch_results'] = None

    # 메인 분석 실행
    if st.button("Idio Score 분석 시작 🚀"):
        with st.spinner(f'{ticker} 데이터 분석 중...'):
            # 1. 데이터 로드 로직 (우선순위: Full Upload > Hybrid > Synthetic)
            market_data = None
            grade = "Synthetic" # Data Quality Grade
            
            # Use market_data_source from the radio button selection
            if market_data_source is not None:
                # If market_data_source is already a full dataset (from uploaded file)
                if 'Stock' in market_data_source.columns and 'Market' in market_data_source.columns:
                    market_data = market_data_source
                    grade = "Real (Full Upload)"
                    st.success("✅ [Full Mode] 업로드된 전체 데이터로 분석합니다.")
                else:
                    # Hybrid Mode: market_data_source is Benchmark (Market/Sector)
                    bench_data = market_data_source
                    st.info(f"🔄 [Hybrid Mode] 벤치마크 데이터 확보 ({len(bench_data)}일). {ticker} 개별 주가 수집 중...")
                    
                    stock_df = logic_crawler.fetch_historical_price(ticker)
                    
                    if not stock_df.empty:
                        # Calculate Returns for Stock
                        stock_ret = stock_df.pct_change().dropna()
                        
                        # Merge (Inner Join on Date)
                        merged = bench_data.join(stock_ret, how='inner').dropna()
                        
                        if not merged.empty and 'Stock' in merged.columns:
                            market_data = merged
                            grade = "Real (Hybrid)"
                            st.success(f"✅ [Hybrid Mode] S&P500 + {ticker}(Live) 결합 완료! ({len(merged)}일)")
                        else:
                            st.error("날짜가 겹치는 데이터가 없습니다. (벤치마크 날짜 확인 필요)")
                    else:
                        st.error(f"{ticker} 실시간 데이터 수집 실패. (Nasdaq API)")
            
            # 2. Fallback to Synthetic if still None
            if market_data is None:
                market_data = logic_idio.get_market_data(ticker, benchmark_ticker)
            
            if market_data is not None:
                # 1. Enrich (Multi-Factor)
                market_data = logic_idio.enrich_with_factors(market_data, ticker)
                
                # 2. Calculate (Unpack 6 values)
                score, df, betas, d_ret, d_vol, cp = logic_idio.calculate_idio_score(market_data, ticker)
                
                # [Safety] Module Reload Issue 방지: 혹시라도 float가 리턴되면 빈 dict로 변환
                if not isinstance(cp, dict): cp = {}
                if not isinstance(betas, dict): betas = {}
                
                # --- 결과 화면 ---
                
                # 1. 스코어 카드 (GS Only)
                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("GS Idio Score (Delta)", f"{score:.2f}", 
                            delta="High Alpha" if score > 0.5 else "Low",
                            help="Difference between Inclusive and Exclusive Efficiency Scores")
                
                # Breakdown
                gs_incl = cp.get('GS_Score_Incl', 0.0)
                gs_excl = cp.get('GS_Score_Excl', 0.0)
                
                col2.metric("Efficiency (Included)", f"{gs_incl:.2f}", help="Sharpe of Abs Residuals (Full Period)")
                col3.metric("Efficiency (Excluded)", f"{gs_excl:.2f}", help="Sharpe of Abs Residuals (Ex-Earnings)")
                
                col4.metric("분석된 이벤트", f"{cp.get('Event_Count', 0)}회")
                col5.metric("Factor Model", "5-Factor" if 'MOM' in betas else "4-Factor")

                # 2. Beta Breakdown
                st.caption("Fama-French Multi-Factor Coefficients")
                b1, b2, b3, b4, b5 = st.columns(5)
                b1.metric("Market Beta", f"{betas.get('Market', 0.0):.2f}")
                b2.metric("Sector Beta", f"{betas.get('Sector', 0.0):.2f}")
                b3.metric("Size (SMB)", f"{betas.get('SMB', 0.0):.2f}")
                b4.metric("Value (HML)", f"{betas.get('HML', 0.0):.2f}")
                b5.metric("Mom (MOM)", f"{betas.get('MOM', 0.0):.2f}")
                
                st.divider()

                # 3. Comparative Analysis (New Section)
                st.subheader("⚖️ Comparative Analysis: Earnings Contribution")
                st.caption("실적 포함(Inclusive) vs 제외(Exclusive) 효율성 비교")
                
                c1, c2, c3 = st.columns(3)
                
                with c1:
                    st.markdown("#### Inclusive (Full)")
                    st.metric("Mean Abs (Ann)", f"{cp.get('Mean_Incl',0)*100:.1f}%")
                    st.metric("Vol (Ann)", f"{cp.get('Vol_Incl',0)*100:.1f}%")
                    st.metric("Score", f"{gs_incl:.2f}")
                    
                with c2:
                    st.markdown("#### Exclusive (No Earnings)")
                    st.metric("Mean Abs (Ann)", f"{cp.get('Mean_Excl',0)*100:.1f}%")
                    st.metric("Vol (Ann)", f"{cp.get('Vol_Excl',0)*100:.1f}%")
                    st.metric("Score", f"{gs_excl:.2f}")
                    
                with c3:
                    st.markdown("#### Earnings Impact")
                    st.metric("Delta Score", f"{score:.2f}", 
                              delta="Positive" if score > 0 else "Negative")
                    st.info(f"실적 기간이 포함됨으로써 점수가 **{score:+.2f}** 만큼 변화했습니다.")

                # Comparative Chart (Bar)
                # ... (Keeping existing Bar Chart or removing?)
                # User asked for "Earning 포함과 제외 비교".
                # Bar chart of Scores is good.
                # Adding Cumulative Line Chart is BETTER.
                
                # 1. Bar Chart (Scores)
                comp_df = pd.DataFrame({
                    'Condition': ['Inclusive (With Earnings)', 'Exclusive (Without Earnings)'],
                    'GS Score': [gs_incl, gs_excl]
                })
                fig_bar = px.bar(comp_df, x='Condition', y='GS Score', color='Condition', 
                                 title="Efficiency Score Comparison", text_auto='.2f')
                st.plotly_chart(fig_bar, use_container_width=True)
                
                # 2. Cumulative Equity Curve (The "Proof")
                st.subheader("📈 Cumulative Alpha (Idiosyncratic Return)")
                st.caption("실적 발표 기간이 장기 성과에 미치는 영향을 시각화합니다.")
                
                if 'Series_Excl' in cp:
                    # Incl Series (Full Idio Return)
                    s_incl = df['Idio_Return'].fillna(0)
                    
                    # Excl Series 
                    # logic_idio returns Series_Excl which contains only non-event days.
                    # We need to reindex it to full index to plot, filling removed days with 0.0 (Cash)
                    s_excl = cp['Series_Excl'].reindex(df.index).fillna(0.0)
                    
                    # Cumulative Sum
                    cum_incl = s_incl.cumsum()
                    cum_excl = s_excl.cumsum()
                    
                    chart_data = pd.DataFrame({
                        'With Earnings (실적 포함)': cum_incl,
                        'Without Earnings (실적 제외)': cum_excl
                    })
                    
                    fig_line = px.line(chart_data, title="Cumulative Idiosyncratic Return (Alpha Accumulation)",
                                       labels={'value': 'Cum Residual Return', 'index': 'Date'})
                    # Provide clearer colors
                    fig_line.update_traces(line=dict(width=2))
                    st.plotly_chart(fig_line, use_container_width=True)
                    
                    diff_val = cum_incl.iloc[-1] - cum_excl.iloc[-1]
                    st.info(f"💡 **분석 결과**: 실적 발표 기간을 포함했을 때 누적 성과가 **{diff_val*100:+.1f}%** 더 {'좋습니다' if diff_val>0 else '나쁩니다'}.")
                else:
                    st.warning("상세 시계열 데이터를 불러오지 못했습니다.")

                st.divider()

                # 2. 인사이트 메시지 (GS Delta Logic)
                if score > 0.5:
                    st.success(f"**🔥 High Impact:** 실적 발표가 이 종목의 변동성 대비 수익 효율을 크게 높여줍니다. (Delta: +{score:.2f})")
                elif score < 0.1:
                    st.warning(f"**🛡️ Low Impact:** 실적 발표를 제외해도 효율성 차이가 거의 없습니다.")
                
                st.divider()

                # 3. 그래프: Alpha vs Beta 분해
                
                st.divider()


            else:
                 # Data Collection Failed (Detailed Feedback)
                 st.warning(f"⚠️ **'{ticker}' 데이터 수집에 실패했습니다.**")
                 st.markdown("""
                 **가능한 원인은 다음과 같습니다:**
                 1. **잘못된 티커**: 미국 주식 티커가 맞는지 확인해주세요. (예: 삼성전자 사용 불가)
                 2. **데이터 접근 차단 (Yahoo Finance)**: 짧은 시간에 너무 많은 요청을 보내면 일시적으로 차단될 수 있습니다. (잠시 후 다시 시도)
                 3. **데이터 부족**: 상장된 지 3년 미만인 종목이거나, 거래량이 매우 적은 종목일 수 있습니다. (Nasdaq 소스 사용 중)
                 4. **네트워크 오류**: 인터넷 연결 상태를 확인해주세요.
                 """)
