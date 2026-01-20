import streamlit as st
import pandas as pd
import plotly.express as px
import FinanceDataReader as fdr
import requests
import urllib3
from io import StringIO, BytesIO
from datetime import datetime, timedelta
import yfinance as yf
import feedparser
import numpy as np
import pytz
import sqlite3
from collections import defaultdict
import math

# [필수] 같은 폴더의 etf.py에서 클래스 임포트
try:
    from etf import ActiveETFMonitor
except ImportError:
    st.error("⚠️ 'etf.py' 파일이 없습니다. 같은 폴더에 넣어주세요.")
    st.stop()

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
    page_icon="🍊",
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


# =========================
# KDI-style Issue Trend MVP
# =========================

ISSUE_DB_PATH = "issue_trend.db"

# 자산배분 관점 이슈 세트 (MVP 12개)
ISSUES = {
    "물가/인플레": {
        "kw": ["cpi", "pce", "inflation", "disinflation", "core", "headline", "prices", "물가", "인플레", "인플레이션", "근원"],
        "asset_hint": ["채권", "환율", "주식"]
    },
    "금리/연준": {
        "kw": ["fed", "fomc", "powell", "rate", "rates", "hike", "cut", "hold", "dot plot", "연준", "fomc", "파월", "기준금리", "금리인상", "금리인하", "동결"],
        "asset_hint": ["채권", "주식", "환율"]
    },
    "채권/수익률": {
        "kw": ["treasury", "ust", "yield", "10y", "2y", "curve", "spread", "duration", "국채", "미국채", "수익률", "일드커브", "커브", "스프레드", "듀레이션"],
        "asset_hint": ["채권"]
    },
    "달러/환율": {
        "kw": ["dollar", "dxy", "fx", "usd", "usdkrw", "eurusd", "yen", "yuan", "달러", "환율", "원달러", "외환", "강달러", "약달러"],
        "asset_hint": ["환율"]
    },
    "유가/에너지": {
        "kw": ["oil", "wti", "brent", "crude", "opec", "gas", "lng", "유가", "원유", "오펙", "감산", "증산", "천연가스", "lng"],
        "asset_hint": ["원자재", "인플레"]
    },
    "원자재/금속": {
        "kw": ["gold", "silver", "copper", "aluminum", "nickel", "lithium", "iron ore", "금", "은", "구리", "알루미늄", "니켈", "리튬", "철광석"],
        "asset_hint": ["원자재"]
    },
    "경기/성장": {
        "kw": ["gdp", "growth", "recession", "soft landing", "hard landing", "pmi", "ism", "unemployment", "jobs", "고용", "실업", "경기침체", "성장률", "pmi", "ism"],
        "asset_hint": ["주식", "채권"]
    },
    "실적/어닝": {
        "kw": ["earnings", "guidance", "revenue", "margin", "eps", "beats", "miss", "실적", "어닝", "가이던스", "매출", "마진", "eps", "서프라이즈"],
        "asset_hint": ["주식"]
    },
    "AI/반도체": {
        "kw": ["ai", "gpu", "semiconductor", "chip", "nvidia", "amd", "tsmc", "hbm", "ai", "반도체", "칩", "gpu", "엔비디아", "tsmc", "hbm"],
        "asset_hint": ["주식"]
    },
    "중국/신흥국": {
        "kw": ["china", "beijing", "yuan", "emerging", "중국", "위안", "신흥국", "부동산", "헝다", "부채"],
        "asset_hint": ["환율", "원자재", "주식"]
    },
    "지정학/리스크": {
        "kw": ["geopolitical", "sanction", "war", "conflict", "shipping", "strait", "iran", "israel", "ukraine", "지정학", "전쟁", "분쟁", "제재", "해운", "홍해"],
        "asset_hint": ["원자재", "환율", "주식"]
    },
    "정책/규제": {
        "kw": ["policy", "regulation", "tariff", "ban", "stimulus", "fiscal", "정책", "규제", "관세", "부양", "재정"],
        "asset_hint": ["주식", "환율", "채권"]
    }
}

STOPWORDS_ISSUE = set([
    "the","a","an","and","or","to","of","in","on","for","with","as","at","by",
    "from","after","before","today","live","update","updates",
    "시장","미국","글로벌","이번","관련","속보","단독","분석","전망","가능","우려","발표"
])

def _norm_text(t: str) -> str:
    t = (t or "").lower()
    t = re.sub(r"<[^>]*>", " ", t)
    t = re.sub(r"[^0-9a-zA-Z가-힣\s/\.%\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def init_issue_db():
    con = sqlite3.connect(ISSUE_DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS issue_windows (
        window_start_kst TEXT NOT NULL,
        window_end_kst   TEXT NOT NULL,
        issue            TEXT NOT NULL,
        mention_count    INTEGER NOT NULL,
        top_terms        TEXT,
        PRIMARY KEY (window_start_kst, window_end_kst, issue)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS issue_articles (
        window_start_kst TEXT NOT NULL,
        window_end_kst   TEXT NOT NULL,
        issue            TEXT NOT NULL,
        title            TEXT,
        link             TEXT,
        published_kst    TEXT,
        source           TEXT
    )
    """)
    con.commit()
    con.close()

@st.cache_resource
def ensure_issue_db():
    init_issue_db()
    return True

def floor_to_30m_kst(dt_kst: datetime) -> datetime:
    m = (dt_kst.minute // 30) * 30
    return dt_kst.replace(minute=m, second=0, microsecond=0)

def score_issue(text: str, issue_name: str) -> int:
    t = _norm_text(text)
    score = 0
    for kw in ISSUES[issue_name]["kw"]:
        k = _norm_text(kw)
        if not k or k in STOPWORDS_ISSUE:
            continue
        if k in t:
            score += 1
    return score

def map_article_to_issue(title: str, summary: str = ""):
    text = f"{title} {summary}"
    t = _norm_text(text)
    if not t or len(t) < 10:
        return None, 0

    best_issue = None
    best_score = 0
    for issue in ISSUES.keys():
        sc = score_issue(t, issue)
        if sc > best_score:
            best_score = sc
            best_issue = issue

    if best_score < 2:
        return None, best_score
    return best_issue, best_score

def fetch_issue_trend_items():
    items = []
    # Yahoo (기존 함수 재사용)
    items.extend(fetch_yahoo_news(["SPY", "QQQ", "^DJI"]))

    # Google RSS (폭 넓게)
    query = (
        "Fed OR FOMC OR CPI OR inflation OR yields OR dollar OR FX OR "
        "oil OR OPEC OR recession OR GDP OR PMI OR earnings OR guidance OR AI OR semiconductor "
        "when:3d"
    )
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        for e in feed.entries[:150]:
            title = getattr(e, "title", "")
            link = getattr(e, "link", "")
            if hasattr(e, "published_parsed") and e.published_parsed:
                dt = datetime(*e.published_parsed[:6])
            else:
                dt = datetime.now()
            items.append({
                "title": title,
                "link": link,
                "published_dt": dt,
                "source": e.source.title if hasattr(e, 'source') else "GoogleNews"
            })
    except:
        pass

    # 중복 제거 + 최신순
    seen = set()
    uniq = []
    for it in items:
        lk = it.get("link", "")
        if not lk or lk in seen:
            continue
        seen.add(lk)
        uniq.append(it)

    uniq.sort(key=lambda x: x.get("published_dt", datetime.min), reverse=True)
    return uniq

def store_window_issue_stats(ws: str, we: str, issue_counts: dict, issue_top_terms: dict, issue_articles: dict):
    con = sqlite3.connect(ISSUE_DB_PATH)
    cur = con.cursor()

    for issue, cnt in issue_counts.items():
        top_terms = issue_top_terms.get(issue, "")
        cur.execute("""
            INSERT INTO issue_windows(window_start_kst, window_end_kst, issue, mention_count, top_terms)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(window_start_kst, window_end_kst, issue)
            DO UPDATE SET mention_count=excluded.mention_count, top_terms=excluded.top_terms
        """, (ws, we, issue, int(cnt), top_terms))

    cur.execute("""
        DELETE FROM issue_articles
        WHERE window_start_kst=? AND window_end_kst=?
    """, (ws, we))

    for issue, rows in issue_articles.items():
        for r in rows[:10]:
            cur.execute("""
                INSERT INTO issue_articles(window_start_kst, window_end_kst, issue, title, link, published_kst, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ws, we, issue, r.get("title"), r.get("link"), r.get("published_kst"), r.get("source")))

    con.commit()
    con.close()

def read_issue_windows(limit_windows=96):
    con = sqlite3.connect(ISSUE_DB_PATH)
    df = pd.read_sql_query("""
        SELECT window_start_kst, window_end_kst, issue, mention_count, top_terms
        FROM issue_windows
        ORDER BY window_end_kst DESC
        LIMIT ?
    """, con, params=(limit_windows * len(ISSUES),))
    con.close()
    return df

def read_issue_articles(ws: str, we: str, issue: str):
    con = sqlite3.connect(ISSUE_DB_PATH)
    df = pd.read_sql_query("""
        SELECT title, link, published_kst, source
        FROM issue_articles
        WHERE window_start_kst=? AND window_end_kst=? AND issue=?
        ORDER BY published_kst DESC
        LIMIT 20
    """, con, params=(ws, we, issue))
    con.close()
    return df

def compute_current_window_issue_trend():
    ensure_issue_db()

    tz = pytz.timezone("Asia/Seoul")
    now_kst = datetime.now(tz)
    we_dt = floor_to_30m_kst(now_kst)
    ws_dt = we_dt - timedelta(minutes=30)

    ws = ws_dt.strftime("%Y-%m-%d %H:%M")
    we = we_dt.strftime("%Y-%m-%d %H:%M")

    items = fetch_issue_trend_items()

    issue_counts = {k: 0 for k in ISSUES.keys()}
    issue_terms = defaultdict(lambda: defaultdict(int))
    issue_evidence = defaultdict(list)

    for it in items:
        dt = it.get("published_dt")
        if not isinstance(dt, datetime):
            continue

        if dt.tzinfo is None:
            dt_kst = tz.localize(dt)
        else:
            dt_kst = dt.astimezone(tz)

        if not (ws_dt <= dt_kst < we_dt):
            continue

        title = it.get("title", "")
        link = it.get("link", "")
        src = it.get("source", "")

        issue, sc = map_article_to_issue(title, "")
        if issue is None:
            continue

        issue_counts[issue] += 1

        tnorm = _norm_text(title)
        for kw in ISSUES[issue]["kw"]:
            k = _norm_text(kw)
            if k and k in tnorm and k not in STOPWORDS_ISSUE:
                issue_terms[issue][k] += 1

        issue_evidence[issue].append({
            "title": title,
            "link": link,
            "published_kst": dt_kst.strftime("%Y-%m-%d %H:%M"),
            "source": src
        })

    issue_top_terms = {}
    for issue, d in issue_terms.items():
        top = sorted(d.items(), key=lambda x: x[1], reverse=True)[:5]
        issue_top_terms[issue] = ", ".join([k for k, v in top])

    store_window_issue_stats(ws, we, issue_counts, issue_top_terms, issue_evidence)
    return ws, we

def build_issue_rank(df_all: pd.DataFrame, current_we: str, lookback_windows=48):
    cur = df_all[df_all["window_end_kst"] == current_we].copy()
    if cur.empty:
        return pd.DataFrame()

    df = df_all.copy()
    df["we_dt"] = pd.to_datetime(df["window_end_kst"])
    cur_we_dt = pd.to_datetime(current_we)

    past = df[(df["we_dt"] < cur_we_dt) & (df["we_dt"] >= cur_we_dt - pd.Timedelta(minutes=30*lookback_windows))]

    rows = []
    for issue in ISSUES.keys():
        cur_cnt = int(cur[cur["issue"] == issue]["mention_count"].sum()) if not cur[cur["issue"] == issue].empty else 0
        hist = past[past["issue"] == issue]["mention_count"].astype(float)
        mu = float(hist.mean()) if len(hist) > 0 else 0.0
        sd = float(hist.std(ddof=0)) if len(hist) > 0 else 0.0

        z = (cur_cnt - mu) / (sd + 1e-6) if (len(hist) > 5) else (cur_cnt - mu)
        rows.append([issue, cur_cnt, mu, sd, z])

    out = pd.DataFrame(rows, columns=["issue", "cur_cnt", "mean", "std", "spike_z"])
    out = out.sort_values(["spike_z", "cur_cnt"], ascending=False)

    out["spike_z"] = out["spike_z"].map(lambda x: round(float(x), 2))
    out["mean"] = out["mean"].map(lambda x: round(float(x), 2))
    out["std"] = out["std"].map(lambda x: round(float(x), 2))
    return out



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
    """슈퍼테마 ETF 수익률 계산 (FDR 사용)"""
    results = []
    
    if ref_date is None:
        ref_date = datetime.now()
    
    # FDR 날짜 포맷 (YYYY-MM-DD)
    end_date_str = ref_date.strftime("%Y-%m-%d")
    # 시작일은 넉넉하게 2달 전
    start_date_str = (ref_date - timedelta(days=60)).strftime("%Y-%m-%d")
    
    for i, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        if ticker.endswith('.KS'): ticker = ticker.replace('.KS', '')
        
        try:
            # FDR 데이터 수집 (기간 지정)
            hist = fdr.DataReader(ticker, start_date_str, end_date_str)
            
            if not hist.empty:
                curr = hist['Close'].iloc[-1]
                
                # 1D Return
                if len(hist) >= 2:
                    ret_1d = ((curr - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
                else: ret_1d = 0
                
                # 5D Return
                if len(hist) >= 6:
                    ret_5d = ((curr - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6]) * 100
                else: ret_5d = 0

                # 1M Return (approx 20 trading days)
                if len(hist) >= 21:
                    ret_1m = ((curr - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21]) * 100
                else: 
                    ret_1m = ((curr - hist['Close'].iloc[0]) / hist['Close'].iloc[0]) * 100
                
                results.append({
                    "Ticker": row['Ticker'],
                    "Name": row['Name'],
                    "Theme": row['Theme'],
                    "Price": curr,
                    "1D": round(ret_1d, 2),
                    "5D": round(ret_5d, 2),
                    "1M": round(ret_1m, 2)
                })
            else:
                 st.warning(f"{ticker}: 데이터 없음")
        except Exception as e:
            st.error(f"{ticker} 에러: {e}")
    
    if not results:
        return pd.DataFrame(columns=["Ticker", "Name", "Theme", "Price", "1D", "5D", "1M"])
    
    return pd.DataFrame(results)

def calculate_super_stock(df, ref_date=None):
    """슈퍼스탁 데이터 계산 (FDR 사용 - 펀더멘털 제외 Price 위주)"""
    results = []
    
    if ref_date is None:
        ref_date = datetime.now()
        
    end_date_str = ref_date.strftime("%Y-%m-%d")
    start_date_str = (ref_date - timedelta(days=15)).strftime("%Y-%m-%d") # 스탁은 짧게 봄

    for i, row in df.iterrows():
        ticker = str(row['Ticker']).strip()
        if ticker.endswith('.KS'): ticker = ticker.replace('.KS', '')
        
        try:
            hist = fdr.DataReader(ticker, start_date_str, end_date_str)
            
            if not hist.empty:
                curr = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2] if len(hist) >= 2 else curr
                pct = ((curr - prev)/prev)*100 if prev else 0
                
                results.append({
                    "Ticker": row['Ticker'],
                    "Name": row['Name'],
                    "Sector": row['Sector'],
                    "Price": curr,
                    "Change": round(pct, 2),
                    "PER": 0, # N/A
                    "PBR": 0, # N/A
                    "ROE": 0  # N/A
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
    st.title("🍊 Mirae Asset")
    st.subheader("고객자산배분본부")
    st.caption("Strategy Dashboard V4.1")
    st.markdown("---")
    
    menu = st.radio("메뉴 선택", [
        "📰 Daily Market Narrative", 
        "📈 Super-Stock",
        "📊 TIMEFOLIO Analysis"
    ])
    
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()

# ---------------------------------------------------------
# 4. 메인 화면 로직
# ---------------------------------------------------------

# [TAB 1] Daily Market Narrative (모닝 미팅용)
if menu == "📰 Daily Market Narrative":
    st.title("📰 Daily Market Narrative")
    st.markdown("### ☕ Morning Meeting Board")
    st.info("오늘의 시장 환경을 점검하고, 유니버스 테마의 리밸런싱 전략을 논의하는 공간입니다.")

    # 1. Macro Environment (시장 환경 점검)
    st.markdown("#### 1. Macro Environment (시장 분위기)")
    cols = st.columns(6)
    
    # 핵심 지표 나열
    indicators = ["KOSPI", "S&P500", "Nasdaq", "USD/KRW", "US 10Y", "WTI Oil"]
    for i, key in enumerate(indicators):
        if key in macro_metrics:
            with cols[i]:
                d = macro_metrics[key]
                color = "normal" if d['pct_change'] >= 0 else "inverse"
                st.metric(key, f"{d['price']:,.2f}", f"{d['pct_change']:.2f}%", delta_color=color)


    st.markdown("---")

    # 1.5 Global Market Event Radar (New Feature)
    st.markdown("#### 🚨 Global Market Event Radar (Key Events)")
    st.info("🌐 이번 주 시장을 움직이는 핵심 매크로 이벤트 & 뉴스")
    
    global_events = fetch_global_events()
    if global_events:
        for n in global_events:
            # 날짜 포맷팅
            try:
                dt = datetime.strptime(n['published'], "%a, %d %b %Y %H:%M:%S %Z")
                date_str = dt.strftime("%Y-%m-%d %H:%M")
            except:
                date_str = ""
            
            # 태그 분석
            tags = get_news_tags(n['title'])
            tag_html = ""
            for t_text, t_bg, t_col in tags:
                tag_html += f"<span style='background-color:{t_bg}; color:{t_col}; padding: 2px 6px; border-radius: 4px; font-size: 11px; margin-right: 4px; font-weight: bold;'>{t_text}</span>"
            
            # 카드 스타일 (조금 더 강조된 디자인)
            st.markdown(f"""
            <div style="padding: 12px; border-left: 4px solid #FF5050; background-color: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); margin-bottom: 10px;">
                <a href="{n['link']}" target="_blank" style="text-decoration: none; color: #111; font-weight: bold; font-size: 15px;">{n['title']}</a>
                <br><div style="margin-top: 6px;">{tag_html} <span style="color: #666; font-size: 12px;">{n['source']} | {date_str}</span></div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.write("현재 감지된 주요 이벤트가 없습니다.")

    st.markdown("---")

    # =========================
    # Issue Trend UI (KDI-style)
    # =========================
    st.markdown("### 📈 Issue Trend (30분 단위)")
    with st.expander("옵션", expanded=False):
        refresh_sec = st.selectbox("자동 새로고침(초)", [30, 60, 120, 300, 600, 1800], index=3)
        st.caption("30분 단위 집계라 너무 짧게 새로고침할 필요는 없음. 데모용으로는 유용함")
        st.markdown(f"<meta http-equiv='refresh' content='{refresh_sec}'>", unsafe_allow_html=True)

    ws, we = compute_current_window_issue_trend()
    df_all = read_issue_windows(limit_windows=96)

    if df_all.empty:
        st.warning("이슈 트렌드 데이터 없음. 현재 30분 윈도우에 매핑되는 뉴스가 없을 수 있음.")
    else:
        rank = build_issue_rank(df_all, current_we=we, lookback_windows=48)

        c1, c2 = st.columns([1.1, 0.9])

        with c1:
            st.markdown(f"**현재 윈도우(KST)**: {ws} ~ {we}")
            st.markdown("**Top Issues (급증 z-score 기준)**")
            show = rank[["issue", "cur_cnt", "spike_z"]].head(10).copy()
            show.columns = ["Issue", "Mentions(현재 30분)", "Spike(z)"]
            st.dataframe(show, use_container_width=True)

            default_issue = show.iloc[0]["Issue"] if len(show) > 0 else list(ISSUES.keys())[0]
            issue_sel = st.selectbox("이슈 선택", list(ISSUES.keys()), index=list(ISSUES.keys()).index(default_issue))

        with c2:
            st.markdown("**Trend (최근 24시간)**")
            tmp = df_all.copy()
            tmp["we_dt"] = pd.to_datetime(tmp["window_end_kst"])
            cur_we_dt = pd.to_datetime(we)
            tmp = tmp[(tmp["we_dt"] <= cur_we_dt) & (tmp["we_dt"] >= cur_we_dt - pd.Timedelta(hours=24))]
            ts = tmp[tmp["issue"] == issue_sel].sort_values("we_dt")[["we_dt", "mention_count"]]

            if ts.empty:
                st.info("해당 이슈의 최근 24시간 데이터가 부족함.")
            else:
                chart_df = ts.rename(columns={"we_dt": "window_end", "mention_count": "mentions"}).set_index("window_end")
                st.line_chart(chart_df)

            cur_row = df_all[(df_all["window_end_kst"] == we) & (df_all["issue"] == issue_sel)]
            top_terms = cur_row["top_terms"].iloc[0] if not cur_row.empty else ""
            st.markdown("**대표 키워드(현재 윈도우)**")
            st.write(top_terms if top_terms else "없음")

        st.markdown("**근거 기사(현재 30분)**")
        ev = read_issue_articles(ws, we, issue_sel)
        if ev.empty:
            st.write("없음")
        else:
            for r in ev.itertuples(index=False):
                title = r.title or "(제목 없음)"
                link = r.link or ""
                meta = f"{r.published_kst or ''} · {r.source or ''}"
                if link:
                    st.markdown(f"- [{title}]({link})  
  {meta}")
                else:
                    st.markdown(f"- {title}  
  {meta}")


    # 2. Global IB House View (대체된 기능)
    st.markdown("#### 2. Global IB House View (Wall St. Insight)")
    st.info("💡 월가 주요 투자은행(IB)들의 최신 시장 전망 및 전략 리포트 요약")

    ib_banks = {
        "JP Morgan": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/07/J_P_Morgan_Chase_Logo_2008_1.svg/1200px-J_P_Morgan_Chase_Logo_2008_1.svg.png",
        "Goldman Sachs": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/61/Goldman_Sachs.svg/1200px-Goldman_Sachs.svg.png",
        "Morgan Stanley": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/34/Morgan_Stanley_Logo_1.svg/1200px-Morgan_Stanley_Logo_1.svg.png"
    }
    
    cols = st.columns(3)
    for i, (bank, logo_url) in enumerate(ib_banks.items()):
        with cols[i]:
            st.markdown(f"**🏦 {bank}**")
            # st.image(logo_url, width=100) # 로고는 링크 깨질 수 있으므로 텍스트로 대체하거나 유지
            
            news = fetch_ib_news(bank)
            if news:
                for n in news:
                    # 날짜 포맷팅 깔끔하게
                    try:
                        dt = datetime.strptime(n['published'], "%a, %d %b %Y %H:%M:%S %Z")
                        date_str = dt.strftime("%Y-%m-%d")
                    except:
                        date_str = ""
                    
                    # 태그 분석
                    tags = get_news_tags(n['title'])
                    tag_html = ""
                    for t_text, t_bg, t_col in tags:
                        tag_html += f"<span style='background-color:{t_bg}; color:{t_col}; padding: 2px 6px; border-radius: 4px; font-size: 11px; margin-right: 4px; font-weight: bold;'>{t_text}</span>"
                        
                    st.markdown(f"""
                    <div style="padding: 10px; border: 1px solid #e0e0e0; border-radius: 5px; margin-bottom: 10px; background-color: #f9f9f9;">
                        <a href="{n['link']}" target="_blank" style="text-decoration: none; color: #333; font-weight: bold; font-size: 14px;">{n['title']}</a>
                        <br><div style="margin-top: 4px;">{tag_html} <span style="color: #666; font-size: 12px;">{n['source']} | {date_str}</span></div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.caption("최신 관련 뉴스가 없습니다.")

    st.markdown("---")

    # 3. Discussion & Action Plan (회의록 작성)
    st.markdown("#### 3. Today's Action Plan (회의 기록)")
    
    c_memo1, c_memo2 = st.columns(2)
    with c_memo1:
        st.text_area("🗣️ Macro View & Issue", height=150, placeholder="예: 미 국채 금리 상승으로 인한 성장주 조정 가능성 논의...")
    with c_memo2:
        st.text_area("⚖️ Rebalancing Idea", height=150, placeholder="예: 'AI 반도체' 비중 유지하되, '2차전지' 비중 축소 의견 우세...")

    st.markdown("---")

    # 4. Morning Report Helper (New Feature moved here)
    with st.expander("📝 Morning Report Helper (데이터 분석 도구)", expanded=False):
        # 기준 날짜 선택 (오늘이 기본)
        col_date, col_dummy = st.columns([1, 2])
        with col_date:
            target_date = st.date_input("📅 기준 날짜 선택 (이 날짜 기준 수익률 계산)", datetime.now())

        # 템플릿 다운로드 버튼 제공
        try:
            with open("universe.xlsx", "rb") as f:
                btn = st.download_button(
                    label="📥 유니버스 템플릿 다운로드 (universe.xlsx)",
                    data=f,
                    file_name="universe.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        except: pass

        # 입력 방식 선택
        input_method = st.radio("데이터 입력 방식 선택", ["📂 엑셀 파일 업로드", "✍️ 티커 직접 입력 (복사/붙여넣기)", "🎁 샘플 데이터 (시연용)"], horizontal=True)
        
        df_themes = None
        df_stocks = None
        
        if input_method == "📂 엑셀 파일 업로드":
            uploaded_file = st.file_uploader("universe.xlsx 업로드", type=['xlsx'])
            if uploaded_file:
                try:
                    uploaded_file.seek(0)
                    df_themes = pd.read_excel(uploaded_file, sheet_name=0, engine='openpyxl')
                    try:
                        df_stocks = pd.read_excel(uploaded_file, sheet_name=1, engine='openpyxl')
                    except:
                        df_stocks = None
                    st.success("파일 로드 성공! (Themes & Stocks)")
                except Exception as e:
                    st.error(f"엑셀 로드 오류 (DRM 등): {e}")
                
        elif input_method == "✍️ 티커 직접 입력 (복사/붙여넣기)":
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**1. 슈퍼테마 (ETF)**")
                txt_theme = st.text_area("티커 입력 (쉼표로 구분)", "396500, LIT, SHLD, 091230", height=100)
                if txt_theme:
                    tickers = [t.strip() for t in txt_theme.split(',')]
                    df_themes = pd.DataFrame({"Ticker": tickers, "Name": tickers, "Theme": ["Manual Input"]*len(tickers)})
            with c2:
                st.markdown("**2. 슈퍼스탁 (개별주)**")
                txt_stock = st.text_area("티커 입력 (쉼표로 구분)", "NVDA, AAPL, 005930, MSFT", height=100)
                if txt_stock:
                    tickers = [t.strip() for t in txt_stock.split(',')]
                    df_stocks = pd.DataFrame({"Ticker": tickers, "Name": tickers, "Sector": ["Manual Input"]*len(tickers)})
                    
        elif input_method == "🎁 샘플 데이터 (시연용)":
            st.caption("※ 발표 시연을 위해 미리 저장된 유니버스 리스트를 사용합니다.")
            # 샘플 데이터 하드코딩
            theme_data = [["396500", "TIGER 반도체", "반도체"], ["LIT", "Global X Lithium", "2차전지"], ["SHLD", "Global X Defense", "방산"]]
            stock_data = [["NVDA", "Nvidia", "Tech"], ["AAPL", "Apple", "Tech"], ["005930", "Samsung Elec", "Tech"]]
            df_themes = pd.DataFrame(theme_data, columns=["Ticker", "Name", "Theme"])
            df_stocks = pd.DataFrame(stock_data, columns=["Ticker", "Name", "Sector"])
            st.success("샘플 데이터 로드 완료 (즉시 분석 가능)")
        
        # 분석 실행 UI
        if df_themes is not None or df_stocks is not None:
            t1, t2 = st.tabs(["■ 슈퍼테마 (ETF) 결과", "■ 슈퍼스탁 (Stock) 결과"])
            
            with t1:
                if df_themes is not None:
                    if st.button("테마 데이터 계산 시작 🚀"):
                        with st.spinner(f"{target_date.strftime('%Y-%m-%d')} 기준 수익률 계산 중..."):
                            res_theme = calculate_super_theme(df_themes, target_date)
                            
                            def color_val(val):
                                if isinstance(val, (int, float)):
                                    color = 'red' if val > 0 else 'blue' if val < 0 else 'black'
                                    return f'color: {color}'
                                return ''
                            
                            st.dataframe(res_theme.style.map(color_val, subset=['1D', '5D', '1M']), use_container_width=True)
            
            with t2:
                if df_stocks is not None:
                    if st.button("스탁 데이터 계산 시작 🚀"):
                        with st.spinner(f"{target_date.strftime('%Y-%m-%d')} 기준 데이터 수집 중..."):
                            res_stock = calculate_super_stock(df_stocks, target_date)
                            st.dataframe(res_stock, use_container_width=True)


# [TAB 2] Super-Stock (StatCounter) - 팀장님 개인 업무
if menu == "📈 Super-Stock":
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
            df = fetch_statcounter_data("search_engine", device="desktop+mobile+tablet+console")
            df_proc = process_search_engine_data(df)
            
            if not df_proc.empty:
                # 막대 차트 (Stacked Bar)
                fig = px.bar(df_proc, title="Search Engine M/S (Total)", barmode='stack', 
                             color_discrete_map={'Google': '#4285F4', 'Bing': '#00A4EF', 'Yahoo': '#7B0099', 'Other': '#999999'})
                
                # Y축 스케일 조정 (비율 파악 용이하도록)
                y_min = df_proc['Google'].min() - 5
                if y_min < 0: y_min = 0
                fig.update_layout(yaxis_range=[y_min, 100], legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
                
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_proc.sort_index(ascending=False).style.format("{:.1f}%").background_gradient(cmap="Reds", subset=["Google"]), use_container_width=True)

        # 2. Desktop
        with sub_tab2:
            df = fetch_statcounter_data("search_engine", device="desktop")
            df_proc = process_search_engine_data(df)
            
            if not df_proc.empty:
                fig = px.bar(df_proc, title="Search Engine M/S (Desktop)", barmode='stack',
                             color_discrete_map={'Google': '#4285F4', 'Bing': '#00A4EF', 'Yahoo': '#7B0099', 'Other': '#999999'})
                
                y_min = df_proc['Google'].min() - 5
                if y_min < 0: y_min = 0
                fig.update_layout(yaxis_range=[y_min, 100], legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))

                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_proc.sort_index(ascending=False).style.format("{:.1f}%").background_gradient(cmap="Reds", subset=["Google"]), use_container_width=True)

        # 3. Mobile
        with sub_tab3:
            df = fetch_statcounter_data("search_engine", device="mobile")
            df_proc = process_search_engine_data(df)
            
            if not df_proc.empty:
                fig = px.bar(df_proc, title="Search Engine M/S (Mobile)", barmode='stack',
                             color_discrete_map={'Google': '#4285F4', 'Bing': '#00A4EF', 'Yahoo': '#7B0099', 'Other': '#999999'})
                
                y_min = df_proc['Google'].min() - 5
                if y_min < 0: y_min = 0
                fig.update_layout(yaxis_range=[y_min, 100], legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))

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
if menu == "📊 TIMEFOLIO Analysis":
    st.title("📊 TIMEFOLIO Official Portfolio & Rebalancing")
    
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

                    # 탭 구성
                    tab1, tab2, tab3 = st.tabs(["주요 변경내역", "세부 변동", "전체 포트폴리오"])
                    
                    with tab1:
                        # 신규 편입 & 편출
                        c1, c2 = st.columns(2)
                        with c1:
                            st.markdown("##### 🟢 신규 편입")
                            if analysis['new_stocks']:
                                rows = []
                                for s in analysis['new_stocks']:
                                    rows.append({
                                        "종목명": s['종목명'],
                                        "현재비중": f"{s['비중_today']:.2f}%",
                                        "순수변동": f"+{s['순수_비중변화']:.2f}%p"
                                    })
                                st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                            else:
                                st.caption("신규 편입 종목 없음")

                        with c2:
                            st.markdown("##### 🔴 완전 편출")
                            if analysis['removed_stocks']:
                                rows = []
                                for s in analysis['removed_stocks']:
                                    rows.append({
                                        "종목명": s['종목명'],
                                        "이전비중": f"{s['비중_prev']:.2f}%",
                                        "순수변동": f"{s['순수_비중변화']:.2f}%p"
                                    })
                                st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                            else:
                                st.caption("완전 편출 종목 없음")

                    with tab2:
                        # 비중 확대 & 축소
                        c1, c2 = st.columns(2)
                        with c1:
                            st.markdown("##### 🔼 비중 확대 (Top 5)")
                            if analysis['increased_stocks']:
                                df_inc = pd.DataFrame(analysis['increased_stocks'])
                                df_inc = df_inc.sort_values('순수_비중변화', ascending=False).head(5)
                                display_df = df_inc[['종목명', '비중_prev', '비중_today', '순수_비중변화']].copy()
                                display_df.columns = ['종목명', '이전(%)', '현재(%)', '변동(%p)']
                                st.dataframe(display_df.style.format({'이전(%)': '{:.2f}', '현재(%)': '{:.2f}', '변동(%p)': '+{:.2f}'}), hide_index=True, use_container_width=True)
                            else:
                                st.caption("비중 확대 종목 없음")

                        with c2:
                            st.markdown("##### 🔽 비중 축소 (Top 5)")
                            if analysis['decreased_stocks']:
                                df_dec = pd.DataFrame(analysis['decreased_stocks'])
                                df_dec = df_dec.sort_values('순수_비중변화', ascending=True).head(5)
                                display_df = df_dec[['종목명', '비중_prev', '비중_today', '순수_비중변화']].copy()
                                display_df.columns = ['종목명', '이전(%)', '현재(%)', '변동(%p)']
                                st.dataframe(display_df.style.format({'이전(%)': '{:.2f}', '현재(%)': '{:.2f}', '변동(%p)': '{:.2f}'}), hide_index=True, use_container_width=True)
                            else:
                                st.caption("비중 축소 종목 없음")
                                
                        st.info("* **순수 변동**: 시장 가격 등락에 의한 '가상 비중'을 제외한 매니저의 실제 매매로 인한 비중 변화 (추정치)")

                    with tab3:
                        st.markdown("##### 📋 전체 포트폴리오 구성")
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