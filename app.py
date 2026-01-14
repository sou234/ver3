import streamlit as st
import pandas as pd
import plotly.express as px
import FinanceDataReader as fdr
import requests
import urllib3
from io import BytesIO
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
import feedparser
from etf import ActiveETFMonitor
import yfinance as yf
from curl_cffi import requests as curequests
import re
from collections import Counter
import plotly.graph_objects as go

# 보안 인증서 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 엑셀 다운로드용 함수
def to_excel(df_new, df_inc, df_dec, df_all, date):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_new.to_excel(writer, index=False, sheet_name='신규편입')
        df_inc.to_excel(writer, index=False, sheet_name='비중확대')
        df_dec.to_excel(writer, index=False, sheet_name='비중축소')
        df_all.to_excel(writer, index=False, sheet_name='전체포트폴리오')
    processed_data = output.getvalue()
    return processed_data

# ---------------------------------------------------------
# 1. 페이지 설정
# ---------------------------------------------------------
st.set_page_config(
    page_title="MAS Market Narrative V5.0",
    page_icon="🍊",
    layout="wide"
)

# ---------------------------------------------------------
# 2. 데이터 수집 로직 (테마/내러티브 중심)
# ---------------------------------------------------------

# 주요 테마와 대표 자산(Proxy) 매핑
MARKET_THEMES = {
    "🤖 AI & 반도체 혁명": {"ticker": "NVDA", "name": "Nvidia", "query": "Nvidia AI semiconductor stock"},
    "⚡ 전기차/2차전지 캐즘": {"ticker": "TSLA", "name": "Tesla", "query": "Tesla EV battery stock"},
    "🏛️ 미 연준(Fed) & 금리": {"ticker": "^TNX", "name": "미국채 10년물", "query": "Federal Reserve interest rate bond yield"},
    "🇨🇳 중국/이머징 마켓": {"ticker": "FXI", "name": "China Large-Cap", "query": "China economy stimulus stock market"},
    "🪙 크립토/디지털자산": {"ticker": "BTC-USD", "name": "Bitcoin", "query": "Bitcoin crypto regulation price"},
    "🛢️ 에너지/지정학 리스크": {"ticker": "CL=F", "name": "WTI 유가", "query": "Oil price Middle East war energy"},
    "💊 비만치료제/바이오": {"ticker": "LLY", "name": "Eli Lilly", "query": "Eli Lilly weight loss drug stock"},
    "🇰🇷 한국 증시 (대표)": {"ticker": "^KS11", "name": "KOSPI", "query": "KOSPI Korea stock market"}
}

@st.cache_data(ttl=600)
def fetch_narrative_data():
    """테마별 대표 자산의 등락률을 계산하여 '오늘의 핫 토픽' 선정"""
    narratives = []
    
    session = curequests.Session(impersonate="chrome")
    session.verify = False

    for theme, info in MARKET_THEMES.items():
        try:
            ticker = info['ticker']
            stock = yf.Ticker(ticker, session=session)
            # 최근 5일치 가져와서 전일비 비교 (휴장일 고려 안전하게)
            hist = stock.history(period="5d")
            
            if len(hist) >= 2:
                current = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2]
                change = current - prev
                pct = (change / prev) * 100
                
                narratives.append({
                    "theme": theme,
                    "proxy": info['name'],
                    "ticker": ticker,
                    "price": current,
                    "pct_change": pct,
                    "query": info['query'],
                    "history": hist['Close'] # 차트용
                })
        except Exception:
            continue
            
    # 등락률 절댓값 기준 정렬 (시장을 가장 크게 움직인 테마 순)
    narratives.sort(key=lambda x: abs(x['pct_change']), reverse=True)
    return narratives

@st.cache_data(ttl=1800)
def fetch_news_headline(query, lang='en'):
    """구글 뉴스 RSS에서 뉴스 수집 (언어 선택 가능)"""
    encoded = requests.utils.quote(query)
    if lang == 'en':
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
    else:
        url = f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"
        
    try:
        feed = feedparser.parse(url)
        items = []
        for e in feed.entries[:2]:
            items.append({"title": e.title, "link": e.link, "source": e.source.title if hasattr(e, 'source') else "News", "lang": lang})
        return items
    except:
        return []

@st.cache_data(ttl=1800)
def fetch_country_briefing(country_code):
    """국가별 핵심 재료 뉴스 헤드라인 Top 5 추출 (요약 형태)"""
    
    # 🔍 Catalyst Search Queries
    if country_code == 'US':
        query = "Earnings OR Awarded OR Launched OR Unveiled OR Acquisition OR FDA OR Regulation"
        encoded = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}+when:1d&hl=en-US&gl=US&ceid=US:en"
        
    elif country_code == 'KR':
        query = "실적 OR 수주 OR 체결 OR 개발 OR 출시 OR 승인 OR 정책 OR 공시"
        encoded = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}+when:1d&hl=ko&gl=KR&ceid=KR:ko"
        
    elif country_code == 'CN':
        query = "China Stimulus OR China Restriction OR China EV OR China Tech"
        encoded = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}+when:1d&hl=en-US&gl=US&ceid=US:en"
    else:
        return []

    try:
        feed = feedparser.parse(url)
        briefings = []
        seen_titles = set()
        
        for e in feed.entries:
            # 제목 전처리: 언론사명 제거 및 길이 제한
            title = re.sub(r'\s*-[^-]*$', '', e.title)
            title = title.strip()
            
            # 중복 제거 (유사 제목 필터링)
            if title not in seen_titles and len(title) > 10:
                briefings.append({"title": title, "link": e.link})
                seen_titles.add(title)
                
            if len(briefings) >= 5: # Top 5만 추출
                break
                
        return briefings
    except:
        return []

# 테마별 한국어 쿼리 매핑
THEME_KR_QUERIES = {
    "🤖 AI & 반도체 혁명": "엔비디아 반도체 AI 주가",
    "⚡ 전기차/2차전지 캐즘?": "테슬라 전기차 배터리 주가",
    "🏛️ 미 연준(Fed) & 금리": "미국 연준 금리 채권",
    "🇨🇳 중국/이머징 마켓": "중국 경기부양책 증시",
    "🪙 크립토/디지털자산": "비트코인 가상화폐 시세 규제",
    "🛢️ 에너지/지정학 리스크": "국제유가 중동 전쟁 에너지",
    "💊 비만치료제/바이오": "일라이릴리 비만치료제 바이오주",
    "🇰🇷 한국 증시 (대표)": "코스피 한국 증시 전망"
}

# 데이터 로딩
hot_narratives = fetch_narrative_data()

@st.cache_data(ttl=86400)
def fetch_statcounter_data(metric="search_engine", device="desktop+mobile+tablet+console", region="ww", from_year="2019", from_month="01"):
    """StatCounter 데이터 수집 (CSV Direct)"""
    import requests
    import io
    from datetime import datetime
    
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

# ---------------------------------------------------------
# 3. 사이드바 구성
# ---------------------------------------------------------
with st.sidebar:
    st.title("🍊 Mirae Asset")
    st.subheader("Daily Market Briefing")
    st.caption("Ver 5.2 - Global Insights")
    st.markdown("---")
    
    menu = st.radio("메뉴 선택", ["📰 데일리 마켓 내러티브", "📈 글로벌 점유율 트래커", "🔍 기업 펀더멘털 스카우터", "⚔️ ETF 운용사 배틀", "📊 타임폴리오 ETF 분석"])
    
    if st.button("🔄 데이터 새로고침"):
        st.cache_data.clear()

if menu == "📈 글로벌 점유율 트래커":
    st.header("📈 Global Market Share Tracker")
    st.caption("StatCounter Data (Google, Bing, Yahoo, Other)")
    
    tab1, tab2, tab3 = st.tabs(["🖥️+📱 Desktop & Mobile", "🖥️ Desktop", "📱 Mobile"])
    
    # 1. Desktop + Mobile (Combined)
    with tab1:
        st.subheader("Global Search Engine M/S (Desktop + Mobile)")
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
    with tab2:
        st.subheader("Global Search Engine M/S (Desktop Only)")
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
    with tab3:
        st.subheader("Global Search Engine M/S (Mobile Only)")
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

# ---------------------------------------------------------
# 4. 메인 화면
# ---------------------------------------------------------

if menu == "📰 데일리 마켓 내러티브":
    
    st.title("📰 Daily Market Narrative")
    st.markdown("""
    단순한 지수 나열이 아닙니다.  
    **"어제 무슨 이슈(Topic)가 있었고 ➡️ 그 결과 어떤 자산이 움직였는지(Impact)"** 인과관계를 중심으로 정리합니다.
    """)
    st.markdown("---")
    
    # [0] 글로벌 마켓 브리핑 (New Feature: Catalyst Summary)
    with st.expander("🌍 Global Market Catalyst Briefing (US/KR/CN)", expanded=True):
        st.markdown("각 국가별 시장을 움직이는 **핵심 재료(실적, 정책, 신기술)** 뉴스 요약입니다.")
        c1, c2, c3 = st.columns(3)
        
        with c1:
            st.markdown("#### 🇺🇸 United States")
            brief_us = fetch_country_briefing('US')
            if brief_us:
                for item in brief_us:
                    st.markdown(f"- [{item['title']}]({item['link']})")
            else:
                st.caption("데이터 수집 불가")
                
        with c2:
            st.markdown("#### 🇰🇷 Korea")
            brief_kr = fetch_country_briefing('KR')
            if brief_kr:
                for item in brief_kr:
                    st.markdown(f"- [{item['title']}]({item['link']})")
            else:
                st.caption("데이터 수집 불가")
                
        with c3:
            st.markdown("#### 🇨🇳 China (Market)")
            brief_cn = fetch_country_briefing('CN')
            if brief_cn:
                for item in brief_cn:
                    st.markdown(f"- [{item['title']}]({item['link']})")
            else:
                st.caption("데이터 수집 불가")

    st.markdown("---")
    
    # [1] 오늘의 Top 3 이슈 카드 (상단 강조)
    st.subheader("🔥 Today's Hot Issues (Top 3 Movers)")
    
    top_movers = hot_narratives[:3] if hot_narratives else []
    
    cols = st.columns(3)
    for i, item in enumerate(top_movers):
        with cols[i]:
            # 스타일링: 상승(빨강) / 하락(파랑)
            color = "red" if item['pct_change'] > 0 else "blue"
            direction = "▲ 급등" if item['pct_change'] > 0 else "▼ 급락"
            bg_color = "rgba(255, 0, 0, 0.1)" if item['pct_change'] > 0 else "rgba(0, 0, 255, 0.1)"
            
            # 카드 형태 디자인
            st.info(f"**{item['theme']}**")
            st.metric(
                label=item['proxy'],
                value=f"{item['price']:,.2f}",
                delta=f"{item['pct_change']:+.2f}%",
                delta_color="normal"
            )
            
            # 미니 차트
            st.line_chart(item['history'], height=80)
            
            # 뉴스 매핑 (왜 올랐나/내렸나?) - EN & KR
            st.caption("📌 Global & Local Headlines")
            
            # English News
            news_en = fetch_news_headline(item['query'], lang='en')
            if news_en:
                st.markdown(f"**🇺🇸 Global**: [{news_en[0]['title']}]({news_en[0]['link']})")
                
            # Korean News
            kr_query = THEME_KR_QUERIES.get(item['theme'], item['theme'])
            news_kr = fetch_news_headline(kr_query, lang='ko')
            if news_kr:
                st.markdown(f"**🇰🇷 Korea**: [{news_kr[0]['title']}]({news_kr[0]['link']})")

    st.markdown("---")

    # [2] 전체 테마별 상세 브리핑 (리스트 뷰)
    st.subheader("📋 Sector & Theme Impact Report (EN vs KR)")
    
    # 탭으로 상승/하락 이슈 구분
    tab_rise, tab_fall = st.tabs(["🚀 상승 모멘텀 (Bullish)", "💧 하락 리스크 (Bearish)"])
    
    with tab_rise:
        risers = [n for n in hot_narratives if n['pct_change'] > 0]
        if risers:
            for item in risers:
                with st.expander(f"**{item['theme']}**: {item['proxy']} (+{item['pct_change']:.2f}%)", expanded=True):
                    c1, c2, c3 = st.columns([1.2, 1.2, 0.6])
                    
                    # English News
                    with c1:
                        st.markdown(f"#### 🇺🇸 Global Perspective")
                        news_en = fetch_news_headline(item['query'], lang='en')
                        for n in news_en:
                            st.success(f"**{n['source']}**: [{n['title']}]({n['link']})")

                    # Korean News
                    with c2:
                        st.markdown(f"#### 🇰🇷 Domestic View")
                        kr_query = THEME_KR_QUERIES.get(item['theme'], item['theme'])
                        news_kr = fetch_news_headline(kr_query, lang='ko')
                        for n in news_kr:
                            st.success(f"**{n['source']}**: [{n['title']}]({n['link']})")

                    with c3:
                        st.markdown(f"#### 📈 Price Action")
                        st.line_chart(item['history'])
        else:
            st.write("오늘 눈에 띄게 상승한 주요 테마가 없습니다.")

    with tab_fall:
        fallers = [n for n in hot_narratives if n['pct_change'] <= 0]
        if fallers:
            for item in fallers:
                with st.expander(f"**{item['theme']}**: {item['proxy']} ({item['pct_change']:.2f}%)", expanded=True):
                    c1, c2, c3 = st.columns([1.2, 1.2, 0.6])
                    
                    # English News
                    with c1:
                        st.markdown(f"#### 🇺🇸 Global Perspective")
                        news_en = fetch_news_headline(item['query'], lang='en')
                        for n in news_en:
                            st.error(f"**{n['source']}**: [{n['title']}]({n['link']})")
                            
                    # Korean News
                    with c2:
                        st.markdown(f"#### 🇰🇷 Domestic View")
                        kr_query = THEME_KR_QUERIES.get(item['theme'], item['theme'])
                        news_kr = fetch_news_headline(kr_query, lang='ko')
                        for n in news_kr:
                            st.error(f"**{n['source']}**: [{n['title']}]({n['link']})")
                            
                    with c3:
                        st.markdown(f"#### 📉 Price Action")
                        st.line_chart(item['history'])
        else:
            st.write("오늘 눈에 띄게 하락한 주요 테마가 없습니다.")

    st.markdown("---")
    st.caption("*데이터: Yahoo Finance, Google News RSS")

# ---------------------------------------------------------
# 5. ETF 운용사 배틀 (New Feature)
# ---------------------------------------------------------

# 주요 테마별 대표 ETF 매핑 (KODEX vs TIGER vs ACE)
BATTLE_THEMES = {
    "🇺🇸 미국 AI & 반도체": {
        "TIGER 미국필라반도체": "381180", 
        "KODEX 미국반도체MV": "396500", 
        "ACE 글로벌반도체TOP4": "444200"
    },
    "🇺🇸 미국 빅테크 Top": {
        "TIGER 미국테크TOP10": "381170", 
        "KODEX 미국빅테크10(H)": "091160",
        "ACE 미국빅테크TOP7": "465580"
    },
    "🇰🇷 2차전지/배터리": {
        "KODEX 2차전지산업": "305720", 
        "TIGER 2차전지테마": "305540",
    },
    "🚀 우주항공 & 방산 (Defense)": {
        "TIGER K방산&우주": "463250",
        "SOL 우주항공&UAM": "442560",
        "ARIRANG 우주항공&UAM": "433250"
    },
    "🧬 K-바이오 & 헬스케어": {
        "KODEX 바이오": "244580",
        "TIGER 바이오TOP10": "364980",
        "TIMEFOLIO K바이오액티브": "463050"
    },
    "💰 미국 배당 다우존스 (한국판 SCHD)": {
        "TIGER 미국배당다우존스": "458730",
        "SOL 미국배당다우존스": "446720", 
        "ACE 미국배당다우존스": "402970"
    },
    "🇮🇳 인도 니프티50 (Next China)": {
        "KODEX 인도Nifty50": "425930",
        "TIGER 인도니프티50": "453870",
        "KOSEF 인도Nifty50(합성)": "200250"
    },
    "🇯🇵 일본 반도체 소부장": {
        "TIGER 일본반도체FACTSET": "465660",
        "ACE 일본반도체": "469160", 
        "PLUS 일본반도체소부장": "464920"
    }
}

@st.cache_data(ttl=3600)
def fetch_etf_comparison(tickers):
    """선택된 ETF들의 주가 데이터 수집 (네이버 금융 크롤링 - SSL 무시)"""
    import xml.etree.ElementTree as ET
    import requests
    
    df_list = []
    
    # SSL 인증 무시 세션 설정
    session = requests.Session()
    session.verify = False
    
    for name, code in tickers.items():
        try:
            # 네이버 금융 차트 데이터 요청 (최근 300일)
            url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=300&requestType=0"
            response = session.get(url)
            
            root = ET.fromstring(response.text)
            data = []
            
            for item in root.findall('./chartdata/item'):
                vals = item.get('data').split('|')
                # 0:날짜, 4:종가
                dt = pd.to_datetime(vals[0], format="%Y%m%d")
                close = float(vals[4])
                data.append({'Date': dt, name: close})
                
            df = pd.DataFrame(data)
            if not df.empty:
                df.set_index('Date', inplace=True)
                # 누적 수익률로 변환
                first_price = df.iloc[0, 0]
                if first_price > 0:
                    df = (df / first_price - 1) * 100
                    df_list.append(df)
                    
        except Exception as e:
            # st.error(f"{name} 수집 실패: {e}")
            continue
            
    if df_list:
        combined = pd.concat(df_list, axis=1).dropna()
        return combined
    return pd.DataFrame()

# ---------------------------------------------------------
# [기존 기능 유지] 스카우터 & ETF
# ---------------------------------------------------------
if menu == "⚔️ ETF 운용사 배틀":
    st.title("⚔️ ETF Brand Battle")
    st.markdown("같은 테마, 다른 수익률! **메이저 운용사(KODEX, TIGER, ACE)** 상품들의 성과를 직접 비교해보세요.")
    
    target_theme = st.selectbox("비교할 테마를 선택하세요", list(BATTLE_THEMES.keys()))
    
    if target_theme:
        st.subheader(f"🥊 {target_theme} 진검승부")
        
        tickers = BATTLE_THEMES[target_theme]
        
        # 1. 경쟁 상품 라인업
        cols = st.columns(len(tickers))
        for i, (name, code) in enumerate(tickers.items()):
            cols[i].info(f"**{name}**\n\n({code})")
            
        # 2. 수익률 차트
        st.write("")
        with st.spinner("선수 입장 중... (데이터 로딩)"):
            df_perf = fetch_etf_comparison(tickers)
        
        if not df_perf.empty:
            container = st.container(border=True)
            with container:
                st.markdown("##### 📈 1년 누적 수익률 대결 (YTD)")
                st.line_chart(df_perf)
                
                # 최종 승자 (현재 기준 1등)
                final_returns = df_perf.iloc[-1].sort_values(ascending=False)
                winner = final_returns.index[0]
                gap = final_returns.iloc[0] - final_returns.iloc[-1]
                
                st.success(f"🏆 현재 1위: **{winner}** (+{final_returns.iloc[0]:.2f}%)")
                st.caption(f"1등과 꼴등의 수익률 격차는 **{gap:.2f}%p** 입니다.")
                
            # 3. 상세 비교 테이블
            st.markdown("##### 🔢 기간별 수익률 상세")
            # (추가 구현 가능: 1개월, 3개월 등 구간 수익률 계산)
            st.dataframe(final_returns.to_frame(name="누적수익률(%)").style.format("{:.2f}%"), use_container_width=True)
        else:
            st.error("데이터를 불러올 수 없습니다.")

elif menu == "🔍 기업 펀더멘털 스카우터":
    st.title("🔍 Stock Fundamental Scout")
    col1, col2 = st.columns([1, 3])
    with col1:
        ticker_input = st.text_input("티커 입력 (예: NVDA, AAPL, 005930.KS)", "NVDA").strip().upper()
    with col2:
        st.write("") 
        st.write("")
        if st.button("스카우팅 시작"):
            st.session_state['scout_trigger'] = True

    if ticker_input:
        try:
            session = curequests.Session(impersonate="chrome")
            session.verify = False
            stock = yf.Ticker(ticker_input, session=session)
            info = stock.info
            
            st.subheader(f"{info.get('longName', ticker_input)} ({ticker_input})")
            
            # 가격 정보
            current_price = info.get('currentPrice', info.get('previousClose', 0))
            target_price = info.get('targetMeanPrice', 0)
            
            # 핵심 지표 카드
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("현재 주가", f"${current_price:,.2f}" if current_price else "N/A")
            m2.metric("시가총액", f"${info.get('marketCap', 0)/1e9:,.1f} B" if info.get('marketCap') else "N/A")
            m3.metric("52주 최고가", f"${info.get('fiftyTwoWeekHigh', 0):,.2f}")
            m4.metric("목표주가", f"${target_price:,.2f}" if target_price else "N/A", 
                        delta=f"{(target_price/current_price - 1)*100:.1f}% Upside" if target_price and current_price else None)

            st.markdown("---")
            
            t1, t2 = st.tabs(["📊 밸류에이션 & 수익성", "📈 주가 차트"])
            
            with t1:
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("##### 💎 밸류에이션")
                    df_val = pd.DataFrame([
                        {"지표": "Trailing P/E", "값": info.get('trailingPE', 'N/A')},
                        {"지표": "Forward P/E", "값": info.get('forwardPE', 'N/A')},
                        {"지표": "PEG Ratio", "값": info.get('pegRatio', 'N/A')},
                        {"지표": "PBR", "값": info.get('priceToBook', 'N/A')},
                    ])
                    st.dataframe(df_val, hide_index=True, use_container_width=True)
                    
                with c2:
                    st.markdown("##### 💰 수익성 & 배당")
                    df_prf = pd.DataFrame([
                        {"지표": "ROE", "값": f"{info.get('returnOnEquity', 0)*100:.2f}%" if info.get('returnOnEquity') else 'N/A'},
                        {"지표": "Profit Margin", "값": f"{info.get('profitMargins', 0)*100:.2f}%" if info.get('profitMargins') else 'N/A'},
                        {"지표": "Dividend Yield", "값": f"{info.get('dividendRate', 0)*100:.2f}%" if info.get('dividendRate') else 'N/A'},
                    ])
                    st.dataframe(df_prf, hide_index=True, use_container_width=True)
                
                st.info(f"💡 {info.get('longBusinessSummary', '기업 설명 정보가 없습니다.')[:300]}...")

            with t2:
                hist = stock.history(period="1y")
                if not hist.empty:
                    st.line_chart(hist['Close'])
                    
        except Exception as e:
            st.error(f"데이터 조회 실패: {e}")

elif menu == "📊 타임폴리오 ETF 분석":
    st.title("📊 TIMEFOLIO ETF Comparison & Monitor")
    
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

    # 분석 모드 선택
    mode = st.radio("분석 모드", ["단일 상품 모니터링", "⚔️ ETF 비교 분석"], horizontal=True)

    if mode == "단일 상품 모니터링":
        c1, c2 = st.columns(2)
        with c1:
            cat = st.selectbox("분류", list(etf_categories.keys()))
        with c2:
            name = st.selectbox("상품명", list(etf_categories[cat].keys()))
        
        target_idx = etf_categories[cat][name]
        
        if st.button("데이터 분석 및 리밸런싱 요약"):
            with st.spinner(f"'{name}' 데이터를 수집 및 분석 중입니다..."):
                try:
                    monitor = ActiveETFMonitor(url=f"https://timefolioetf.co.kr/m11_view.php?idx={target_idx}", etf_name=name)
                    today = datetime.now(pytz.timezone('Asia/Seoul')).strftime("%Y-%m-%d")
                    df_today = monitor.get_portfolio_data(today)
                    monitor.save_data(df_today, today)
                    
                    try:
                        prev_day = monitor.get_previous_business_day(today)
                        df_prev = monitor.load_data(prev_day)
                        analysis = monitor.analyze_rebalancing(df_today, df_prev, prev_day, today)
                        analysis_success = True
                    except Exception as e:
                        st.warning(f"전일 데이터를 찾을 수 없어 리밸런싱 분석을 건너뜁니다: {e}")
                        analysis_success = False
                        df_prev = None

                    st.success(f"✅ {name} 데이터 분석 완료" + (f" (기준: {today} vs {prev_day})" if analysis_success else ""))

                    if analysis_success:
                        st.subheader("🔄 리밸런싱 정밀 분석 (시장수익률 조정 반영)")
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("비중 확대", f"{len(analysis['increased_stocks'])} 종목")
                        m2.metric("비중 축소", f"{len(analysis['decreased_stocks'])} 종목")
                        m3.metric("신규 편입", f"{len(analysis['new_stocks'])} 종목")
                        m4.metric("완전 편출", f"{len(analysis['removed_stocks'])} 종목")

                        tab1, tab2, tab3 = st.tabs(["주요 변경내역", "세부 변동", "전체 포트폴리오"])
                        with tab1:
                            c1, c2 = st.columns(2)
                            with c1:
                                st.markdown("##### 🟢 신규 편입")
                                if analysis['new_stocks']:
                                    rows = []
                                    for s in analysis['new_stocks']:
                                        rows.append({"종목명": s['종목명'], "현재비중": f"{s['비중_today']:.2f}%", "순수변동": f"+{s['순수_비중변화']:.2f}%p"})
                                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                                else:
                                    st.caption("신규 편입 종목 없음")
                            with c2:
                                st.markdown("##### 🔴 완전 편출")
                                if analysis['removed_stocks']:
                                    rows = []
                                    for s in analysis['removed_stocks']:
                                        rows.append({"종목명": s['종목명'], "이전비중": f"{s['비중_prev']:.2f}%", "순수변동": f"{s['순수_비중변화']:.2f}%p"})
                                    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
                                else:
                                    st.caption("완전 편출 종목 없음")

                        with tab2:
                            c1, c2 = st.columns(2)
                            with c1:
                                st.markdown("##### 🔼 비중 확대 (Top 5)")
                                if analysis['increased_stocks']:
                                    df_inc = pd.DataFrame(analysis['increased_stocks']).sort_values('순수_비중변화', ascending=False).head(5)
                                    display_df = df_inc[['종목명', '비중_prev', '비중_today', '순수_비중변화']].copy()
                                    display_df.columns = ['종목명', '이전(%)', '현재(%)', '변동(%p)']
                                    st.dataframe(display_df.style.format({'이전(%)': '{:.2f}', '현재(%)': '{:.2f}', '변동(%p)': '+{:.2f}'}), hide_index=True, use_container_width=True)
                                else:
                                    st.caption("비중 확대 종목 없음")

                            with c2:
                                st.markdown("##### 🔽 비중 축소 (Top 5)")
                                if analysis['decreased_stocks']:
                                    df_dec = pd.DataFrame(analysis['decreased_stocks']).sort_values('순수_비중변화', ascending=True).head(5)
                                    display_df = df_dec[['종목명', '비중_prev', '비중_today', '순수_비중변화']].copy()
                                    display_df.columns = ['종목명', '이전(%)', '현재(%)', '변동(%p)']
                                    st.dataframe(display_df.style.format({'이전(%)': '{:.2f}', '현재(%)': '{:.2f}', '변동(%p)': '{:.2f}'}), hide_index=True, use_container_width=True)
                                else:
                                    st.caption("비중 축소 종목 없음")
                    else:
                        st.subheader("📋 전체 포트폴리오 구성")

                    col_chart, col_list = st.columns([1, 1])
                    with col_chart:
                        chart_df = df_today.copy()
                        chart_df['비중'] = pd.to_numeric(chart_df['비중'], errors='coerce')
                        chart_df.loc[chart_df['비중'] < 1.0, '종목명'] = '기타'
                        fig = px.pie(chart_df, values="비중", names="종목명", hole=0.4, title="포트폴리오 비중", color_discrete_sequence=px.colors.qualitative.Set3)
                        st.plotly_chart(fig, use_container_width=True)

                    with col_list:
                        top_df = df_today[['종목명', '비중', '수량']].head(15)
                        st.dataframe(top_df.style.format({'비중': '{:.2f}%', '수량': '{:,}'}), use_container_width=True)

                    # 엑셀 다운로드
                    st.markdown("---")
                    e_new = pd.DataFrame(analysis['new_stocks']) if analysis_success and analysis['new_stocks'] else pd.DataFrame()
                    e_inc = pd.DataFrame(analysis['increased_stocks']) if analysis_success and analysis['increased_stocks'] else pd.DataFrame()
                    e_dec = pd.DataFrame(analysis['decreased_stocks']) if analysis_success and analysis['decreased_stocks'] else pd.DataFrame()
                    excel_data = to_excel(e_new, e_inc, e_dec, df_today, today)
                    st.download_button(label="📊 엑셀 리포트 내려받기 (.xlsx)", data=excel_data, file_name=f"{name}_Report_{today}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                except Exception as e:
                    st.error(f"오류 발생: {e}")
    
    # [새 기능] ETF 비교 분석
    elif mode == "⚔️ ETF 비교 분석":
        st.subheader("⚔️ ETF Portfolio Comparison")
        st.markdown("두 개의 Timefolio Active ETF 구성을 비교하여 **교집합 종목**과 **Overlap 비중**을 확인합니다.")
        
        flat_etfs = {}
        for cat, items in etf_categories.items():
            for name, idx in items.items():
                flat_etfs[f"[{cat}] {name}"] = idx
        
        c1, c2 = st.columns(2)
        with c1:
            etf_a_key = st.selectbox("ETF A 선택", list(flat_etfs.keys()), index=0)
        with c2:
            etf_b_key = st.selectbox("ETF B 선택", list(flat_etfs.keys()), index=1)
            
        if st.button("비교 분석 실행"):
            if etf_a_key == etf_b_key:
                st.warning("서로 다른 ETF를 선택해주세요.")
            else:
                with st.spinner("두 ETF 데이터를 수집 및 비교 중..."):
                    try:
                        today = datetime.now(pytz.timezone('Asia/Seoul')).strftime("%Y-%m-%d")
                        
                        # Data A
                        mon_a = ActiveETFMonitor(url=f"https://timefolioetf.co.kr/m11_view.php?idx={flat_etfs[etf_a_key]}")
                        df_a = mon_a.get_portfolio_data(today)
                        
                        # Data B
                        mon_b = ActiveETFMonitor(url=f"https://timefolioetf.co.kr/m11_view.php?idx={flat_etfs[etf_b_key]}")
                        df_b = mon_b.get_portfolio_data(today)
                        
                        # 비교 로직 using 종목코드
                        # 현금 제외
                        df_a = df_a[df_a['종목명'] != '현금']
                        df_b = df_b[df_b['종목명'] != '현금']
                        
                        merged = pd.merge(df_a[['종목코드', '종목명', '비중']], df_b[['종목코드', '종목명', '비중']], 
                                        on='종목코드', how='inner', suffixes=('_A', '_B'))
                        merged['종목명'] = merged['종목명_A'] # 이름 통일
                        
                        # Overlap Weight 계산 (두 비중 중 작은 값의 합)
                        merged['Overlap'] = merged[['비중_A', '비중_B']].min(axis=1)
                        total_overlap = merged['Overlap'].sum()
                        
                        # 결과 표시
                        st.markdown("---")
                        res_col1, res_col2 = st.columns(2)
                        with res_col1:
                            st.metric("공통 보유 종목 수", f"{len(merged)} 개")
                        with res_col2:
                            st.metric("Overlap Weight (중복 비중)", f"{total_overlap:.2f}%")
                            
                        # 시각화 (양쪽 비중 비교)
                        if not merged.empty:
                            st.subheader("📊 공통 종목 비중 비교")
                            merged_sorted = merged.sort_values('Overlap', ascending=False)
                            
                            fig = go.Figure(data=[
                                go.Bar(name=etf_a_key, x=merged_sorted['종목명'], y=merged_sorted['비중_A']),
                                go.Bar(name=etf_b_key, x=merged_sorted['종목명'], y=merged_sorted['비중_B'])
                            ])
                            fig.update_layout(barmode='group', title="공통 종목 비중 비교")
                            st.plotly_chart(fig, use_container_width=True)
                            
                            st.dataframe(merged[['종목명', '비중_A', '비중_B', 'Overlap']].style.format("{:.2f}%", subset=['비중_A', '비중_B', 'Overlap']), use_container_width=True)
                        else:
                            st.info("두 ETF 간 겹치는 종목이 없습니다.")
                            
                    except Exception as e:
                        st.error(f"비교 분석 중 오류 발생: {e}")

    st.markdown("---")
    st.caption("Data source: TIMEFOLIO ETF Official Website")