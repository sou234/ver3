# 🤖 AI-Driven Investment System (Project 2026)

이 프로젝트는 **거시적 시장 트렌드(Macro)**부터 **개별 종목 알파(Alpha)**, 그리고 **경쟁사 동향(Competitor)**까지 하나의 대시보드에서 분석하는 **AI 기반 투자 의사결정 시스템**입니다.

## 🚀 주요 기능 (3 Pillars)

### 1. 📈 Super-Stock Dashboard
*   **Macro Insight**: StatCounter 데이터를 기반으로 글로벌 검색엔진 및 모바일 OS(안드로이드 vs iOS) 시장 점유율 트렌드를 추적합니다.
*   **Competition Tracker**: 구글/애플 등 빅테크 기업들의 독점력 변화를 시각화하여 Top-Down 투자 아이디어를 제공합니다.

### 2. 💎 Earnings Idio Score
*   **High Probability Alpha**: 골드만삭스(Goldman Sachs) 방법론을 기반으로 VIX와 연동된 실적 발표 수익률을 분석합니다.
*   **5-Factor Regression**: 시장(Market), 섹터(Sector), 스타일(Size, Value, Momentum) 효과를 제거한 **순수 잔차(Residual)**를 추출하여 "진짜 종목의 실력"을 평가합니다.

### 3. 📊 Active ETF Analysis
*   **Competitor Intelligence**: 타임폴리오(Timefolio), 키움(Kiwoom) 등 상위 1% 액티브 ETF 운용사의 포트폴리오를 매일 추적합니다.
*   **Intent Detection**: 단순 주가 등락(Drift)을 배제하고, 매니저의 **의도적인 리밸런싱(Intent)** 내역만 발라내어 벤치마킹합니다.

## 📂 핵심 파일 구성 (GitHub 업로드 필수)

*   `app.py`: 메인 애플리케이션 실행 파일 (Streamlit)
*   `logic_idio.py`: Idio Score 및 5-Factor Regression 핵심 로직 모듈
*   `logic_crawler.py`: 데이터 수집(크롤링) 및 정제 모듈
*   `etf.py`: 타임폴리오 ETF 크롤링 및 분석 모듈
*   `etf_kiwoom.py`: 키움 ETF API 연동 모듈
*   `universe_stocks.csv`: 분석 대상 종목 리스트 (유니버스)
*   `requirements.txt`: 프로젝트 실행에 필요한 라이브러리 목록
*   `project_ppt.html`: 프로젝트 결과 발표 자료 (Standalone HTML)

## 💻 실행 방법 (로컬)

1. 파이썬 설치 (3.9 이상 권장)
2. 라이브러리 설치:
   ```bash
   pip install -r requirements.txt
   ```
3. 앱 실행:
   ```bash
   streamlit run app.py
   ```

## ☁️ 배포 방법 (Streamlit Cloud)
1. 이 저장소(Repository)를 GitHub에 업로드합니다.
2. [Streamlit Cloud](https://streamlit.io/cloud)에 로그인하여 GitHub를 연결합니다.
3. Main file로 `app.py`를 선택하고 배포합니다.
