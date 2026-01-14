# 🍊 Mirae Asset Market Briefing & ETF Battle

이 프로젝트는 **최신 시장 트렌드(Catalyst)**와 **ETF 운용사 성과 비교(Brand Battle)**를 한눈에 볼 수 있는 Streamlit 웹 애플리케이션입니다.

## 🚀 주요 기능
1.  **Daily Market Catalyst**: 미국/한국/중국 시장의 핵심 재료(실적, 정책, 신기술) 뉴스 3줄 요약
2.  **ETF Brand Battle**: 반도체, 배당주, 인도 등 주요 테마별 운용사(KODEX, TIGER, ACE 등) 수익률 진검승부
3.  **TIMEFOLIO ETF 분석**: 실시간 PDF(자산구성내역) 및 리밸런싱 추적

## 📂 파일 구성
*   `app.py`: 메인 애플리케이션 실행 파일 (Streamlit)
*   `etf.py`: ETF 데이터 처리 및 분석 로직 모듈
*   `requirements.txt`: 프로젝트 실행에 필요한 파이썬 라이브러리 목록

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
2. [Streamlit Cloud](https://streamlit.io/cloud)에 로그인합니다.
3. 'New app'을 클릭하고 GitHub 저장소를 연결합니다.
4. Main file로 `app.py`를 선택하고 'Deploy'를 클릭하면 끝!
