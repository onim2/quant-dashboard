import os
import pandas as pd
from sqlalchemy import create_engine, text
from pykrx import stock
from datetime import datetime, timedelta
import time

# 1. DB 접속 정보 (GitHub Secrets 연동)
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = "34.50.62.220"
db_name = "stock_db"

# SQL 연동 엔진 생성
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4")

def get_market_data(target_date_str, market_name):
    """
    target_date_str: '20260408' 형식의 날짜
    """
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"
    
    # [시세 정보 수집] - pykrx 최신 버전 규격 적용
    df_price = stock.get_market_ohlcv_by_date(target_date_str, target_date_str, mkt)
    if df_price.empty: 
        return pd.DataFrame()
    
    # [종목명 및 수급 정보 수집]
    tickers = stock.get_market_ticker_list(target_date_str, market=mkt)
    ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(target_date_str, target_date_str, mkt)
    
    # [데이터 정리]
    df_price = df_price.reset_index()
    df_price['nm'] = df_price['티커'].map(ticker_names)
    # 날짜를 '2026-04-08' 형식의 문자열로 고정하여 과거 데이터 혼선 방지
    df_price['date'] = pd.to_datetime(target_date_str, format='%Y%m%d').strftime('%Y-%m-%d')
    
    # 시세와 수급 데이터 병합
    final = pd.merge(df_price, df_investor, left_on='티커', right_on='티커')
    
    # 컬럼명 매칭 (사용자 DB 스키마 기준)
    final = final.rename(columns={
        '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close', 
        '거래량': 'volume', '등락률': 'change_rate', 
        '외국인': 'for_net', '기관합계': 'inst_net', '개인': 'ind_net'
    })
    
    # ⭐ [핵심 보완] 모든 숫자 데이터를 실수형(float)으로 변환 (소수점 에러 방지)
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']
    for col in numeric_cols:
        final[col] = pd.to_numeric(final[col], errors='coerce').fillna(0)
    
    return final[['date', 'nm', 'open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']]

def update_process():
    today = datetime.now()
    print(f"🕒 현재 실행 시간: {today}")

    # 최근 10일치를 검사하여 2026년 데이터를 채웁니다.
    for i in range(10):
        current_dt = today - timedelta(days=i)
        target_dt_str = current_dt.strftime('%Y%m%d') # '20260408'
        target_date_db = current_dt.strftime('%Y-%m-%d') # '2026-04-08'
        
        for table in ["kospi_stocks", "kosdaq_stocks"]:
            try:
                # 해당 날짜 데이터가 이미 있는지 체크 (중복 방지)
                query = text(f"SELECT count(*) as cnt FROM {table} WHERE date LIKE :dt")
                existing_cnt = pd.read_sql(query, engine, params={"dt": f"{target_date_db}%"}).iloc[0, 0]
                
                if existing_cnt > 500:
                    print(f"✅ [{target_date_db}] {table}: 이미 데이터가 존재합니다. 건너뜀.")
                    continue
                
                print(f"🔄 [{target_date_db}] {table}: 수집 시작...")
                df = get_market_data(target_dt_str, table)
                
                if not df.empty:
                    df.to_sql(table, engine, if_exists='append', index=False)
                    print(f"🚀 [{target_date_db}] {table}: {len(df)}건 업데이트 성공!")
                    time.sleep(1) # 과부하 방지
                else:
                    print(f"⏸️ [{target_date_db}] {table}: 데이터가 없습니다. (휴장일 가능성)")
                    
            except Exception as e:
                print(f"❌ [{target_date_db}] {table} 처리 중 오류: {e}")

if __name__ == "__main__":
    print("🎬 주식 자동 업데이트 시스템 가동...")
    update_process()
    print("🏁 모든 업데이트 작업이 완료되었습니다.")
