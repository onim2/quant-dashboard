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

def get_market_data(target_date, market_name):
    """표준화된 주식 데이터를 수집합니다."""
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"
    
    # [시세 정보 수집] - 최신 pykrx 버전에 맞춰 detail 옵션 제거
    df_price = stock.get_market_ohlcv_by_date(target_date, target_date, mkt)
    if df_price.empty: 
        return pd.DataFrame()
    
    # [종목명 및 투자자 정보 수집]
    tickers = stock.get_market_ticker_list(target_date, market=mkt)
    ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, mkt)
    
    # [데이터 정리]
    df_price = df_price.reset_index()
    df_price['nm'] = df_price['티커'].map(ticker_names)
    df_price['date'] = pd.to_datetime(target_date).strftime('%Y-%m-%d')
    
    # 시세와 수급 데이터 병합
    final = pd.merge(df_price, df_investor, left_on='티커', right_on='티커')
    
    # ⭐ [필수] 사용자님 DB 테이블 컬럼명과 100% 일치화
    final = final.rename(columns={
        '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close', 
        '거래량': 'volume', '등락률': 'change_rate', 
        '외국인': 'for_net', '기관합계': 'inst_net', '개인': 'ind_net'
    })
    
    # 필요한 컬럼만 추출 (정해진 순서대로)
    return final[['date', 'nm', 'open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']]

def update_process():
    today = datetime.now()
    # 최근 10일치를 검사하여 빈 날짜를 모두 채웁니다.
    for i in range(10):
        target_dt = (today - timedelta(days=i)).strftime('%Y%m%d')
        target_date_str = pd.to_datetime(target_dt).strftime('%Y-%m-%d')
        
        for table in ["kospi_stocks", "kosdaq_stocks"]:
            try:
                # % 기호 충돌 방지 쿼리
                query = text(f"SELECT count(*) as cnt FROM {table} WHERE date LIKE :dt")
                existing_cnt = pd.read_sql(query, engine, params={"dt": f"{target_date_str}%"}).iloc[0, 0]
                
                if existing_cnt > 500:
                    print(f"✅ [{target_date_str}] {table}: 이미 데이터 존재 (건너뜀)")
                    continue
                
                print(f"🔄 [{target_date_str}] {table}: 수집 시작...")
                df = get_market_data(target_dt, table)
                
                if not df.empty:
                    df.to_sql(table, engine, if_exists='append', index=False)
                    print(f"🚀 [{target_date_str}] {table}: {len(df)}건 업데이트 완료!")
                    time.sleep(1) 
                else:
                    print(f"⏸️ [{target_date_str}] {table}: 데이터 없음 (휴장일)")
                    
            except Exception as e:
                print(f"❌ [{target_date_str}] {table} 오류: {e}")

if __name__ == "__main__":
    print("🎬 주식 자동 업데이트 시스템 가동...")
    update_process()
    print("🏁 모든 업데이트 작업이 완료되었습니다.")
