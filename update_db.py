import os
import pandas as pd
from sqlalchemy import create_engine, text
from pykrx import stock
from datetime import datetime, timedelta
import time

# 1. DB 접속 정보
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = "34.50.62.220"
db_name = "stock_db"

# SQL 연동 엔진 생성
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4")

def get_market_data(target_date_str, market_name):
    """
    target_date_str: '20260408' 형식
    """
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"
    
    # [시세 수집]
    df_price = stock.get_market_ohlcv_by_date(target_date_str, target_date_str, mkt)
    if df_price.empty: return pd.DataFrame()
    
    # [종목명/수급 수집]
    tickers = stock.get_market_ticker_list(target_date_str, market=mkt)
    ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(target_date_str, target_date_str, mkt)
    
    # [데이터 정리]
    df_price = df_price.reset_index()
    df_price['nm'] = df_price['티커'].map(ticker_names)
    df_price['date'] = pd.to_datetime(target_date_str, format='%Y%m%d').strftime('%Y-%m-%d')
    
    final = pd.merge(df_price, df_investor, left_on='티커', right_on='티커')
    
    # 컬럼명 매칭 (사용자 DB 스키마 기준)
    final = final.rename(columns={
        '시가':'open', '고가':'high', '저가':'low', '종가':'close', 
        '거래량':'volume', '등락률':'change_rate', 
        '외국인':'for_net', '기관합계':'inst_net', '개인':'ind_net'
    })
    
    # ⭐ [핵심 포인트] 데이터를 강제로 'int'로 바꾸지 않고 'float'로 유지!
    # 금융 데이터의 소수점 아래 숫자들을 있는 그대로 보존하며 에러를 방지합니다.
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']
    for col in numeric_cols:
        # to_numeric으로 변환하되, 에러는 무시하고 NaN은 0으로 채움 (반올림 안함)
        final[col] = pd.to_numeric(final[col], errors='coerce').fillna(0)
    
    return final[['date', 'nm', 'open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']]

def update_process():
    today = datetime.now()
    # 최근 10일치를 훑으며 누락된 2026년 데이터를 채웁니다.
    for i in range(10):
        target_dt = (today - timedelta(days=i)).strftime('%Y%m%d')
        target_date_db = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        
        for table in ["kospi_stocks", "kosdaq_stocks"]:
            try:
                # 중복 체크 (2026년 데이터 기준)
                query = text(f"SELECT count(*) as cnt FROM {table} WHERE date LIKE :dt")
                existing_cnt = pd.read_sql(query, engine, params={"dt": f"{target_date_db}%"}).iloc[0, 0]
                
                if existing_cnt > 500:
                    print(f"✅ [{target_date_db}] {table}: 스킵")
                    continue
                
                print(f"🔄 [{target_date_db}] {table}: 2026년 데이터 수집 및 전송 중...")
                df = get_market_data(target_dt, table)
                
                if not df.empty:
                    # DB 테이블이 소수점을 받을 준비가 되어 있으므로 float 데이터를 그대로 저장
                    df.to_sql(table, engine, if_exists='append', index=False)
                    print(f"🚀 [{target_date_db}] {table}: 업데이트 성공!")
                    time.sleep(1) 
                else:
                    print(f"⏸️ [{target_date_db}] {table}: 장이 열리지 않은 날입니다.")
                    
            except Exception as e:
                print(f"❌ [{target_date_db}] {table} 오류: {e}")

if __name__ == "__main__":
    update_process()
