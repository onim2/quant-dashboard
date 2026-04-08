import os
import pandas as pd
from sqlalchemy import create_engine
from pykrx import stock
from datetime import datetime, timedelta
import time

# 1. DB 연결 설정 (GitHub Secrets에서 배달된 정보 사용)
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = "34.50.62.220"
db_name = "stock_db"

# 연결 엔진 생성
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4")

def get_market_data(target_date, market_name):
    """특정 날짜의 시장 데이터를 수집하여 정리합니다."""
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"
    
    # 시세 데이터 수집
    df_price = stock.get_market_ohlcv_by_date(target_date, target_date, mkt, detail=True)
    if df_price.empty: return pd.DataFrame()
    
    # 종목명 및 수급 데이터 수집
    tickers = stock.get_market_ticker_list(target_date, market=mkt)
    ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, mkt)
    
    # 데이터 병합 및 가공
    df_price = df_price.reset_index()
    df_price['nm'] = df_price['티커'].map(ticker_names)
    df_price['date'] = pd.to_datetime(target_date).strftime('%Y-%m-%d')
    
    final = pd.merge(df_price, df_investor, left_on='티커', right_on='티커')
    
    # DB 컬럼명 매칭 (스크린샷 기준 최적화)
    final = final.rename(columns={
        '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close', 
        '거래량': 'volume', '등락률': 'change_rate', 
        '외국인': 'for_net', '기관합계': 'inst_net', '개인': 'ind_net'
    })
    
    return final[['date', 'nm', 'open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']]

def main_process():
    today = datetime.now()
    # 최근 10일치를 검사하여 빈틈을 메우고 오늘치를 업데이트합니다.
    for i in range(10):
        target_dt = (today - timedelta(days=i)).strftime('%Y%m%d')
        target_date_str = pd.to_datetime(target_dt).strftime('%Y-%m-%d')
        
        for table in ["kospi_stocks", "kosdaq_stocks"]:
            try:
                # 중복 데이터 체크 (이미 500개 이상 있으면 해당 날짜는 완료된 것으로 간주)
                query = f"SELECT count(*) as cnt FROM {table} WHERE date LIKE '{target_date_str}%'"
                existing_cnt = pd.read_sql(query, engine).iloc[0, 0]
                
                if existing_cnt > 500:
                    print(f"✅ [{target_date_str}] {table}: 이미 데이터가 존재합니다. (건너뜀)")
                    continue
                
                print(f"🔄 [{target_date_str}] {table}: 데이터 수집 중...")
                df = get_market_data(target_dt, table)
                
                if not df.empty:
                    df.to_sql(table, engine, if_exists='append', index=False)
                    print(f"🚀 [{target_date_str}] {table}: {len(df)}건 업데이트 성공!")
                    time.sleep(1) # 과부하 방지를 위한 짧은 휴식
                else:
                    print(f"⏸️ [{target_date_str}] {table}: 장이 열리지 않은 날입니다.")
                    
            except Exception as e:
                print(f"❌ [{target_date_str}] {table} 처리 중 오류 발생: {e}")

if __name__ == "__main__":
    print("🏁 주식 데이터 업데이트 프로세스를 시작합니다.")
    main_process()
    print("✨ 모든 작업이 끝났습니다.")
