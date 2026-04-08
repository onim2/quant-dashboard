import os
import pandas as pd
from sqlalchemy import create_engine
from pykrx import stock
from datetime import datetime, timedelta

# 1. DB 연결 (스트림릿과 동일한 DB 주소)
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = "34.50.62.220"
db_name = "stock_db"

engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4")

def get_full_market_data(target_date, market_name):
    """특정 날짜의 전 종목 시세 및 수급 데이터를 가져옵니다."""
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"
    
    # [시세 데이터 수집]
    df_price = stock.get_market_ohlcv_by_date(target_date, target_date, mkt, detail=True)
    if df_price.empty: 
        return pd.DataFrame()
    
    # [종목명 맵핑]
    tickers = stock.get_market_ticker_list(target_date, market=mkt)
    ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    
    # [수급 데이터 수집]
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, mkt)
    
    # [데이터 다듬기]
    df_price = df_price.reset_index()
    df_price['nm'] = df_price['티커'].map(ticker_names)
    df_price['date'] = pd.to_datetime(target_date).strftime('%Y-%m-%d')
    
    # 시세와 수급 병합
    final = pd.merge(df_price, df_investor, left_on='티커', right_on='티커')
    
    # ⭐ [핵심] 사용자님 DB 스크린샷과 100% 일치하도록 이름표 변경!
    final = final.rename(columns={
        '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close', 
        '거래량': 'volume', '등락률': 'change_rate', 
        '외국인': 'for_net', '기관합계': 'inst_net', '개인': 'ind_net'
    })
    
    # DB에 삽입할 최종 컬럼 (code 제외, 순서 맞춤)
    return final[['date', 'nm', 'open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']]

def update_all_data():
    today = datetime.now()
    
    # 최근 10일치를 훑으며 누락된 날짜를 찾아냅니다. (매일 실행될 때는 오늘치만 쏙쏙 들어갑니다)
    for i in range(10): 
        target_dt = (today - timedelta(days=i)).strftime('%Y%m%d')
        target_date_str = pd.to_datetime(target_dt).strftime('%Y-%m-%d')
        
        for m_table in ["kospi_stocks", "kosdaq_stocks"]:
            try:
                # 중복 확인: 이미 해당 날짜 데이터가 500개 이상 있으면 안전하게 건너뜀
                check_query = f"SELECT count(*) FROM {m_table} WHERE date = '{target_date_str}'"
                exists = pd.read_sql(check_query, engine).iloc[0, 0]
                
                if exists > 500:
                    print(f"✅ [{target_date_str}] {m_table} : 이미 데이터가 존재합니다 ({exists}건). 스킵!")
                    continue
                
                # 누락 확인됨: 수집 후 DB에 밀어넣기
                print(f"🔄 [{target_date_str}] {m_table} : 수집 시작...")
                full_df = get_full_market_data(target_dt, m_table)
                
                if not full_df.empty:
                    full_df.to_sql(m_table, engine, if_exists='append', index=False)
                    print(f"🚀 [{target_date_str}] {m_table} : {len(full_df)}건 업데이트 완료!")
                else:
                    print(f"⏸️ [{target_date_str}] {m_table} : 데이터가 없습니다 (주말/휴일).")
            except Exception as e:
                print(f"❌ [{target_date_str}] {m_table} 에러 발생: {e}")

if __name__ == "__main__":
    print("🔥 전체 시장 데이터 업데이트 프로세스 시작...")
    update_all_data()
    print("🎉 모든 작업이 완료되었습니다!")
