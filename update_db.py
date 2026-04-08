import os
import pandas as pd
from sqlalchemy import create_engine
from pykrx import stock
from datetime import datetime, timedelta

# DB 연결
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = "34.50.62.220"
db_name = "stock_db"

engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4")

def get_full_market_data(target_date, market_name):
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"
    df_price = stock.get_market_ohlcv_by_date(target_date, target_date, mkt, detail=True)
    if df_price.empty: return pd.DataFrame()
    
    tickers = stock.get_market_ticker_list(target_date, market=mkt)
    ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, mkt)
    
    df_price = df_price.reset_index()
    df_price['nm'] = df_price['티커'].map(ticker_names)
    df_price['date'] = pd.to_datetime(target_date).strftime('%Y-%m-%d')
    
    final = pd.merge(df_price, df_investor, left_on='티커', right_on='티커')
    final = final.rename(columns={
        '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close', 
        '거래량': 'volume', '등락률': 'change_rate', 
        '외국인': 'for_net', '기관합계': 'inst_net', '개인': 'ind_net'
    })
    return final[['date', 'nm', 'open', 'high', 'low', 'close', 'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']]

def update_all_data():
    today = datetime.now()
    
    # ⭐ [수정됨] 날짜 비교 로직을 제거하고 최근 10일치를 "무조건" 시도합니다.
    # DB에 이미 데이터가 있으면 SQL 에러가 날 수 있으므로, 
    # try-except로 감싸서 없는 날짜만 쏙쏙 채우도록 만들었습니다.
    for i in range(10): 
        target_dt = (today - timedelta(days=i)).strftime('%Y%m%d')
        target_date_str = pd.to_datetime(target_dt).strftime('%Y-%m-%d')
        
        for m_table in ["kospi_stocks", "kosdaq_stocks"]:
            try:
                # 해당 날짜 데이터가 있는지 아주 간단하게 체크
                res = pd.read_sql(f"SELECT count(*) as cnt FROM {m_table} WHERE date LIKE '{target_date_str}%'", engine)
                if res.iloc[0, 0] > 500:
                    print(f"✅ {target_date_str} {m_table} : 이미 데이터가 있습니다. 패스!")
                    continue

                print(f"🔄 {target_date_str} {m_table} : 수집 및 입력 시작...")
                full_df = get_full_market_data(target_dt, m_table)
                
                if not full_df.empty:
                    full_df.to_sql(m_table, engine, if_exists='append', index=False)
                    print(f"🚀 {target_date_str} {m_table} : 업데이트 성공!")
            except Exception as e:
                print(f"❌ {target_date_str} {m_table} 에러: {e}")

if __name__ == "__main__":
    update_all_data()
