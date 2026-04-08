import os
import pandas as pd
from sqlalchemy import create_engine
from pykrx import stock
import yfinance as yf
import time
from datetime import datetime, timedelta

# 1. DB 연결 (GitHub Secrets에서 정보를 가져옵니다)
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = "34.47.100.183"
db_name = "quant_db"

engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4")

def update_macro():
    """거시경제 10년치 및 최신화 (yfinance)"""
    macros = {"exchange_rate": "USDKRW=X", "us_10y_bond": "^TNX", "dollar_index": "DX-Y.NYB", "oil_wti": "CL=F", "gold": "GC=F"}
    for table, ticker in macros.items():
        try:
            df = yf.download(ticker, start="2015-01-01", progress=False)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                df = df.reset_index()
                df.columns = [str(c).lower().strip() for c in df.columns]
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                df.to_sql(table, engine, if_exists='replace', index=False)
                print(f"✅ Macro {table} updated")
        except Exception as e: print(f"❌ Macro {table} error: {e}")

def update_stocks(market_table):
    """주식 데이터 빈틈 메우기 및 오늘 데이터 추가 (pykrx)"""
    today = datetime.now().strftime('%Y%m%d')
    start_10y = "20150101"
    mkt = "KOSPI" if "kospi" in market_table else "KOSDAQ"
    tickers = stock.get_market_ticker_list(today, market=mkt)
    
    # 깃허브 6시간 제한을 고려하여, 한 번 실행 시 200개씩만 처리 (자주 돌리면 결국 다 채워짐)
    print(f"🏗️ {mkt} 업데이트 시작...")
    for i, t in enumerate(tickers[:200]): 
        try:
            # DB의 마지막 날짜 확인
            res = pd.read_sql(f"SELECT MAX(date) as last_date FROM {market_table} WHERE code = '{t}'", engine)
            last_date = res.iloc[0, 0]
            
            # 데이터가 없으면 10년 전부터, 있으면 그 이후부터 수집
            fetch_start = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y%m%d') if last_date else start_10y
            if fetch_start > today: continue
            
            df = stock.get_market_ohlcv_by_date(fetch_start, today, t)
            if df.empty: continue
            
            df_inv = stock.get_market_net_purchases_of_equities_by_ticker(fetch_start, today, t)
            df_for = stock.get_exhaustion_rates_of_foreign_investment_by_date(fetch_start, today, t)
            
            final = pd.concat([df, df_inv[['외국인', '기관', '개인']], df_for[['수량', '지분율']]], axis=1)
            final = final.reset_index()
            final.columns = ['date','open','high','low','close','volume','change','foreign_net','inst_net','indiv_net','foreign_hold','foreign_ratio']
            final['code'] = t
            final['date'] = pd.to_datetime(final['date']).dt.strftime('%Y-%m-%d')
            
            final.to_sql(market_table, engine, if_exists='append', index=False)
            if i % 50 == 0: print(f"🚀 {mkt} {i}종목 완료")
            time.sleep(0.1)
        except: continue

if __name__ == "__main__":
    update_macro()
    update_stocks("kospi_stocks")
    update_stocks("kosdaq_stocks")
