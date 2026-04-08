import os
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# 1. 깃허브 금고(Secrets)에서 아이디와 비번을 안전하게 가져옵니다.
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = "34.47.100.183"
db_name = "quant_db"

# 데이터베이스 연결 주소 생성
db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(db_url)

def update_macro_data(ticker, table_name):
    try:
        # DB에서 마지막 날짜 확인
        query = f"SELECT MAX(date) FROM {table_name}"
        last_date_df = pd.read_sql(query, engine)
        last_date = last_date_df.iloc[0, 0]
        
        # 마지막 날짜 다음 날부터 수집
        start_date = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = datetime.now().strftime('%Y-%m-%d')

        if start_date < end_date:
            print(f"🚀 {table_name} 업데이트 시작: {start_date} ~")
            df = yf.download(ticker, start=start_date)
            if not df.empty:
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                # 창고에 덧붙이기(append)
                df.to_sql(table_name, engine, if_exists='append', index=False)
                print(f"✅ {table_name}에 {len(df)}건 추가 완료!")
            else:
                print(f"📅 {table_name}: 새로운 데이터가 없습니다.")
        else:
            print(f"✨ {table_name}: 이미 최신 상태입니다.")
    except Exception as e:
        print(f"❌ {table_name} 업데이트 중 오류: {e}")

# 2. 업데이트 실행 (환율, 금리, 종목 등)
if __name__ == "__main__":
    update_macro_data("USDKRW=X", "exchange_rate")  # 환율
    update_macro_data("^TNX", "us_10y_bond")        # 금리
    update_macro_data("005930.KS", "kospi_stocks")  # 삼성전자(예시)
