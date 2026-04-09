import os
import builtins
import pandas as pd
from sqlalchemy import create_engine, text
from pykrx import stock
from datetime import datetime, timedelta
import time

# ══════════════════════════════════════════════════════════════════════════════
# ✅ pykrx 내부 버그 패치
# KRX API가 지수값을 float 문자열('5804.7')로 반환하기 시작했는데
# pykrx 내부에서 int('5804.7') 를 호출해 에러 발생 → 전역 패치로 해결
# ══════════════════════════════════════════════════════════════════════════════
_original_int = builtins.int

def _patched_int(x=0, *args, **kwargs):
    if isinstance(x, str) and not args and not kwargs:
        try:
            return _original_int(x)
        except ValueError:
            try:
                return _original_int(float(x))   # '5804.7' → 5804
            except (ValueError, TypeError):
                raise ValueError(f"invalid literal for int() with base 10: '{x}'")
    return _original_int(x, *args, **kwargs)

builtins.int = _patched_int
# ══════════════════════════════════════════════════════════════════════════════


# ── 1. DB 접속 정보 ─────────────────────────────────────────────────────────
db_user     = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host     = "34.50.62.220"
db_name     = "stock_db"

engine = create_engine(
    f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4",
    connect_args={"ssl": {}}
)


# ── 2. 시장 데이터 수집 ──────────────────────────────────────────────────────
def get_market_data(target_date_str, market_name):
    """
    target_date_str : '20260408' 형식
    market_name     : 'kospi_stocks' or 'kosdaq_stocks'
    """
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"

    # [시세 수집]
    try:
        df_price = stock.get_market_ohlcv_by_date(target_date_str, target_date_str, mkt)
    except Exception as e:
        print(f"    pykrx 시세 수집 실패: {e}")
        return pd.DataFrame()

    if df_price.empty:
        return pd.DataFrame()

    # [종목명 수집]
    try:
        tickers      = stock.get_market_ticker_list(target_date_str, market=mkt)
        ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    except Exception as e:
        print(f"    pykrx 종목명 수집 실패: {e}")
        return pd.DataFrame()

    # [수급 수집]
    try:
        df_investor = stock.get_market_net_purchases_of_equities_by_ticker(
            target_date_str, target_date_str, mkt
        )
    except Exception as e:
        print(f"    pykrx 수급 수집 실패 (0으로 채움): {e}")
        df_investor = pd.DataFrame()

    # [데이터 정리]
    df_price = df_price.reset_index()
    first_col = df_price.columns[0]
    df_price.rename(columns={first_col: '티커'}, inplace=True)

    df_price['nm']   = df_price['티커'].map(ticker_names)
    df_price['date'] = pd.to_datetime(target_date_str, format='%Y%m%d').strftime('%Y-%m-%d')

    # nm 없는 행(지수 등) 제거
    df_price = df_price[df_price['nm'].notna()].copy()

    if df_investor.empty:
        df_price['for_net']  = 0
        df_price['inst_net'] = 0
        df_price['ind_net']  = 0
        final = df_price
    else:
        df_investor = df_investor.reset_index()
        inv_first = df_investor.columns[0]
        df_investor.rename(columns={inv_first: '티커'}, inplace=True)
        final = pd.merge(df_price, df_investor, on='티커', how='left')

    # 컬럼명 → DB 스키마
    final = final.rename(columns={
        '시가'    : 'open',
        '고가'    : 'high',
        '저가'    : 'low',
        '종가'    : 'close',
        '거래량'  : 'volume',
        '등락률'  : 'change_rate',
        '외국인'  : 'for_net',
        '기관합계': 'inst_net',
        '개인'    : 'ind_net',
    })

    # 누락 컬럼 보정
    for col in ['for_net', 'inst_net', 'ind_net']:
        if col not in final.columns:
            final[col] = 0

    # ── DB 스키마에 맞게 타입 변환 ───────────────────────────────────────────
    final['date'] = pd.to_datetime(final['date'])

    for col in ['open', 'high', 'low', 'close']:
        final[col] = pd.to_numeric(final[col], errors='coerce').astype(float)

    final['volume'] = (
        pd.to_numeric(final['volume'], errors='coerce')
        .fillna(0).round(0).astype('int64')
    )
    final['change_rate'] = pd.to_numeric(final['change_rate'], errors='coerce').astype(float)

    if "kospi" in market_name:
        for col in ['for_net', 'inst_net', 'ind_net']:
            final[col] = (
                pd.to_numeric(final[col], errors='coerce')
                .fillna(0).round(0).astype('int64')
            )
    else:
        for col in ['for_net', 'inst_net', 'ind_net']:
            final[col] = (
                pd.to_numeric(final[col], errors='coerce')
                .fillna(0).astype(float)
            )

    return final[['date', 'nm', 'open', 'high', 'low', 'close',
                  'volume', 'change_rate', 'for_net', 'inst_net', 'ind_net']]


# ── 3. 업데이트 메인 로직 ────────────────────────────────────────────────────
def update_process():
    today = datetime.now()

    for i in range(10):
        target_dt      = (today - timedelta(days=i)).strftime('%Y%m%d')
        target_date_db = (today - timedelta(days=i)).strftime('%Y-%m-%d')

        for table in ["kospi_stocks", "kosdaq_stocks"]:
            try:
                query        = text(f"SELECT count(*) as cnt FROM {table} WHERE date LIKE :dt")
                existing_cnt = pd.read_sql(
                    query, engine, params={"dt": f"{target_date_db}%"}
                ).iloc[0, 0]

                if existing_cnt > 500:
                    print(f"✅ [{target_date_db}] {table}: 스킵")
                    continue

                print(f"🔄 [{target_date_db}] {table}: 데이터 수집 및 전송 중...")
                df = get_market_data(target_dt, table)

                if not df.empty:
                    df.to_sql(table, engine, if_exists='append', index=False)
                    print(f"🚀 [{target_date_db}] {table}: 업데이트 성공! ({len(df)}건)")
                    time.sleep(1)
                else:
                    print(f"📅 [{target_date_db}] {table}: 장이 열리지 않은 날입니다.")

            except Exception as e:
                print(f"❌ [{target_date_db}] {table} 오류: {e}")


if __name__ == "__main__":
    update_process()
