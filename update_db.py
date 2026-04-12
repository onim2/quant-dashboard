import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError, DataError
from pykrx import stock
from datetime import datetime, timedelta
import time
import requests

# ── 1. DB 접속 정보 & 엔진 생성 ─────────────────────────────────────────────
db_user     = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host     = "34.50.62.220"
db_name     = "stock_db"

engine = create_engine(
    f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4"
)


# ── 2. 거래일 교차검증 ───────────────────────────────────────────────────────
def is_trading_day(target_date_str: str, mkt: str) -> bool | None:
    """
    pykrx 거래일 캘린더로 실제 휴장일 여부를 교차검증.

    Returns
    -------
    True  : 거래일 확인 → 데이터 부재는 API 장애 의심
    False : 휴장일 확인 → 정상적인 빈 응답
    None  : 검증 API 자체도 실패 → 판단 불가
    """
    try:
        trading_days = stock.get_market_trading_days(
            target_date_str, target_date_str, market=mkt
        )
        return len(trading_days) > 0
    except requests.exceptions.ConnectionError:
        print(f"    ⚠️  거래일 검증 실패 - 네트워크 연결 오류")
        return None
    except requests.exceptions.Timeout:
        print(f"    ⚠️  거래일 검증 실패 - API 타임아웃")
        return None
    except Exception as e:
        print(f"    ⚠️  거래일 검증 실패 - 알 수 없는 오류: {e}")
        return None


# ── 3. 시장 데이터 수집 ──────────────────────────────────────────────────────
def get_market_data(target_date_str: str, market_name: str) -> pd.DataFrame:
    """
    target_date_str : '20260408' 형식
    market_name     : 'kospi_stocks' or 'kosdaq_stocks'
    """
    mkt = "KOSPI" if "kospi" in market_name else "KOSDAQ"

    # [시세 수집] ──────────────────────────────────────────────────────────────
    df_price = pd.DataFrame()
    try:
        df_price = stock.get_market_ohlcv_by_date(target_date_str, target_date_str, mkt)
    except requests.exceptions.ConnectionError as e:
        print(f"    ❌ 시세 수집 실패 - 네트워크 연결 오류: {e}")
        return pd.DataFrame()
    except requests.exceptions.Timeout as e:
        print(f"    ❌ 시세 수집 실패 - API 타임아웃: {e}")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as e:
        print(f"    ❌ 시세 수집 실패 - HTTP 오류 (상태코드 확인 필요): {e}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"    ❌ 시세 수집 실패 - 응답 컬럼 구조 변경 감지 (KeyError: {e})")
        return pd.DataFrame()
    except ValueError as e:
        print(f"    ❌ 시세 수집 실패 - 데이터 파싱 오류 (ValueError: {e})")
        return pd.DataFrame()
    except Exception as e:
        print(f"    ❌ 시세 수집 실패 - 알 수 없는 오류: {type(e).__name__}: {e}")
        return pd.DataFrame()

    if df_price.empty:
        return pd.DataFrame()

    # [종목명 수집] ────────────────────────────────────────────────────────────
    ticker_names = {}
    try:
        tickers      = stock.get_market_ticker_list(target_date_str, market=mkt)
        ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
    except requests.exceptions.ConnectionError as e:
        print(f"    ❌ 종목명 수집 실패 - 네트워크 연결 오류: {e}")
        return pd.DataFrame()
    except requests.exceptions.Timeout as e:
        print(f"    ❌ 종목명 수집 실패 - API 타임아웃: {e}")
        return pd.DataFrame()
    except KeyError as e:
        print(f"    ❌ 종목명 수집 실패 - 응답 구조 변경 감지 (KeyError: {e})")
        return pd.DataFrame()
    except Exception as e:
        print(f"    ❌ 종목명 수집 실패 - 알 수 없는 오류: {type(e).__name__}: {e}")
        return pd.DataFrame()

    # [수급 수집] ──────────────────────────────────────────────────────────────
    # 수급 실패는 치명적이지 않으므로 0으로 채우고 계속 진행
    df_investor = pd.DataFrame()
    try:
        df_investor = stock.get_market_net_purchases_of_equities_by_ticker(
            target_date_str, target_date_str, mkt
        )
    except requests.exceptions.ConnectionError as e:
        print(f"    ⚠️  수급 수집 실패 - 네트워크 연결 오류 (0으로 채움): {e}")
    except requests.exceptions.Timeout as e:
        print(f"    ⚠️  수급 수집 실패 - API 타임아웃 (0으로 채움): {e}")
    except requests.exceptions.HTTPError as e:
        print(f"    ⚠️  수급 수집 실패 - HTTP 오류 (0으로 채움): {e}")
    except KeyError as e:
        print(f"    ⚠️  수급 수집 실패 - 응답 구조 변경 감지 (0으로 채움) (KeyError: {e})")
    except Exception as e:
        print(f"    ⚠️  수급 수집 실패 - 알 수 없는 오류 (0으로 채움): {type(e).__name__}: {e}")

    # [데이터 정리] ────────────────────────────────────────────────────────────
    df_price         = df_price.reset_index()
    first_col        = df_price.columns[0]
    df_price.rename(columns={first_col: '티커'}, inplace=True)
    df_price['nm']   = df_price['티커'].map(ticker_names)
    df_price['date'] = pd.to_datetime(target_date_str, format='%Y%m%d').strftime('%Y-%m-%d')
    df_price         = df_price[df_price['nm'].notna()].copy()

    if df_investor.empty:
        df_price['for_net']  = 0
        df_price['inst_net'] = 0
        df_price['ind_net']  = 0
        final = df_price
    else:
        df_investor = df_investor.reset_index()
        inv_first   = df_investor.columns[0]
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

    for col in ['for_net', 'inst_net', 'ind_net']:
        if col not in final.columns:
            final[col] = 0

    # ── DB 스키마 타입 변환 ──────────────────────────────────────────────────
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


# ── 4. 업데이트 메인 로직 ────────────────────────────────────────────────────
def update_process():
    today = datetime.now()

    for i in range(10):
        target_dt      = (today - timedelta(days=i)).strftime('%Y%m%d')
        target_date_db = (today - timedelta(days=i)).strftime('%Y-%m-%d')

        for table in ["kospi_stocks", "kosdaq_stocks"]:
            mkt = "KOSPI" if "kospi" in table else "KOSDAQ"
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
                    # ── 교차검증: 빈 데이터가 휴장일 때문인지 API 장애 때문인지 판별
                    verdict = is_trading_day(target_dt, mkt)
                    if verdict is False:
                        print(f"📅 [{target_date_db}] {table}: 휴장일입니다.")
                    elif verdict is True:
                        print(f"🚨 [{target_date_db}] {table}: 거래일임에도 데이터 없음 → API 장애 의심. 수동 확인 필요.")
                    else:
                        print(f"⚠️  [{target_date_db}] {table}: 데이터 없음 (휴장일/API 장애 여부 판단 불가).")

            except OperationalError as e:
                print(f"❌ [{target_date_db}] {table} DB 연결 오류 (OperationalError): {e}")
            except ProgrammingError as e:
                print(f"❌ [{target_date_db}] {table} SQL 오류 (ProgrammingError): {e}")
            except DataError as e:
                print(f"❌ [{target_date_db}] {table} 데이터 타입 불일치 (DataError): {e}")
            except Exception as e:
                print(f"❌ [{target_date_db}] {table} 알 수 없는 오류: {type(e).__name__}: {e}")


if __name__ == "__main__":
    update_process()
