import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine

# ==========================================
# [1] 시스템 설정 및 깔끔한 하이엔드 디자인
# ==========================================
st.set_page_config(page_title="Alpha-Vision Pro", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700;900&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; background-color: #F8F9FA; }
    
    /* ✨ 영문 삭제 및 종목명 여백/간격 확보로 겹침 현상 완벽 해결 */
    .stock-title { 
        font-size: 130px; 
        font-weight: 900; 
        background: linear-gradient(135deg, #111111 0%, #343A40 100%); 
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: -2px; /* 글자 간격 여유 */
        line-height: 1.2; 
        margin-top: 20px; 
        margin-bottom: 50px; /* 아래쪽 여유 공간 확보 */
        text-shadow: 2px 4px 10px rgba(0, 0, 0, 0.05);
    }
    
    div[data-testid="stMetric"] { background-color: #FFFFFF; padding: 22px; border-radius: 12px; border: 1px solid #E9ECEF; box-shadow: 0 4px 12px rgba(0,0,0,0.03); }
    .expert-comment { background-color: #FFFFFF; padding: 20px; border-radius: 12px; border-left: 6px solid #007BFF; color: #343A40; line-height: 1.7; box-shadow: 0 4px 12px rgba(0,0,0,0.03); margin-bottom: 30px; }
    
    .performance-table { width: 100%; border-collapse: collapse; margin-top: 20px; text-align: center; }
    .performance-table th { background-color: #F1F3F5; padding: 12px; border-bottom: 2px solid #DEE2E6; font-weight: 700; }
    .performance-table td { padding: 12px; border-bottom: 1px solid #E9ECEF; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_engine():
    return create_engine(
        "mysql+pymysql://yyy:790412@34.50.62.220:3306/stock_db?charset=utf8mb4",
        connect_args={"ssl": {}}
    )

engine = get_engine()

# ==========================================
# [2] 지표 계산 엔진
# ==========================================
def calculate_metrics(df):
    df['daily_ret'] = df['close'].pct_change()
    df['cum_ret'] = (1 + df['daily_ret']).cumprod() - 1
    for window in [5, 10, 20, 50, 60, 120]:
        df[f'MA{window}'] = df['close'].rolling(window).mean()
    
    df['std'] = df['close'].rolling(20).std()
    df['BB_up'], df['BB_dn'] = df['MA20'] + (df['std'] * 2), df['MA20'] - (df['std'] * 2)
    
    df['EMA12'], df['EMA26'] = df['close'].ewm(span=12).mean(), df['close'].ewm(span=26).mean()
    df['MACD'] = df['EMA12'] - df['EMA26']
    df['Signal'] = df['MACD'].ewm(span=9).mean()
    df['MACD_hist'] = df['MACD'] - df['Signal']
    
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + (gain / (loss + 1e-10))))
    
    df['Peak'] = df['close'].cummax()
    df['DD'] = (df['close'] / df['Peak'] - 1) * 100
    df['MDD'] = df['DD'].cummin()
    return df

# ==========================================
# [3] 데이터 로드 및 사이드바
# ==========================================
with st.sidebar:
    st.markdown("### 🔍 Stock Selection")
    market = st.radio("시장 선택", ["kospi_stocks", "kosdaq_stocks"], horizontal=True)
    ticker_list = pd.read_sql(f"SELECT DISTINCT nm FROM {market}", engine)['nm'].tolist()
    ticker = st.selectbox("종목 선택", ticker_list)
    lookback = st.select_slider("조회 기간 (일)", options=[120, 250, 500, 750, 1000], value=500)
    
    st.divider()
    st.markdown("### ⚙️ Backtest Optimizer")
    bt_fast_ma = st.number_input("단기 이평선 (Fast MA)", min_value=1, max_value=60, value=20)
    bt_slow_ma = st.number_input("장기 이평선 (Slow MA)", min_value=20, max_value=200, value=60)
    trade_mode = st.radio("전략 모드", ["Long Only (현금 관망)", "Long/Short (인버스 헷지)"])
    
    st.divider()
    st.markdown("### 🛠️ Chart Indicators")
    show_ma = st.multiselect("이동평균선(MA)", ["MA5", "MA10", "MA20", "MA50", "MA60", "MA120"], default=["MA20", "MA60"])
    show_bb = st.checkbox("볼린저 밴드", value=True)
    show_macd = st.checkbox("MACD", value=True)
    show_rsi = st.checkbox("RSI", value=True)

df = pd.read_sql(f"SELECT * FROM {market} WHERE nm = '{ticker}' ORDER BY date DESC LIMIT {lookback}", engine).sort_values('date')
df = calculate_metrics(df)
last = df.iloc[-1]

# ==========================================
# [4] 메인 화면: 오직 종목명만 주인공으로!
# ==========================================
st.markdown(f'<p class="stock-title">{ticker}</p>', unsafe_allow_html=True)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Current Price", f"{last['close']:,.0f}")
c2.metric("RSI (14D)", f"{last['RSI']:.1f}")
c3.metric("Max Drawdown", f"{last['MDD']:.1f}%")
c4.metric("BB Pos", f"{((last['close']-last['BB_dn'])/(last['BB_up']-last['BB_dn'])*100):.1f}%")
c5.metric("Total Return", f"{(last['cum_ret']*100):.1f}%")

st.divider()

col_radar, col_diag = st.columns([4, 6])
with col_radar:
    categories = ['모멘텀', '추세', '안정성', '방어력(MDD)', '수급', '효율']
    ann_ret = df['daily_ret'].mean() * 252
    ann_vol = df['daily_ret'].std() * np.sqrt(252)
    sharpe = (ann_ret - 0.03) / ann_vol if ann_vol != 0 else 0
    
    values = [last['RSI'], 100+(last['close']/last['MA20']-1)*100, 100+last['DD'], 100+last['MDD'], min(last['volume']/df['volume'].mean()*50, 100), min(max(50+(sharpe*15),0),100)]
    fig_radar = go.Figure(data=go.Scatterpolar(r=values, theta=categories, fill='toself', line_color='#007BFF'))
    fig_radar.update_layout(polar=dict(radialaxis=dict(visible=False, range=[0, 100])), showlegend=False, height=350, margin=dict(t=20, b=20))
    st.plotly_chart(fig_radar, use_container_width=True)

with col_diag:
    st.markdown("### 🎙️ Strategic Diagnosis")
    st.markdown(f"""
    <div class="expert-comment">
        <b>{ticker} 실시간 퀀트 진단:</b><br>
        현재 가격은 20일 이동평균선 대비 {(last['close']/last['MA20']-1)*100:.1f}% 이격되어 있으며, 
        RSI는 {last['RSI']:.1f} 수준으로 {'단기 과열' if last['RSI'] > 70 else '침체 구간' if last['RSI'] < 30 else '중립 국면'}을 나타내고 있습니다.<br><br>
        <b>투자 효율성 평가:</b><br>
        해당 기간 단순 보유 시 연환산 수익률(CAGR)은 {ann_ret*100:.1f}%, 리스크 대비 수익을 나타내는 샤프 지수는 {sharpe:.2f}로 산출되었습니다.
    </div>
    """, unsafe_allow_html=True)

# ==========================================
# [5] 탭 구성 (차트 제목, 간격, 단위, 코멘트 추가)
# ==========================================
tab1, tab2, tab3 = st.tabs(["📉 Technical Analysis", "📊 Performance Review", "⚙️ Backtest Optimizer"])

with tab1:
    # 차트 이름(서브플롯 타이틀) 동적 생성
    subplot_titles = ["📈 주가 및 이동평균선 흐름"]
    if show_macd: subplot_titles.append("📊 MACD (추세 강도 및 전환)")
    if show_rsi: subplot_titles.append("📉 RSI (과매수/과매도 지표)")

    row_count = 1 + (1 if show_macd else 0) + (1 if show_rsi else 0)
    
    # 간격(vertical_spacing)을 0.1로 넓혀 여유를 줌
    fig = make_subplots(rows=row_count, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.1, 
                        subplot_titles=subplot_titles,
                        row_heights=[0.5] + [0.25]*(row_count-1))
    
    # 1. 주가 차트 (Y축: 가격)
    fig.add_trace(go.Candlestick(x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], name="Price"), row=1, col=1)
    
    ma_colors = {'MA5': '#82E0AA', 'MA10': '#F7DC6F', 'MA20': '#F39C12', 'MA50': '#E74C3C', 'MA60': '#3498DB', 'MA120': '#9B59B6'}
    for ma in show_ma:
        fig.add_trace(go.Scatter(x=df['date'], y=df[ma], name=ma, line=dict(color=ma_colors[ma], width=1.5)), row=1, col=1)
    if show_bb:
        fig.add_trace(go.Scatter(x=df['date'], y=df['BB_up'], name="BB Up", line=dict(dash='dash', color='rgba(100,100,100,0.3)')), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['date'], y=df['BB_dn'], name="BB Dn", line=dict(dash='dash', color='rgba(100,100,100,0.3)')), row=1, col=1)
    
    fig.update_yaxes(title_text="가격 (KRW)", row=1, col=1)

    cur_row = 2
    # 2. MACD 차트 (Y축: MACD 수치)
    if show_macd:
        fig.add_trace(go.Scatter(x=df['date'], y=df['MACD'], name="MACD", line=dict(color='#007BFF')), row=cur_row, col=1)
        fig.add_trace(go.Scatter(x=df['date'], y=df['Signal'], name="Signal", line=dict(color='#FF9F43')), row=cur_row, col=1)
        fig.add_trace(go.Bar(x=df['date'], y=df['MACD_hist'], name="Histogram", marker_color='#E9ECEF'), row=cur_row, col=1)
        fig.update_yaxes(title_text="MACD (±)", row=cur_row, col=1)
        cur_row += 1
        
    # 3. RSI 차트 (Y축: RSI 지수)
    if show_rsi:
        fig.add_trace(go.Scatter(x=df['date'], y=df['RSI'], name="RSI", line=dict(color='#FD7E14')), row=cur_row, col=1)
        fig.add_hrect(y0=70, y1=100, fillcolor="red", opacity=0.05, row=cur_row, col=1)
        fig.add_hrect(y0=0, y1=30, fillcolor="blue", opacity=0.05, row=cur_row, col=1)
        fig.update_yaxes(title_text="RSI 지수 (0~100)", range=[0, 100], row=cur_row, col=1)
    
    fig.update_layout(height=400 + (250 * (row_count - 1)), template="plotly_white", xaxis_rangeslider_visible=False)
    st.plotly_chart(fig, use_container_width=True)

    # 차트 하단 기본 해석 코멘트 추가
    st.info("""
    💡 **기술적 분석(Technical Analysis) 차트 해석 가이드**
    * **주가 및 이평선:** 주가가 선택한 주요 이동평균선(MA) 위에 위치하면 단기적 상승 추세, 아래에 위치하면 하락 추세로 판단합니다.
    * **MACD (선택 시):** 파란색 MACD 선이 주황색 Signal 선을 위로 뚫고 올라갈 때(골든크로스) 매수 타이밍으로 고려합니다.
    * **RSI (선택 시):** 수치가 70 이상(빨간 영역)이면 단기 과열로 매도 경계, 30 이하(파란 영역)면 과매도로 반등 기회로 해석합니다.
    """)

with tab2:
    st.subheader("📊 누적 수익률 및 최대 낙폭(MDD) 분석")
    fig_p = make_subplots(specs=[[{"secondary_y": True}]])
    fig_p.add_trace(go.Scatter(x=df['date'], y=df['cum_ret']*100, name="누적 수익률 (%)", line=dict(color='#28A745', width=3)), secondary_y=False)
    fig_p.add_trace(go.Scatter(x=df['date'], y=df['DD'], name="낙폭 (Drawdown %)", fill='tozeroy', line=dict(color='#DC3545')), secondary_y=True)
    
    fig_p.update_yaxes(title_text="누적 수익률 (%)", secondary_y=False)
    fig_p.update_yaxes(title_text="낙폭 (%)", secondary_y=True)
    fig_p.update_layout(template="plotly_white", height=500)
    st.plotly_chart(fig_p, use_container_width=True)

with tab3:
    st.subheader(f"⚙️ {ticker} 알고리즘 전략 백테스트")
    st.markdown(f"**현재 전략:** 단기 {bt_fast_ma}일선 / 장기 {bt_slow_ma}일선 교차 | 모드: {trade_mode}")
    
    bt_df = df.copy()
    bt_df['Fast_MA'] = bt_df['close'].rolling(bt_fast_ma).mean()
    bt_df['Slow_MA'] = bt_df['close'].rolling(bt_slow_ma).mean()
    
    if "Long/Short" in trade_mode:
        bt_df['Signal'] = np.where(bt_df['Fast_MA'] > bt_df['Slow_MA'], 1, -1)
    else:
        bt_df['Signal'] = np.where(bt_df['Fast_MA'] > bt_df['Slow_MA'], 1, 0)
        
    bt_df['Strategy_Ret'] = bt_df['Signal'].shift(1) * bt_df['daily_ret']
    bt_df['Strat_Cum_Ret'] = (1 + bt_df['Strategy_Ret'].fillna(0)).cumprod() - 1
    
    def get_stats(returns):
        total_ret = (1 + returns.fillna(0)).prod() - 1
        ann_ret = returns.mean() * 252
        ann_vol = returns.std() * np.sqrt(252)
        sharpe = (ann_ret - 0.03) / ann_vol if ann_vol != 0 else 0
        cum_rets = (1 + returns.fillna(0)).cumprod()
        rolling_max = cum_rets.cummax()
        mdd = ((cum_rets - rolling_max) / rolling_max).min()
        return [f"{total_ret*100:.2f}%", f"{ann_ret*100:.2f}%", f"{ann_vol*100:.2f}%", f"{sharpe:.2f}", f"{mdd*100:.2f}%"]

    bh_stats = get_stats(bt_df['daily_ret'])
    st_stats = get_stats(bt_df['Strategy_Ret'])

    stats_data = {
        "분석 지표": ["총 누적 수익률", "연환산 수익률 (CAGR)", "연환산 변동성 (Risk)", "샤프 지수 (Sharpe Ratio)", "최대 낙폭 (MDD)"],
        "단순 보유 (Buy & Hold)": bh_stats,
        "알고리즘 전략 (Strategy)": st_stats
    }
    st.table(pd.DataFrame(stats_data).set_index("분석 지표"))

    fig_bt = go.Figure()
    fig_bt.add_trace(go.Scatter(x=bt_df['date'], y=bt_df['cum_ret']*100, name="Buy & Hold (단순 보유)", line=dict(color='#ADB5BD', dash='dash')))
    fig_bt.add_trace(go.Scatter(x=bt_df['date'], y=bt_df['Strat_Cum_Ret']*100, name="Strategy (전략 수익률)", line=dict(color='#007BFF', width=4)))
    
    fig_bt.update_yaxes(title_text="누적 수익률 (%)")
    fig_bt.update_layout(title="전략 성과 vs 단순 보유 벤치마크 비교", template="plotly_white", height=500)
    st.plotly_chart(fig_bt, use_container_width=True)