import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def is_trading_day(date):
    """주말이 아닌 평일인지 확인"""
    return date.weekday() < 5  # 0=월요일, 4=금요일

# 수집할 환율 및 지수의 티커(symbol) 설정
tickers = {
    # 환율
    'USD/KRW': 'KRW=X',
    'EUR/KRW': 'EURKRW=X',
    'JPY/KRW': 'JPYKRW=X',
    
    # 국내 지수
    'KOSPI': '^KS11',
    'KOSDAQ': '^KQ11',
    'KOSPI200': '^KS200',
    
    # 글로벌 지수
    'S&P500': '^GSPC',
    'NASDAQ': '^IXIC',
    'DOW': '^DJI',
    
    # 추가 모니터링 자산
    'BITCOIN': 'BTC-USD',
    'GOLD': 'GC=F',
    'US_30Y_BOND': '^TYX',
    'CRUDE_OIL': 'CL=F',
    'VIX': '^VIX'
}

# 데이터 수집 기간 설정
start_date = '2019-01-01'
end_date = '2024-11-08'

# 데이터를 저장할 데이터프레임 초기화
data = pd.DataFrame()

# 각 티커에 대해 데이터 다운로드 및 병합
for name, ticker in tickers.items():
    print(f"Downloading data for {name}...")
    try:
        # 데이터 다운로드
        df = yf.download(ticker, start=start_date, end=end_date)
        
        # Close 가격만 선택하고 컬럼명 변경
        df = df[['Close']].rename(columns={'Close': name})
        
        if data.empty:
            data = df
        else:
            data = data.join(df, how='outer')
            
    except Exception as e:
        print(f"Error downloading {name}: {e}")
        continue

# 인덱스를 리셋하여 'Date' 컬럼 생성
data.reset_index(inplace=True)

# 평일 데이터만 필터링
data['is_weekday'] = data['Date'].apply(is_trading_day)
data = data[data['is_weekday']].drop('is_weekday', axis=1)

# 'Timestamp' 컬럼 생성 (시간은 15:30:00으로 설정 - 한국 장 마감 시간)
data['Timestamp'] = pd.to_datetime(data['Date'].astype(str) + ' 15:30:00').dt.strftime('%Y-%m-%d %H:%M:%S')

# 결측값 처리
# 전일 데이터로 채우기 (forward fill)
data = data.fillna(method='ffill')
# 남은 결측값은 이후 데이터로 채우기 (backward fill)
data = data.fillna(method='bfill')

# 데이터 소수점 첫째자리까지 반올림
data = data.round(1)

# 데이터프레임을 날짜 기준 내림차순으로 정렬
data.sort_values('Date', ascending=False, inplace=True)

# 컬럼 순서 재배열
columns = ['Date', 'Timestamp'] + [col for col in data.columns if col not in ['Date', 'Timestamp']]
data = data[columns]

# CSV 파일로 저장
data.to_csv('data/selected/selected_indicators.csv', index=False)

print("\n데이터 수집 및 저장이 완료되었습니다.")
print(f"수집된 데이터 기간: {data['Date'].min()} ~ {data['Date'].max()}")
print(f"총 데이터 수: {len(data):,}행")