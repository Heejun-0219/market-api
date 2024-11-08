import pandas as pd
import datetime
from pathlib import Path
import yfinance as yf

class SelectedIndicatorsService:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 모니터링할 지표들 정의
        self.indicators = {
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
        
        self.file_path = self.data_dir / 'selected_indicators.csv'

    async def collect_current_data(self) -> dict:
        """현재 지표 데이터 수집"""
        results = {}
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        results['timestamp'] = timestamp
        for name, symbol in self.indicators.items():
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(period='1d')
                if not data.empty:
                    results[name] = round(float(data['Close'].iloc[-1]), 1)
                else:
                    results[name] = None
            except Exception as e:
                print(f"Error collecting {name}: {e}")
                results[name] = None
                
        return results

    async def save_data(self, data: dict):
        """데이터를 CSV 파일에 저장 - 같은 날짜 데이터 업데이트"""
        try:
            new_df = pd.DataFrame([data])
            
            if 'date' not in new_df.columns:
                new_df['date'] = pd.to_datetime(new_df['timestamp']).dt.strftime('%Y-%m-%d')
            
            today = new_df['date'].iloc[0]
            numeric_columns = [col for col in new_df.columns if col not in ['date', 'timestamp']]
            for col in numeric_columns:
                new_df[col] = pd.to_numeric(new_df[col], errors='coerce').round(1)
            
            if self.file_path.exists():
                existing_df = pd.read_csv(self.file_path)
                
                if 'date' not in existing_df.columns:
                    existing_df['date'] = pd.to_datetime(existing_df['timestamp']).dt.strftime('%Y-%m-%d')
                
                for col in numeric_columns:
                    existing_df[col] = pd.to_numeric(existing_df[col], errors='coerce').round(1)
                
                if today in existing_df['date'].values:
                    print(f"Updating data for {today}...")
                    existing_df = existing_df[existing_df['date'] != today]
                
                updated_df = pd.concat([new_df, existing_df], ignore_index=True)
            else:
                updated_df = new_df
            
            columns = ['date', 'timestamp'] + [col for col in updated_df.columns if col not in ['date', 'timestamp']]
            updated_df = updated_df[columns]
            
            updated_df = updated_df.sort_values('date', ascending=False)
            
            updated_df.to_csv(self.file_path, index=False)
            print(f"Data saved successfully at {datetime.now()}")
            
            print(f"Total records: {len(updated_df)}")
            print(f"Date range: {updated_df['date'].min()} ~ {updated_df['date'].max()}")
            
        except Exception as e:
            error_msg = f"Error saving data: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
        
    async def get_historical_data(self, days: int = 7) -> dict:
        """저장된 과거 데이터 조회"""
        try:
            if not self.file_path.exists():
                return {
                    "status": "error",
                    "message": "No historical data found"
                }
            
            df = pd.read_csv(self.file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp', ascending=False)
            
            if days:
                df = df.head(days)
            
            return {
                "status": "success",
                "data": df.to_dict('records')
            }
            
        except Exception as e:
            error_msg = f"Error retrieving historical data: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    async def calculate_changes(self, days: int = 1) -> dict:
        """기간별 변화율 계산"""
        try:
            if not self.file_path.exists():
                return {
                    "status": "error",
                    "message": "No data found for comparison"
                }
            
            # 데이터 로드
            df = pd.read_csv(self.file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 숫자형 컬럼 변환
            numeric_columns = [col for col in df.columns if col not in ['date', 'timestamp']]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df = df.sort_values(by='timestamp', ascending=False)
            
            # 현재값과 이전값 비교
            current_data = df.iloc[0]
            previous_data = df.iloc[days] if len(df) > days else None
            
            if previous_data is None:
                return {
                    "status": "error",
                    "message": f"Not enough data for {days} day comparison"
                }
            
            changes = {}
            for column in numeric_columns:  # timestamp 제외한 숫자형 컬럼만 처리
                current_value = current_data[column]
                previous_value = previous_data[column]
                
                if pd.notna(current_value) and pd.notna(previous_value):
                    if previous_value != 0:
                        change_pct = ((current_value - previous_value) / abs(previous_value)) * 100
                        changes[column] = {
                            'current': round(float(current_value), 1),  # float로 변환 후 반올림
                            'previous': round(float(previous_value), 1),
                            'change_pct': round(change_pct, 1),
                            'direction': 'UP' if change_pct > 0 else 'DOWN' if change_pct < 0 else 'UNCHANGED'
                        }
            
            return {
                "status": "success",
                "comparison_period": f"{days} day(s)",
                "current_time": current_data['timestamp'],
                "previous_time": previous_data['timestamp'],
                "changes": changes
            }
            
        except Exception as e:
            error_msg = f"Error calculating changes: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    async def update_data(self) -> dict:
        """새로운 데이터 수집 및 저장"""
        try:
            current_data = await self.collect_current_data()
            
            await self.save_data(current_data)
            
            changes = await self.calculate_changes()
            return {
                "status": "success",
                "current_data": current_data,
                "changes": changes
            }
            
        except Exception as e:
            error_msg = f"Error updating data: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
        
    def _get_vix_level(vix_value: float) -> str:
        """VIX 수준 평가"""
        if vix_value is None:
            return "UNKNOWN"
        if vix_value < 15:
            return "LOW"
        elif vix_value < 25:
            return "MODERATE"
        elif vix_value < 35:
            return "HIGH"
        else:
            return "VERY HIGH"
        
    async def get_period_data(self, start_date: str = None, end_date: str = None, last_n_days: int = None) -> dict:
        """기간별 데이터 조회
        
        Args:
            start_date (str): 시작일 (YYYY-MM-DD)
            end_date (str): 종료일 (YYYY-MM-DD)
            last_n_days (int): 최근 N일 데이터
            
        Returns:
            dict: 기간별 데이터 및 통계
        """
        try:
            if not self.file_path.exists():
                return {
                    "status": "error",
                    "message": "No data available"
                }
                
            # 데이터 로드
            df = pd.read_csv(self.file_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp', ascending=False)
            
            # 기간 필터링
            if last_n_days:
                filtered_df = df.head(last_n_days)
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                if start_date:
                    df = df[df['timestamp'] >= pd.to_datetime(start_date)]
                if end_date:
                    df = df[df['timestamp'] <= pd.to_datetime(end_date)]
                filtered_df = df
                
            if filtered_df.empty:
                return {
                    "status": "error",
                    "message": "No data found for the specified period",
                }
                
            return {
                "status": "success",
                "period": {
                    "start": filtered_df['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S'),
                    "end": filtered_df['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S'),
                    "days": len(filtered_df['timestamp'].dt.date.unique())
                },
                "data": filtered_df.to_dict('records'),
            }
                
        except Exception as e:
            error_msg = f"Error retrieving period data: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)