import pandas as pd
from datetime import datetime
from pathlib import Path
import yfinance as yf
from app.services.data_storage import DataStorageService
import numpy as np

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
        """데이터를 CSV 파일에 저장 - NaN 처리 추가"""
        try:
            storage_service = DataStorageService()
            
            if 'date' not in data:
                data['date'] = pd.to_datetime(data['timestamp']).strftime('%Y-%m-%d')
            
            storage_service.update_csv_file(data, self.file_path)
            
            print(f"Data saved successfully at {datetime.now()}")
            
            # 저장된 데이터 확인을 위한 요약 정보
            if self.file_path.exists():
                df = pd.read_csv(self.file_path)
                print(f"Total records: {len(df)}")
                print(f"Date range: {df['date'].min()} ~ {df['date'].max()}")
            
        except Exception as e:
            error_msg = f"Error saving data: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)
        
    async def get_historical_data(self, days: int = 7) -> dict:
        """저장된 과거 데이터 조회 - NaN 처리 추가"""
        try:
            if not self.file_path.exists():
                return {
                    "status": "error",
                    "message": "No historical data found"
                }
            
            df = pd.read_csv(self.file_path)
            df = df.fillna(0)  # NaN 값을 0으로 변환
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp', ascending=False)
            
            if days:
                df = df.head(days)
            
            # 숫자형 컬럼 반올림
            numeric_columns = df.select_dtypes(include=[np.number]).columns
            df[numeric_columns] = df[numeric_columns].round(2)
            
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
            for column in numeric_columns:
                current_value = current_data[column]
                previous_value = previous_data[column]
                
                if pd.notna(current_value) and pd.notna(previous_value):
                    if previous_value != 0:
                        change_pct = ((current_value - previous_value) / abs(previous_value)) * 100
                        changes[column] = {
                            'current': round(float(current_value), 1),
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
        """기간별 데이터 조회 - NaN 처리 추가"""
        try:
            if not self.file_path.exists():
                return {
                    "status": "error",
                    "message": "No data available"
                }
                
            df = pd.read_csv(self.file_path)
            df = df.fillna(0)  # NaN 값을 0으로 변환
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp', ascending=False)
            
            if last_n_days:
                filtered_df = df.head(last_n_days)
            else:
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
            
            # 숫자형 컬럼 반올림
            numeric_columns = filtered_df.select_dtypes(include=[np.number]).columns
            filtered_df[numeric_columns] = filtered_df[numeric_columns].round(2)
                
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