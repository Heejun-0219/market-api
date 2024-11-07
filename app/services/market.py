import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path
import numpy as np

class MarketService:
    def __init__(self, current_dir: Path, daily_dir: Path):
        self.current_dir = current_dir
        self.daily_dir = daily_dir
        
        self.currency_pairs = {
            'USD/KRW': 'KRW=X',
            'EUR/KRW': 'EURKRW=X',
            'JPY/KRW': 'JPYKRW=X',
            'CNY/KRW': 'CNYKRW=X'
        }
        
        self.indices = {
            'KOSPI': '^KS11',
            'KOSDAQ': '^KQ11',
            'KOSPI200': '^KS200'
        }
        
        # 디렉토리 생성
        self.current_dir.mkdir(parents=True, exist_ok=True)
        self.daily_dir.mkdir(parents=True, exist_ok=True)

    async def get_market_data(self) -> dict:
        """시장 데이터 수집"""
        try:
            exchange_rates = {}
            market_indices = {}
            current_time = datetime.now()
            
            # 환율 데이터 수집
            for pair, symbol in self.currency_pairs.items():
                ticker = yf.Ticker(symbol)
                data = ticker.history(period='1d')
                if not data.empty:
                    exchange_rates[pair] = round(data['Close'].iloc[-1], 2)
            
            # 지수 데이터 수집
            for index_name, symbol in self.indices.items():
                ticker = yf.Ticker(symbol)
                data = ticker.history(period='1d')
                if not data.empty:
                    market_indices[index_name] = round(data['Close'].iloc[-1], 2)
            
            current_data = {
                'exchange_rates': exchange_rates,
                'market_indices': market_indices,
                'timestamp': current_time
            }
            
            # 데이터 저장
            await self._save_current_data(current_data)
            await self._save_daily_data(current_data)
            
            return current_data
            
        except Exception as e:
            raise Exception(f"Error collecting market data: {e}")
    
    async def _save_current_data(self, data: dict):
        """현재 데이터 저장"""
        try:
            timestamp = data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # 환율 데이터 저장
            current_rates = pd.DataFrame([{
                'timestamp': timestamp,
                **data['exchange_rates']
            }])
            current_rates.to_csv(self.current_dir / "current_exchange_rates.csv", index=False)
            
            # 지수 데이터 저장
            current_indices = pd.DataFrame([{
                'timestamp': timestamp,
                **data['market_indices']
            }])
            current_indices.to_csv(self.current_dir / "current_market_indices.csv", index=False)
            
        except Exception as e:
            print(f"Error saving current data: {e}")
    
    async def _save_daily_data(self, data: dict):
        """일별 데이터 저장 - 단일 파일에 모든 기록 유지"""
        try:
            current_date = data['timestamp'].strftime('%Y-%m-%d')
            current_time = data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # 환율 데이터 처리
            exchange_rates_file = self.daily_dir / "exchange_rates.csv"
            new_exchange_rates = pd.DataFrame([{
                'date': current_date,
                'timestamp': current_time,
                **data['exchange_rates']
            }])
            
            if exchange_rates_file.exists():
                existing_rates = pd.read_csv(exchange_rates_file)
                # 오늘 데이터가 있는지 확인
                existing_rates['date'] = pd.to_datetime(existing_rates['date']).dt.strftime('%Y-%m-%d')
                
                if current_date in existing_rates['date'].values:
                    # 오늘 데이터 업데이트
                    existing_rates = existing_rates[existing_rates['date'] != current_date]
                
                # 새 데이터를 위에 추가
                updated_rates = pd.concat([new_exchange_rates, existing_rates], ignore_index=True)
            else:
                updated_rates = new_exchange_rates
            
            updated_rates.to_csv(exchange_rates_file, index=False)
            
            # 지수 데이터 처리
            market_indices_file = self.daily_dir / "market_indices.csv"
            new_market_indices = pd.DataFrame([{
                'date': current_date,
                'timestamp': current_time,
                **data['market_indices']
            }])
            
            if market_indices_file.exists():
                existing_indices = pd.read_csv(market_indices_file)
                # 오늘 데이터가 있는지 확인
                existing_indices['date'] = pd.to_datetime(existing_indices['date']).dt.strftime('%Y-%m-%d')
                
                if current_date in existing_indices['date'].values:
                    # 오늘 데이터 업데이트
                    existing_indices = existing_indices[existing_indices['date'] != current_date]
                
                # 새 데이터를 위에 추가
                updated_indices = pd.concat([new_market_indices, existing_indices], ignore_index=True)
            else:
                updated_indices = new_market_indices
            
            updated_indices.to_csv(market_indices_file, index=False)
            
        except Exception as e:
            print(f"Error saving daily data: {e}")

    async def get_historical_data(self, days: int = 7) -> dict:
        """과거 데이터 조회"""
        try:
            exchange_rates_file = self.daily_dir / "exchange_rates.csv"
            market_indices_file = self.daily_dir / "market_indices.csv"
            
            historical_data = {
                'exchange_rates': None,
                'market_indices': None
            }
            
            if exchange_rates_file.exists():
                rates_df = pd.read_csv(exchange_rates_file)
                historical_data['exchange_rates'] = rates_df.head(days).to_dict('records')
            
            if market_indices_file.exists():
                indices_df = pd.read_csv(market_indices_file)
                historical_data['market_indices'] = indices_df.head(days).to_dict('records')
            
            return historical_data
            
        except Exception as e:
            raise Exception(f"Error fetching historical data: {e}")
