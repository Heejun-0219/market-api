from pykrx import stock
from pykrx import bond
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
from typing import Dict, Optional

class KoreaMarketService:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 주요 지수 코드
        self.index_codes = {
            'KOSPI': '1001',
            'KOSDAQ': '2001',
            'KOSPI200': '1028'
        }
        
        # 관심 있는 채권 종류
        self.bond_types = {
            'KTB3Y': '국고채3년',
            'KTB5Y': '국고채5년',
            'KTB10Y': '국고채10년',
            'MSB': '통화안정증권',
            'CORP3Y': '회사채3년'
        }

    async def get_daily_market_data(self) -> dict:
        """일별 시장 데이터 수집"""
        try:
            today = datetime.now().strftime("%Y%m%d")
            
            data = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'indices': await self._get_indices_data(today),
                'market_cap': await self._get_market_cap_data(today),
                'trading_volume': await self._get_trading_volume_data(today),
                'interest_rates': await self._get_interest_rates_data(today),
                'foreign_trade': await self._get_foreign_trade_data(today)
            }
            
            # 데이터 저장
            await self._save_daily_data(data)
            
            return data
            
        except Exception as e:
            raise Exception(f"Error collecting daily market data: {e}")

    async def _get_indices_data(self, date: str) -> dict:
        """지수 데이터 수집"""
        indices_data = {}
        for name, code in self.index_codes.items():
            try:
                df = stock.get_index_ohlcv(date, date, code)
                if not df.empty:
                    indices_data[name] = {
                        'close': float(df['종가'].iloc[-1]),
                        'volume': int(df['거래량'].iloc[-1]),
                        'value': float(df['거래대금'].iloc[-1])
                    }
            except Exception as e:
                print(f"Error fetching {name} data: {e}")
        return indices_data

    async def _get_market_cap_data(self, date: str) -> dict:
        """시가총액 데이터 수집"""
        try:
            kospi = stock.get_market_cap(date, market="KOSPI")
            kosdaq = stock.get_market_cap(date, market="KOSDAQ")
            
            return {
                'KOSPI': float(kospi['시가총액'].sum()),
                'KOSDAQ': float(kosdaq['시가총액'].sum())
            }
        except Exception as e:
            print(f"Error fetching market cap data: {e}")
            return {}

    async def _get_trading_volume_data(self, date: str) -> dict:
        """거래량 데이터 수집"""
        try:
            volume_data = {}
            for market in ['KOSPI', 'KOSDAQ']:
                df = stock.get_market_trading_volume_by_date(date, date, market)
                if not df.empty:
                    volume_data[market] = {
                        'institutional': int(df['기관'].iloc[-1]),
                        'foreign': int(df['외국인'].iloc[-1]),
                        'individual': int(df['개인'].iloc[-1])
                    }
            return volume_data
        except Exception as e:
            print(f"Error fetching trading volume data: {e}")
            return {}

    async def _get_interest_rates_data(self, date: str) -> dict:
        """금리 데이터 수집"""
        try:
            rates_data = {}
            for code, name in self.bond_types.items():
                df = bond.get_bond_rate(date, date)
                if not df.empty and name in df.index:
                    rates_data[code] = float(df.loc[name]['수익률'])
            return rates_data
        except Exception as e:
            print(f"Error fetching interest rates data: {e}")
            return {}

    async def _get_foreign_trade_data(self, date: str) -> dict:
        """외국인 거래 동향 수집"""
        try:
            trade_data = {}
            for market in ['KOSPI', 'KOSDAQ']:
                df = stock.get_market_net_purchases_of_equities(date, date, market)
                if not df.empty:
                    trade_data[market] = {
                        'foreign_net': float(df['외국인'].iloc[-1]),
                        'institutional_net': float(df['기관'].iloc[-1])
                    }
            return trade_data
        except Exception as e:
            print(f"Error fetching foreign trade data: {e}")
            return {}

    async def _save_daily_data(self, data: dict):
        """일별 데이터 저장"""
        try:
            current_date = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            
            # 각 카테고리별로 CSV 파일 저장
            categories = ['indices', 'market_cap', 'trading_volume', 'interest_rates', 'foreign_trade']
            
            for category in categories:
                file_path = self.data_dir / f"daily_{category}.csv"
                
                new_data = pd.DataFrame([{
                    'date': current_date,
                    'timestamp': data['timestamp'],
                    **self._flatten_dict(data[category])
                }])
                
                if file_path.exists():
                    existing_data = pd.read_csv(file_path)
                    existing_data['date'] = pd.to_datetime(existing_data['date']).dt.strftime('%Y-%m-%d')
                    
                    if current_date in existing_data['date'].values:
                        existing_data = existing_data[existing_data['date'] != current_date]
                    
                    updated_data = pd.concat([new_data, existing_data], ignore_index=True)
                else:
                    updated_data = new_data
                
                updated_data.to_csv(file_path, index=False)
                
        except Exception as e:
            print(f"Error saving daily data: {e}")

    def _flatten_dict(self, d: dict, parent_key: str = '', sep: str = '_') -> dict:
        """중첩된 딕셔너리를 1차원으로 변환"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _serialize_for_json(self, data: any) -> any:
        """JSON 직렬화를 위한 데이터 변환"""
        if isinstance(data, dict):
            return {k: self._serialize_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._serialize_for_json(item) for item in data]
        elif isinstance(data, (np.int64, np.int32)):
            return int(data)
        elif isinstance(data, (np.float64, np.float32)):
            return float(data)
        elif isinstance(data, datetime):
            return data.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(data, np.ndarray):
            return data.tolist()
        elif pd.isna(data):
            return None
        return data