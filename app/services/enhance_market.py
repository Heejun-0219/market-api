import yfinance as yf
import pandas as pd
from datetime import datetime
from pathlib import Path
import numpy as np
from app.services.korea_market import KoreaMarketService
from app.services.data_storage import DataStorageService

class EnhancedMarketService:
    def __init__(self, current_dir: Path, daily_dir: Path, korea_market_service: KoreaMarketService):
        self.current_dir = current_dir
        self.daily_dir = daily_dir
        self.korea_market_service = korea_market_service
        
        self.current_dir.mkdir(parents=True, exist_ok=True)
        self.daily_dir.mkdir(parents=True, exist_ok=True)
        
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
        
        # 글로벌 주요 지수
        self.global_indices = {
            'S&P500': '^GSPC',
            'NASDAQ': '^IXIC',
            'DOW': '^DJI',
            'NIKKEI': '^N225',
            'SHANGHAI': '000001.SS',
            'HANG_SENG': '^HSI',
            'FTSE': '^FTSE',
            'DAX': '^GDAXI'
        }
        
        # 주요 원자재
        self.commodities = {
            'GOLD': 'GC=F',
            'SILVER': 'SI=F',
            'CRUDE_OIL': 'CL=F',
            'NATURAL_GAS': 'NG=F',
            'COPPER': 'HG=F'
        }
        
        # 채권 수익률
        self.bonds = {
            'US_10Y': '^TNX',
            'US_30Y': '^TYX',
            'US_2Y': '^IRX'
        }
        
        # 변동성 지수
        self.volatility = {
            'VIX': '^VIX',
        }
        
        # 주요 암호화폐
        self.crypto = {
            'BTC/USD': 'BTC-USD',
            'ETH/USD': 'ETH-USD'
        }

    async def _collect_data(self, symbols: dict) -> dict:
        """데이터 수집 헬퍼 함수"""
        results = {}
        for name, symbol in symbols.items():
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(period='1d')
                if not data.empty:
                    results[name] = round(data['Close'].iloc[-1], 2)
            except Exception as e:
                print(f"Error collecting {name}: {e}")
                results[name] = None
        return results

    async def _calculate_derived_indicators(self, data: dict) -> dict:
        """파생 지표 계산"""
        try:
            derived = {}
            
            # 1. 글로벌 시장 동조화 지수
            if data['local_indices'].get('KOSPI') and data['global_indices'].get('S&P500'):
                kospi = data['local_indices']['KOSPI']
                snp = data['global_indices']['S&P500']
                derived['global_correlation'] = self._calculate_correlation(kospi, snp)
            
            # 2. 금-달러 비율
            if data['commodities'].get('GOLD') and data['exchange_rates'].get('USD/KRW'):
                gold = data['commodities']['GOLD']
                usd = data['exchange_rates']['USD/KRW']
                derived['gold_dollar_ratio'] = round(gold / usd, 4)
            
            # 3. 리스크 지표
            if data['volatility'].get('VIX'):
                vix = data['volatility']['VIX']
                derived['risk_level'] = self._calculate_risk_level(vix)
            
            # 4. 원자재 종합 지수
            commodity_prices = [v for v in data['commodities'].values() if v is not None]
            if commodity_prices:
                derived['commodity_index'] = round(sum(commodity_prices) / len(commodity_prices), 2)
            
            return derived
            
        except Exception as e:
            print(f"Error calculating derived indicators: {e}")
            return {}

    def _calculate_correlation(self, x: float, y: float) -> float:
        """상관관계 계산 (간단한 버전)"""
        try:
            return round((x * y) / (abs(x) * abs(y)), 4)
        except:
            return 0

    def _calculate_risk_level(self, vix: float) -> str:
        """VIX 기반 리스크 수준 평가"""
        if vix < 15:
            return "LOW"
        elif vix < 25:
            return "MODERATE"
        elif vix < 35:

            return "HIGH"
        else:
            return "VERY HIGH"

    async def get_enhanced_historical_data(self, days: int = 7) -> dict:
        """과거 데이터 조회"""
        try:
            basic_data = await self._load_historical_data('basic_indicators.csv', days)
            global_data = await self._load_historical_data('global_indicators.csv', days)
            
            return {
                'basic_indicators': basic_data,
                'global_indicators': global_data
            }
            
        except Exception as e:
            raise Exception(f"Error fetching enhanced historical data: {e}")

    async def _load_historical_data(self, filename: str, days: int) -> dict:
        """CSV 파일에서 과거 데이터 로드"""
        try:
            file_path = self.daily_dir / filename
            if file_path.exists():
                data = pd.read_csv(file_path)
                data['date'] = pd.to_datetime(data['date'])
                data = data.sort_values(by='date', ascending=False).head(days)
                return data.to_dict('records')
            else:
                return []
        except Exception as e:
            print(f"Error loading historical data: {e}")
            return []

    def _analyze_market_status(self, changes: dict) -> dict:
        """시장 전반적인 상태 분석"""
        try:
            status = {
                'market_direction': {
                    'local': self._get_market_direction(changes['local_indices']),
                    'global': self._get_market_direction(changes['global_indices']),
                    'currency': self._get_market_direction(changes['exchange_rates']),
                    'commodity': self._get_market_direction(changes['commodities'])
                },
                'risk_indicators': {
                    'volatility_change': self._analyze_volatility(changes['volatility']),
                    'bond_market': self._analyze_bonds(changes['bonds'])
                },
                'significant_changes': self._get_significant_changes(changes)
            }
            
            return status
        except Exception as e:
            print(f"Error analyzing market status: {e}")
            return {}

    def _get_market_direction(self, category_changes: dict) -> str:
        """시장 방향성 판단"""
        up_count = sum(1 for item in category_changes.values() 
                      if item and item.get('direction') == 'UP')
        down_count = sum(1 for item in category_changes.values() 
                        if item and item.get('direction') == 'DOWN')
        
        total = len(category_changes)
        if total == 0:
            return 'UNDEFINED'
        
        up_ratio = up_count / total
        down_ratio = down_count / total
        
        if up_ratio > 0.6:
            return 'STRONGLY_UP'
        elif up_ratio > 0.5:
            return 'SLIGHTLY_UP'
        elif down_ratio > 0.6:
            return 'STRONGLY_DOWN'
        elif down_ratio > 0.5:
            return 'SLIGHTLY_DOWN'
        else:
            return 'MIXED'

    def _analyze_volatility(self, volatility_changes: dict) -> dict:
        """변동성 지표 분석"""
        vix_change = volatility_changes.get('VIX', {})
        return {
            'level': self._calculate_risk_level(vix_change.get('current')) if vix_change else 'UNKNOWN',
            'trend': vix_change.get('direction', 'UNKNOWN') if vix_change else 'UNKNOWN'
        }

    def _analyze_bonds(self, bond_changes: dict) -> str:
        """채권 시장 분석"""
        if not bond_changes:
            return 'UNKNOWN'
            
        up_count = sum(1 for change in bond_changes.values() 
                      if change and change.get('direction') == 'UP')
        down_count = sum(1 for change in bond_changes.values() 
                        if change and change.get('direction') == 'DOWN')
        
        if up_count > down_count:
            return 'YIELD_RISING'
        elif down_count > up_count:
            return 'YIELD_FALLING'
        else:
            return 'STABLE'

    def _get_significant_changes(self, changes: dict, threshold: float = 2.0) -> list:
        """주요 변화 감지 (임계값 이상의 변화)"""
        significant = []
        for category, items in changes.items():
            if isinstance(items, dict):
                for item, data in items.items():
                    if isinstance(data, dict) and 'change_pct' in data:
                        if abs(data['change_pct']) >= threshold:
                            significant.append({
                                'category': category,
                                'item': item,
                                'change_pct': data['change_pct'],
                                'direction': data['direction']
                            })
        
        return sorted(significant, key=lambda x: abs(x['change_pct']), reverse=True)

    async def save_enhanced_data(self, data: dict):
        """확장된 데이터 저장 - NaN 처리 추가"""
        try:
            storage_service = DataStorageService()
            
            # timestamp 처리
            timestamp = data['timestamp']
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except ValueError:
                        timestamp = datetime.now()
            elif not isinstance(timestamp, datetime):
                timestamp = datetime.now()
                
            current_date = timestamp.strftime('%Y-%m-%d')
            current_time = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            # 기본 지표 데이터
            basic_file = self.daily_dir / 'basic_indicators.csv'
            basic_data = {
                'date': current_date,
                'timestamp': current_time,
                **data['exchange_rates'],
                **data['local_indices']
            }
            storage_service.update_csv_file(basic_data, basic_file)
            
            # 글로벌 지표 데이터
            global_file = self.daily_dir / 'global_indicators.csv'
            global_data = {
                'date': current_date,
                'timestamp': current_time,
                **data['global_indices'],
                **data['commodities'],
                **data['bonds'],
                **data['volatility'],
                **data['crypto'],
                **{f"derived_{k}": v for k, v in data['derived_indicators'].items()}
            }
            storage_service.update_csv_file(global_data, global_file)
            
            print(f"Successfully saved enhanced data for {current_date}")
            
        except Exception as e:
            error_msg = f"Error saving enhanced data: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    async def get_enhanced_market_data(self) -> dict:
        """확장된 시장 데이터 수집 - timestamp 처리 수정"""
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            data = {
                'timestamp': current_time,
                'exchange_rates': {},
                'local_indices': {},
                'global_indices': {},
                'commodities': {},
                'bonds': {},
                'volatility': {},
                'crypto': {}
            }
            
            data['exchange_rates'] = await self._collect_data(self.currency_pairs)
            data['local_indices'] = await self._collect_data(self.indices)
            data['global_indices'] = await self._collect_data(self.global_indices)
            data['commodities'] = await self._collect_data(self.commodities)
            data['bonds'] = await self._collect_data(self.bonds)
            data['volatility'] = await self._collect_data(self.volatility)
            data['crypto'] = await self._collect_data(self.crypto)
            
            data['derived_indicators'] = await self._calculate_derived_indicators(data)
            
            return data
                
        except Exception as e:
            raise Exception(f"Error collecting enhanced market data: {e}")
        
    async def _load_yesterday_data(self, filename: str) -> dict:
        """전일 데이터 로드 - 파일 읽기 방식 개선"""
        try:
            file_path = self.daily_dir / filename
            if file_path.exists():
                df = pd.read_csv(file_path)
                if not df.empty:
                    # derived_ 접두사 처리
                    yesterday_data = {}
                    for column in df.columns:
                        if column not in ['date', 'timestamp']:
                            if column.startswith('derived_'):
                                key = column.replace('derived_', '')
                                yesterday_data[key] = df.iloc[0][column]
                            else:
                                yesterday_data[column] = df.iloc[0][column]
                    return yesterday_data
            return {}
        except Exception as e:
            print(f"Error loading yesterday's data from {filename}: {e}")
            return {}

    async def _load_latest_data(self) -> tuple:
        """가장 최신 데이터 로드"""
        try:
            # 기본 지표 데이터 로드
            basic_file = self.daily_dir / 'basic_indicators.csv'
            basic_data = {}
            if basic_file.exists():
                basic_df = pd.read_csv(basic_file)
                if not basic_df.empty:
                    latest_basic = basic_df.iloc[1].to_dict()
                    basic_data = {k: v for k, v in latest_basic.items() 
                                if k not in ['date', 'timestamp']}

            # 글로벌 지표 데이터 로드
            global_file = self.daily_dir / 'global_indicators.csv'
            global_data = {}
            if global_file.exists():
                global_df = pd.read_csv(global_file)
                if not global_df.empty:
                    latest_global = global_df.iloc[1].to_dict()
                    for k, v in latest_global.items():
                        if k not in ['date', 'timestamp']:
                            if k.startswith('derived_'):
                                key = k.replace('derived_', '')
                                global_data[key] = v
                            else:
                                global_data[k] = v

            return basic_data, global_data

        except Exception as e:
            print(f"Error loading latest data: {e}")
            return {}, {}

    async def get_market_changes(self) -> dict:
        """시장 변화 데이터 조회 및 분석"""
        try:
            # 현재 데이터 수집
            current_data = await self.get_enhanced_market_data()
            await self.save_enhanced_data(current_data)
            
            # 최신 저장 데이터 로드
            latest_basic, latest_global = await self._load_latest_data()
            
            # 변화율 계산
            changes = {
                'timestamp': current_data['timestamp'],
                'exchange_rates': self._calculate_changes(
                    current_data['exchange_rates'],
                    {k: v for k, v in latest_basic.items() 
                     if k in self.currency_pairs.keys()},
                    is_percentage=True
                ),
                'local_indices': self._calculate_changes(
                    current_data['local_indices'],
                    {k: v for k, v in latest_basic.items() 
                     if k in self.indices.keys()},
                    is_percentage=True
                ),
                'global_indices': self._calculate_changes(
                    current_data['global_indices'],
                    {k: v for k, v in latest_global.items() 
                     if k in self.global_indices.keys()},
                    is_percentage=True
                ),
                'commodities': self._calculate_changes(
                    current_data['commodities'],
                    {k: v for k, v in latest_global.items() 
                     if k in self.commodities.keys()},
                    is_percentage=True
                ),
                'bonds': self._calculate_changes(
                    current_data['bonds'],
                    {k: v for k, v in latest_global.items() 
                     if k in self.bonds.keys()},
                    is_percentage=False
                ),
                'volatility': self._calculate_changes(
                    current_data['volatility'],
                    {k: v for k, v in latest_global.items() 
                     if k in self.volatility.keys()},
                    is_percentage=True
                ),
                'crypto': self._calculate_changes(
                    current_data['crypto'],
                    {k: v for k, v in latest_global.items() 
                     if k in self.crypto.keys()},
                    is_percentage=True
                ),
                'derived_indicators': self._calculate_changes(
                    current_data['derived_indicators'],
                    {k: v for k, v in latest_global.items() 
                     if k in current_data['derived_indicators'].keys()},
                    is_percentage=True
                )
            }
            
            # 시장 상태 분석
            market_status = self._analyze_market_status(changes)

            # 비교 정보 생성
            comparison_info = {
                'current_time': current_data['timestamp'],
                'comparison_time': latest_basic.get('timestamp', 'N/A') if latest_basic else 'N/A',
            }
            
            result = {
                'current_data': current_data,
                'latest_saved_data': {
                    'basic_indicators': latest_basic,
                    'global_indicators': latest_global
                },
                'changes': changes,
                'market_status': market_status,
                'comparison_info': comparison_info
            }
            
            return self._serialize_for_json(result)

        except Exception as e:
            error_msg = f"Error calculating market changes: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

    def _calculate_changes(self, current: dict, previous: dict, is_percentage: bool = True) -> dict:
        """변화율 계산 로직 - None 값 처리 개선"""
        changes = {}
        for key, current_value in current.items():
            try:
                if current_value is not None and key in previous:
                    previous_value = previous.get(key)
                    if previous_value is not None:
                        if isinstance(current_value, (int, float)) and isinstance(previous_value, (int, float)):
                            if is_percentage and previous_value != 0:
                                change_pct = ((current_value - previous_value) / abs(previous_value)) * 100
                                changes[key] = {
                                    'current': round(current_value, 2),
                                    'previous': round(previous_value, 2),
                                    'change_pct': round(change_pct, 2),
                                    'change_value': round(current_value - previous_value, 2),
                                    'direction': 'UP' if change_pct > 0 else 'DOWN' if change_pct < 0 else 'UNCHANGED'
                                }
                            else:
                                change_value = current_value - previous_value
                                changes[key] = {
                                    'current': round(current_value, 2),
                                    'previous': round(previous_value, 2),
                                    'change_value': round(change_value, 2),
                                    'direction': 'UP' if change_value > 0 else 'DOWN' if change_value < 0 else 'UNCHANGED'
                                }
                        elif isinstance(current_value, str):
                            changes[key] = {
                                'current': current_value,
                                'previous': previous_value,
                                'change': 'CHANGED' if current_value != previous_value else 'UNCHANGED'
                            }
                else:
                    changes[key] = {
                        'current': current_value,
                        'previous': previous.get(key, None),
                        'status': 'NO_COMPARISON_DATA'
                    }
            except Exception as e:
                print(f"Error calculating change for {key}: {e}")
                changes[key] = {
                    'current': current_value,
                    'previous': previous.get(key, None),
                    'status': 'CALCULATION_ERROR',
                    'error': str(e)
                }

        return changes

    def _serialize_for_json(self, data: dict) -> dict:
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
        return data
