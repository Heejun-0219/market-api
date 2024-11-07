import wbdata
import pandas as pd
from datetime import datetime
from pathlib import Path
import numpy as np
from typing import Dict, List, Optional

class WorldBankService:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 한국 country code
        self.country_code = 'KOR'
        
        # 지표 정의
        self.indicators = {
            'GDP': 'NY.GDP.MKTP.CD',                    # GDP (current US$)
            'GDP_PER_CAPITA': 'NY.GDP.PCAP.CD',         # GDP per capita (current US$)
            'POPULATION': 'SP.POP.TOTL',                # Population, total
            'POPULATION_GROWTH': 'SP.POP.GROW',         # Population growth (annual %)
            'LIFE_EXPECTANCY': 'SP.DYN.LE00.IN',        # Life expectancy at birth
            'EDUCATION_EXPENDITURE': 'SE.XPD.TOTL.GD.ZS', # Government expenditure on education
            'HEALTH_EXPENDITURE': 'SH.XPD.CHEX.GD.ZS',  # Current health expenditure
            'TRADE_BALANCE': 'NE.RSB.GNFS.ZS',          # External balance on goods and services
            'DEBT_TO_GDP': 'GC.DOD.TOTL.GD.ZS',        # Central government debt, total
            'UNEMPLOYMENT': 'SL.UEM.TOTL.ZS'            # Unemployment, total
        }

    async def fetch_and_save_data(self) -> dict:
        """World Bank 데이터 수집 및 저장"""
        try:
            current_year = datetime.now().year
            results = {}
            
            # wbdata의 date 형식에 맞게 날짜 범위 설정
            date_range = (datetime(2000, 1, 1), datetime(current_year, 12, 31))
            
            # 각 지표별 데이터 수집
            for indicator_name, indicator_code in self.indicators.items():
                try:
                    # 데이터 조회
                    data = wbdata.get_data(
                        indicator=indicator_code,
                        country=self.country_code,
                        date=date_range
                    )
                    
                    if data:
                        # 데이터프레임 변환
                        df = pd.DataFrame(data)
                        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y')
                        df = df[['date', 'value']].sort_values('date', ascending=False)
                        
                        # CSV 파일명
                        filename = f"{indicator_name.lower()}.csv"
                        file_path = self.data_dir / filename
                        
                        # 기존 파일이 있는 경우
                        if file_path.exists():
                            existing_df = pd.read_csv(file_path)
                            merged_df = self._merge_data(existing_df, df)
                            merged_df.to_csv(file_path, index=False)
                        else:
                            df.to_csv(file_path, index=False)
                        
                        results[indicator_name] = {
                            'status': 'success',
                            'latest_year': df['date'].iloc[0],
                            'latest_value': float(df['value'].iloc[0]) if pd.notnull(df['value'].iloc[0]) else None,
                            'total_records': len(df)
                        }
                    else:
                        results[indicator_name] = {
                            'status': 'error',
                            'error': 'No data available'
                        }
                    
                except Exception as e:
                    results[indicator_name] = {
                        'status': 'error',
                        'error': str(e)
                    }
            
            return results
            
        except Exception as e:
            raise Exception(f"Error fetching World Bank data: {e}")

    def _merge_data(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
        """기존 데이터와 새 데이터 병합"""
        try:
            # 날짜 형식 통일
            existing_df['date'] = existing_df['date'].astype(str)
            new_df['date'] = new_df['date'].astype(str)
            
            # 중복 제거를 위해 병합
            merged_df = pd.concat([new_df, existing_df]).drop_duplicates(subset=['date'], keep='first')
            
            # 날짜 기준 내림차순 정렬
            return merged_df.sort_values('date', ascending=False).reset_index(drop=True)
            
        except Exception as e:
            print(f"Error merging data: {e}")
            # 에러 발생 시 새 데이터 반환
            return new_df

    async def get_indicator_data(self, indicator_name: str, years: Optional[int] = None) -> dict:
        """저장된 지표 데이터 조회"""
        try:
            if indicator_name not in self.indicators:
                raise ValueError(f"Invalid indicator: {indicator_name}")
            
            file_path = self.data_dir / f"{indicator_name.lower()}.csv"
            
            if not file_path.exists():
                return {
                    'status': 'error',
                    'error': 'Data not found',
                    'indicator': indicator_name
                }
            
            df = pd.read_csv(file_path)
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            if years:
                df = df.head(years)
            
            data_records = [{
                'year': row['date'],
                'value': float(row['value']) if pd.notnull(row['value']) else None
            } for _, row in df.iterrows()]
            
            return {
                'status': 'success',
                'indicator': indicator_name,
                'indicator_code': self.indicators[indicator_name],
                'country': 'Korea, Rep.',
                'data': data_records,
                'metadata': {
                    'total_years': len(df),
                    'latest_year': df['date'].iloc[0],
                    'latest_value': float(df['value'].iloc[0]) if pd.notnull(df['value'].iloc[0]) else None,
                    'oldest_year': df['date'].iloc[-1],
                    'unit': self._get_indicator_unit(indicator_name)
                }
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'indicator': indicator_name
            }

    async def get_all_indicators(self, latest_only: bool = False) -> dict:
        """모든 지표 데이터 조회"""
        try:
            results = {}
            
            for indicator_name in self.indicators.keys():
                data = await self.get_indicator_data(indicator_name)
                
                if latest_only and data['status'] == 'success':
                    # 최신 데이터만 포함
                    latest_record = data['data'][0] if data['data'] else None
                    results[indicator_name] = {
                        'status': 'success',
                        'latest_data': latest_record,
                        'metadata': data['metadata']
                    }
                else:
                    results[indicator_name] = data
            
            return {
                'status': 'success',
                'country': 'Korea, Rep.',
                'indicators': results,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }

    def _get_indicator_unit(self, indicator_name: str) -> str:
        """지표별 단위 반환"""
        units = {
            'GDP': 'current US$',
            'GDP_PER_CAPITA': 'current US$',
            'POPULATION': 'total',
            'POPULATION_GROWTH': 'annual %',
            'LIFE_EXPECTANCY': 'years',
            'EDUCATION_EXPENDITURE': '% of GDP',
            'HEALTH_EXPENDITURE': '% of GDP',
            'TRADE_BALANCE': '% of GDP',
            'DEBT_TO_GDP': '% of GDP',
            'UNEMPLOYMENT': '% of total labor force'
        }
        return units.get(indicator_name, '')
    
    async def get_all_indicators(self, latest_only: bool = False) -> dict:
        """모든 지표 데이터 조회"""
        try:
            results = {}
            
            for indicator_name in self.indicators.keys():
                data = await self.get_indicator_data(indicator_name)
                
                if data['status'] == 'success':
                    if latest_only:
                        # 최신 데이터만 포함
                        latest_data = data['data'][0] if data['data'] else None
                        results[indicator_name] = {
                            'status': 'success',
                            'latest_data': {
                                'year': latest_data['year'] if latest_data else None,
                                'value': float(latest_data['value']) if latest_data and latest_data['value'] is not None else None
                            },
                            'metadata': {
                                'indicator_code': data['indicator_code'],
                                'unit': data['metadata']['unit'],
                                'total_years': data['metadata']['total_years']
                            }
                        }
                    else:
                        # 전체 데이터 포함
                        results[indicator_name] = {
                            'status': 'success',
                            'indicator_code': data['indicator_code'],
                            'data': [{
                                'year': record['year'],
                                'value': float(record['value']) if record['value'] is not None else None
                            } for record in data['data']],
                            'metadata': {
                                'unit': data['metadata']['unit'],
                                'total_years': data['metadata']['total_years'],
                                'latest_year': data['metadata']['latest_year'],
                                'oldest_year': data['metadata']['oldest_year']
                            }
                        }
                else:
                    results[indicator_name] = {
                        'status': 'error',
                        'error': data.get('error', 'Unknown error')
                    }
            
            return {
                'status': 'success',
                'country': 'Korea, Rep.',
                'indicators': results,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }

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

    async def get_indicator_data(self, indicator_name: str, years: Optional[int] = None) -> dict:
        """저장된 지표 데이터 조회"""
        try:
            if indicator_name not in self.indicators:
                raise ValueError(f"Invalid indicator: {indicator_name}")
            
            file_path = self.data_dir / f"{indicator_name.lower()}.csv"
            
            if not file_path.exists():
                return {
                    'status': 'error',
                    'error': 'Data not found',
                    'indicator': indicator_name
                }
            
            df = pd.read_csv(file_path)
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            
            if years:
                df = df.head(years)
            
            data_records = [{
                'year': str(int(float(row['date']))) if pd.notnull(row['date']) else str(row['date']),  # 소수점 제거
                'value': float(row['value']) if pd.notnull(row['value']) else None
            } for _, row in df.iterrows()]
            
            result = {
                'status': 'success',
                'indicator': indicator_name,
                'indicator_code': self.indicators[indicator_name],
                'country': 'Korea, Rep.',
                'data': data_records,
                'metadata': {
                    'total_years': len(df),
                    'latest_year': str(int(float(df['date'].iloc[0]))) if pd.notnull(df['date'].iloc[0]) else str(df['date'].iloc[0]),  # 소수점 제거
                    'latest_value': float(df['value'].iloc[0]) if pd.notnull(df['value'].iloc[0]) else None,
                    'oldest_year': str(int(float(df['date'].iloc[-1]))) if pd.notnull(df['date'].iloc[-1]) else str(df['date'].iloc[-1]),  # 소수점 제거
                    'unit': self._get_indicator_unit(indicator_name)
                }
            }
            
            return result
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'indicator': indicator_name
            }
