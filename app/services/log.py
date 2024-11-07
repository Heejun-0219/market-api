from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import numpy as np
from app.core.config import settings
from typing import Optional, List
import os

class DataLogService:
    def __init__(self, daily_dir: Path):
        self.daily_dir = daily_dir

    async def get_data_logs(self, 
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           last_n_days: Optional[int] = None) -> dict:
        """데이터 로그 조회"""
        try:
            # 파일 경로
            exchange_rates_file = self.daily_dir / "exchange_rates.csv"
            market_indices_file = self.daily_dir / "market_indices.csv"
            
            # 결과 저장용 딕셔너리
            log_data = {
                'exchange_rates': {},
                'market_indices': {},
                'summary': {
                    'total_records': 0,
                    'date_range': '',
                    'latest_update': '',
                    'currencies_tracked': [],
                    'indices_tracked': []
                }
            }
            
            # 환율 데이터 로그 처리
            if exchange_rates_file.exists():
                rates_df = pd.read_csv(exchange_rates_file)
                rates_df['date'] = pd.to_datetime(rates_df['date'])
                
                # 날짜 필터링
                rates_df = self._filter_data_by_date(rates_df, start_date, end_date, last_n_days)
                
                log_data['exchange_rates'] = {
                    'data': rates_df.to_dict('records'),
                    'statistics': self._calculate_statistics(rates_df, 'exchange_rates')
                }
                log_data['summary']['currencies_tracked'] = [col for col in rates_df.columns if col not in ['date', 'timestamp']]
            
            # 지수 데이터 로그 처리
            if market_indices_file.exists():
                indices_df = pd.read_csv(market_indices_file)
                indices_df['date'] = pd.to_datetime(indices_df['date'])
                
                # 날짜 필터링
                indices_df = self._filter_data_by_date(indices_df, start_date, end_date, last_n_days)
                
                log_data['market_indices'] = {
                    'data': indices_df.to_dict('records'),
                    'statistics': self._calculate_statistics(indices_df, 'market_indices')
                }
                log_data['summary']['indices_tracked'] = [col for col in indices_df.columns if col not in ['date', 'timestamp']]
            
            # 전체 요약 정보 업데이트
            log_data['summary'].update({
                'total_records': len(rates_df) if 'rates_df' in locals() else 0,
                'date_range': f"{rates_df['date'].min().strftime('%Y-%m-%d')} ~ {rates_df['date'].max().strftime('%Y-%m-%d')}" if 'rates_df' in locals() else '',
                'latest_update': rates_df['timestamp'].iloc[0] if 'rates_df' in locals() else ''
            })
            
            return log_data
            
        except Exception as e:
            raise Exception(f"Error fetching data logs: {str(e)}")

    def _filter_data_by_date(self, df: pd.DataFrame, 
                            start_date: Optional[str], 
                            end_date: Optional[str],
                            last_n_days: Optional[int]) -> pd.DataFrame:
        """날짜 기준 데이터 필터링"""
        if last_n_days:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=last_n_days)
            return df[df['date'].between(start_date, end_date)]
        
        if start_date:
            df = df[df['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df['date'] <= pd.to_datetime(end_date)]
        
        return df

    def _calculate_statistics(self, df: pd.DataFrame, data_type: str) -> dict:
        """통계 정보 계산"""
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        stats = {}
        
        for col in numeric_columns:
            if col not in ['date', 'timestamp']:
                stats[col] = {
                    'mean': round(df[col].mean(), 2),
                    'min': round(df[col].min(), 2),
                    'max': round(df[col].max(), 2),
                    'std': round(df[col].std(), 2),
                }
                
                # 변화율 계산
                if len(df) > 1:
                    first_value = df[col].iloc[-1]
                    last_value = df[col].iloc[0]
                    change = round((last_value - first_value) / first_value * 100, 2)
                    stats[col]['change_percent'] = change
        
        return stats
