import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, Any

class DataStorageService:
    @staticmethod
    def _handle_nan_values(data: Any) -> Any:
        """NaN 값을 0으로 변환하는 헬퍼 함수"""
        if isinstance(data, dict):
            return {k: DataStorageService._handle_nan_values(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [DataStorageService._handle_nan_values(item) for item in data]
        elif isinstance(data, (pd.Series, pd.DataFrame)):
            return data.fillna(0)
        elif pd.isna(data) or (isinstance(data, float) and np.isnan(data)):
            return 0
        return data

    @staticmethod
    def save_to_csv(df: pd.DataFrame, file_path: Path, index: bool = False) -> None:
        """DataFrame을 CSV로 저장하면서 NaN 값을 0으로 변환"""
        # NaN 값을 0으로 변환
        df_processed = df.fillna(0)
        
        # 숫자형 컬럼에 대해 반올림 처리
        numeric_columns = df_processed.select_dtypes(include=[np.number]).columns
        df_processed[numeric_columns] = df_processed[numeric_columns].round(2)
        
        # CSV로 저장
        df_processed.to_csv(file_path, index=index)

    @staticmethod
    def save_to_json(data: Dict, file_path: Path, ensure_ascii: bool = False) -> None:
        """딕셔너리를 JSON으로 저장하면서 NaN 값을 0으로 변환"""
        # NaN 값을 0으로 변환
        processed_data = DataStorageService._handle_nan_values(data)
        
        # JSON으로 저장
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=ensure_ascii, indent=2, default=str)

    @staticmethod
    def update_csv_file(new_data: dict, file_path: Path, date_column: str = 'date') -> None:
        """기존 CSV 파일을 업데이트하면서 NaN 값을 0으로 변환"""
        new_df = pd.DataFrame([new_data])
        new_df = new_df.fillna(0)
        
        if file_path.exists():
            existing_df = pd.read_csv(file_path)
            existing_df = existing_df.fillna(0)
            
            if date_column in new_df.columns:
                # 같은 날짜의 데이터가 있으면 제거
                existing_df = existing_df[existing_df[date_column] != new_df[date_column].iloc[0]]
            
            # 새 데이터를 추가하고 날짜순으로 정렬
            updated_df = pd.concat([new_df, existing_df], ignore_index=True)
            if date_column in updated_df.columns:
                updated_df = updated_df.sort_values(date_column, ascending=False)
        else:
            updated_df = new_df
        
        # 숫자형 컬럼에 대해 반올림 처리
        numeric_columns = updated_df.select_dtypes(include=[np.number]).columns
        updated_df[numeric_columns] = updated_df[numeric_columns].round(2)
        
        # 저장
        updated_df.to_csv(file_path, index=False)
