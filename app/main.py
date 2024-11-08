from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.models.market import MarketData
from app.services.market import MarketService
from app.services.log import DataLogService
from app.services.enhance_market import EnhancedMarketService
from app.services.world_bank import WorldBankService
from app.services.korea_market import KoreaMarketService
from app.services.mix_indicator import SelectedIndicatorsService
from typing import Dict, List, Optional
import json
from datetime import datetime

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 서비스 인스턴스 생성
market_service = MarketService(settings.CURRENT_DIR, settings.DAILY_DIR)
world_bank_service = WorldBankService(settings.DATA_DIR / 'worldbank')

korea_market_service = KoreaMarketService(
    data_dir=settings.DATA_DIR / 'korea'
)

enhanced_market_service = EnhancedMarketService(
    settings.CURRENT_DIR,
    settings.DAILY_DIR,
    korea_market_service
)

selected_indicators_service = SelectedIndicatorsService(
    data_dir=settings.DATA_DIR / 'selected'
)

@app.get("/")
async def root():
    return {"message": "Market Data API is running"}

@app.get(f"{settings.API_V1_STR}/current", response_model=MarketData)
async def get_current_market_data():
    """현재 시장 데이터 조회"""
    try:
        return await market_service.get_market_data()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/historical/{{days}}")
async def get_historical_data(days: int = 7):
    """과거 데이터 조회"""
    try:
        return await market_service.get_historical_data(days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/logs")
async def get_data_logs(
    start_date: Optional[str] = Query(None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="종료일 (YYYY-MM-DD)"),
    last_n_days: Optional[int] = Query(None, description="최근 N일간의 데이터")
):
    """저장된 데이터 로그 조회"""
    try:
        log_service = DataLogService(settings.DAILY_DIR)
        return await log_service.get_data_logs(start_date, end_date, last_n_days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/logs/summary")
async def get_log_summary():
    """데이터 로그 요약 정보"""
    try:
        log_service = DataLogService(settings.DAILY_DIR)
        log_data = await log_service.get_data_logs()
        return log_data['summary']
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get(f"{settings.API_V1_STR}/enhanced")
async def get_enhanced_market_data():
    """확장된 시장 데이터 조회"""
    try:
        data = await enhanced_market_service.get_enhanced_market_data()
        
        await enhanced_market_service.save_enhanced_data(data)
        
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/enhanced/historical/{{days}}")
async def get_enhanced_historical_data(days: int = 7):
    """확장된 과거 데이터 조회"""
    try:
        return await enhanced_market_service.get_enhanced_historical_data(days)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/market-changes")
async def get_market_changes():
    """시장 변화 데이터 조회"""
    try:
        return await enhanced_market_service.get_market_changes()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/market-changes/save")
async def save_market_changes():
    """시장 변화 데이터 조회 및 JSON 파일로 저장"""
    try:
        # 시장 변화 데이터 조회
        market_changes = await enhanced_market_service.get_market_changes()
        
        # 저장 경로 설정
        save_dir = settings.CURRENT_DIR
        save_dir.mkdir(parents=True, exist_ok=True)
        file_path = save_dir / 'market.json'
        
        # JSON으로 저장
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(market_changes, f, ensure_ascii=False, indent=2, default=str)
        
        return {
            "status": "success",
            "message": "Market changes data saved successfully",
            "file_path": str(file_path)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save market changes: {str(e)}")

@app.get(f"{settings.API_V1_STR}/market-changes/current")
async def get_current_market_changes():
    """저장된 시장 변화 데이터 조회"""
    try:
        # 파일 경로 설정
        file_path = settings.CURRENT_DIR / 'market.json'
        
        # 파일이 존재하지 않는 경우
        if not file_path.exists():
            return {
                "status": "no_data",
                "message": "No saved market changes data found",
                "data": None
            }
        
        # JSON 파일 읽기
        with open(file_path, 'r', encoding='utf-8') as f:
            market_data = json.load(f)
        
        return {
            "status": "success",
            "data": market_data,
            "file_updated_at": file_path.stat().st_mtime
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read market changes: {str(e)}")
    
@app.get(f"{settings.API_V1_STR}/worldbank/update")
async def update_worldbank_data():
    """World Bank 데이터 업데이트"""
    try:
        result = await world_bank_service.fetch_and_save_data()
        return {
            "status": "success",
            "message": "World Bank data updated successfully",
            "details": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/worldbank/indicators/latest")
async def get_all_indicators(
    latest_only: bool = Query(True, description="Only retrieve the latest data")
):
    """모든 최신 지표 데이터 조회"""
    try:
        result = await world_bank_service.get_all_indicators(latest_only)
        if result['status'] == 'error':
            raise HTTPException(status_code=500, detail=result['error'])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get(f"{settings.API_V1_STR}/worldbank/indicators/{{indicator_name}}")
async def get_indicator_data(
    indicator_name: str,
    years: Optional[int] = Query(None, description="Number of years to retrieve")
):
    """특정 지표 데이터 조회"""
    try:
        result = await world_bank_service.get_indicator_data(indicator_name.upper(), years)
        if result['status'] == 'error':
            raise HTTPException(status_code=404, detail=result['error'])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get(f"{settings.API_V1_STR}/worldbank/indicators")
async def get_all_indicators(
    latest_only: bool = Query(False, description="Only retrieve the latest data")
):
    """모든 지표 데이터 조회"""
    try:
        result = await world_bank_service.get_all_indicators(latest_only)
        if result['status'] == 'error':
            raise HTTPException(status_code=500, detail=result['error'])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get(f"{settings.API_V1_STR}/selected-indicators/update")
async def update_selected_indicators():
    """선택된 지표 데이터 업데이트"""
    try:
        result = await selected_indicators_service.update_data()
        return {
            "status": "success",
            "message": "Selected indicators updated successfully",
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/selected-indicators/historical")
async def get_selected_indicators_history(
    days: Optional[int] = Query(7, description="조회할 일수")
):
    """선택된 지표의 과거 데이터 조회"""
    try:
        result = await selected_indicators_service.get_historical_data(days)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/selected-indicators/changes")
async def get_selected_indicators_changes(
    days: Optional[int] = Query(1, description="비교할 기간(일)")
):
    """선택된 지표의 변화율 조회"""
    try:
        result = await selected_indicators_service.calculate_changes(days)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/selected-indicators/current")
async def get_current_selected_indicators():
    """선택된 지표의 최신 데이터만 조회"""
    try:
        result = await selected_indicators_service.get_historical_data(days=1)
        if result["status"] == "success" and result["data"]:
            return {
                "status": "success",
                "data": result["data"][0]
            }
        return {
            "status": "error",
            "message": "No current data available"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/selected-indicators/period")
async def get_selected_indicators_period(
    start_date: Optional[str] = Query(None, description="시작일 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="종료일 (YYYY-MM-DD)"),
    last_n_days: Optional[int] = Query(None, description="최근 N일간의 데이터")
):
    """기간별 지표 데이터 조회
    
    - start_date와 end_date로 특정 기간 조회
    - 또는 last_n_days로 최근 N일간의 데이터 조회
    - 모든 매개변수가 None이면 전체 데이터 반환
    """
    try:
        # 날짜 형식 검증
        if start_date:
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid start_date format. Use YYYY-MM-DD"
                )
                
        if end_date:
            try:
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid end_date format. Use YYYY-MM-DD"
                )
        
        result = await selected_indicators_service.get_period_data(
            start_date=start_date,
            end_date=end_date,
            last_n_days=last_n_days
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
