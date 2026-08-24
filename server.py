# device_check_server.py

import os
import time
import uuid
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

import httpx
import asyncpg
from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.responses import JSONResponse
from pyrogram.raw.functions.account import GetAuthorizations, ResetAuthorization
from pydantic import BaseModel, Field, validator
from pyrogram import Client
from pyrogram.errors import (
    SessionExpired, 
    SessionRevoked, 
    FloodWait,
    Unauthorized,
    AuthKeyUnregistered
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Environment variables
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")
MAIN_SERVER_CALLBACK_URL = os.getenv("MAIN_SERVER_CALLBACK_URL", "")
MAIN_SERVER_RETRY_URL = os.getenv("MAIN_SERVER_RETRY_URL", "")
DEVICE_DB_URL = os.getenv("DEVICE_DB_URL", "")
OUR_DEVICE_MODEL = os.getenv("OUR_DEVICE_MODEL", "CPython 3.12.0")

# Initialize FastAPI app
app = FastAPI(
    title="Step 3: Device Check Server",
    description="Telegram device/session management server",
    version="1.0.0"
)

# Database pool
db_pool: Optional[asyncpg.Pool] = None

# Pydantic models for request validation
class ProcessRequest(BaseModel):
    session_id: str = Field(..., description="Unique session identifier")
    session_string: str = Field(..., description="Pyrogram session string")
    phone_number: str = Field(..., description="Phone number with country code")
    endpoint_name: str = Field(..., description="Bot endpoint name")
    callback_url: Optional[str] = Field(None, description="Success callback URL")
    retry_callback_url: Optional[str] = Field(None, description="Retry callback URL")
    
    @validator('session_id')
    def validate_session_id(cls, v):
        try:
            uuid.UUID(v)
            return v
        except ValueError:
            raise ValueError('Invalid session_id format')
    
    @validator('phone_number')
    def validate_phone(cls, v):
        if not v.startswith('+'):
            raise ValueError('Phone number must start with +')
        return v
    
    @validator('session_string')
    def validate_session_string(cls, v):
        if len(v) < 50:
            raise ValueError('Invalid session string')
        return v

class CallbackData(BaseModel):
    session_id: str
    success: bool
    device_termination_status: Optional[str] = None
    other_devices_terminated: Optional[int] = None
    retry_after_hours: Optional[int] = None
    error: Optional[str] = None
    message: Optional[str] = None

# Database initialization
async def init_database():
    """Initialize database pool and create tables"""
    global db_pool
    
    try:
        db_pool = await asyncpg.create_pool(
            DEVICE_DB_URL,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        
        # Create table if not exists
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS device_checks (
                    id SERIAL PRIMARY KEY,
                    session_id UUID NOT NULL,
                    phone_number VARCHAR(20) NOT NULL,
                    total_devices INTEGER DEFAULT 0,
                    our_device_count INTEGER DEFAULT 0,
                    other_devices_terminated INTEGER DEFAULT 0,
                    termination_status VARCHAR(50),
                    wait_until TIMESTAMP,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_time_ms INTEGER
                );
                
                CREATE INDEX IF NOT EXISTS idx_device_session_id 
                    ON device_checks(session_id);
                CREATE INDEX IF NOT EXISTS idx_device_phone 
                    ON device_checks(phone_number);
                CREATE INDEX IF NOT EXISTS idx_device_checked_at 
                    ON device_checks(checked_at);
            ''')
        
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

# Authentication dependency
async def verify_internal_key(x_internal_key: str = Header(...)):
    """Verify internal API key"""
    if x_internal_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal API key")
    return x_internal_key

# Save device check results to database
async def save_device_check_results(
    session_id: str,
    phone_number: str,
    total_devices: int,
    our_device_count: int,
    other_devices_terminated: int,
    termination_status: str,
    wait_until: Optional[datetime],
    processing_time_ms: int
):
    """Save device check results to database"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO device_checks 
                (session_id, phone_number, total_devices, our_device_count, 
                 other_devices_terminated, termination_status, wait_until, 
                 processing_time_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', 
            uuid.UUID(session_id), 
            phone_number, 
            total_devices,
            our_device_count,
            other_devices_terminated,
            termination_status,
            wait_until,
            processing_time_ms
            )
        logger.info(f"Device check results saved for session {session_id}")
    except Exception as e:
        logger.error(f"Failed to save device check results: {e}")

# Send callback to main server
async def send_callback(
    callback_url: str,
    callback_data: Dict[str, Any]
):
    """Send callback to main server"""
    try:
        async with httpx.AsyncClient(timeout=30) as http_client:
            response = await http_client.post(
                callback_url,
                json=callback_data,
                headers={"X-Internal-Key": INTERNAL_API_KEY}
            )
            if response.status_code == 200:
                logger.info(f"Callback sent successfully to {callback_url}")
                return True
            else:
                logger.error(f"Callback failed with status {response.status_code}")
                return False
    except Exception as e:
        logger.error(f"Callback error: {e}")
        return False

# Handle FloodWait with retry
async def handle_flood_wait(client: Client, error: FloodWait, max_retries: int = 3):
    """Handle FloodWait errors with exponential backoff"""
    for attempt in range(max_retries):
        wait_time = error.value + (attempt * 10)  # Add extra time for each retry
        logger.warning(f"FloodWait: waiting {wait_time} seconds (attempt {attempt + 1}/{max_retries})")
        await asyncio.sleep(wait_time)
        try:
            # Retry the operation after waiting
            return True
        except FloodWait as e:
            error = e
            continue
    return False

# Identify our device and others
async def identify_devices(client: Client) -> Tuple[Optional[str], List[str], int]:
    """
    Identify our device and other devices
    Returns: (our_device_hash, other_device_hashes, total_devices)
    """
    try:
        authorizations = await client.invoke(GetAuthorizations())
        total_devices = len(authorizations)
        our_device_hash = None
        other_devices = []
        
        for auth in authorizations:
            try:
                device_model = getattr(auth, 'device_model', 'Unknown')
                device_hash = getattr(auth, 'hash', None)
                
                if device_hash is None:
                    continue
                
                if device_model == OUR_DEVICE_MODEL:
                    our_device_hash = device_hash
                    logger.info(f"Our device found: {device_model}")
                else:
                    other_devices.append(device_hash)
                    logger.info(f"Other device found: {device_model}")
            except Exception as e:
                logger.warning(f"Error processing authorization: {e}")
                continue
        
        return our_device_hash, other_devices, total_devices
    except Exception as e:
        logger.error(f"Failed to get authorizations: {e}")
        raise

# Terminate other devices
async def terminate_other_devices(
    client: Client, 
    other_devices: List[str]
) -> Tuple[int, bool]:
    """
    Terminate other devices
    Returns: (terminated_count, wait_required)
    """
    terminated = 0
    wait_required = False
    
    for device_hash in other_devices:
        try:
            await client.invoke(ResetAuthorization(hash=device_hash))
            terminated += 1
            logger.info(f"Terminated device with hash: {device_hash[:10]}...")
            await asyncio.sleep(1)  # Small delay between terminations
        except FloodWait as e:
            logger.warning(f"FloodWait error: {e}")
            wait_handled = await handle_flood_wait(client, e)
            if not wait_handled:
                wait_required = True
                break
        except Exception as e:
            error_str = str(e)
            if "FRESH_CHANGE_ADMINS_FORBIDDEN" in error_str.upper() or \
               "fresh change" in error_str.lower():
                logger.warning("24h wait required for device termination")
                wait_required = True
                break
            else:
                logger.error(f"Error terminating device: {e}")
                continue
    
    return terminated, wait_required

# Main processing function
async def process_device_check(request: ProcessRequest):
    """Main device check processing function"""
    start_time = time.time()
    client = None
    termination_status = "unknown"
    wait_until = None
    
    try:
        # Initialize Pyrogram client
        logger.info(f"Starting device check for session {request.session_id}")
        client = Client(
            f"device_check_{request.session_id[:8]}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=request.session_string,
            in_memory=True
        )
        
        # Connect to Telegram
        await client.start()
        logger.info("Connected to Telegram successfully")
        
        # Get active authorizations
        our_device_hash, other_devices, total_devices = await identify_devices(client)
        
        # Check if we found our device
        our_device_count = 1 if our_device_hash else 0
        
        # Terminate other devices
        terminated = 0
        wait_required = False
        
        if other_devices:
            terminated, wait_required = await terminate_other_devices(client, other_devices)
        
        # Set termination status
        if wait_required:
            termination_status = "waiting_24h"
            wait_until = datetime.utcnow() + timedelta(hours=24)
            logger.info("24h wait required for device termination")
        else:
            termination_status = "all_terminated"
            logger.info(f"All devices terminated: {terminated} devices")
        
        # Calculate processing time
        processing_time = int((time.time() - start_time) * 1000)
        
        # Save results to database
        await save_device_check_results(
            session_id=request.session_id,
            phone_number=request.phone_number,
            total_devices=total_devices,
            our_device_count=our_device_count,
            other_devices_terminated=terminated,
            termination_status=termination_status,
            wait_until=wait_until,
            processing_time_ms=processing_time
        )
        
        # Prepare callback data
        if wait_required:
            callback_data = {
                "session_id": request.session_id,
                "success": False,
                "device_termination_status": "waiting_24h",
                "retry_after_hours": 24,
                "message": "Fresh device change wait required"
            }
            callback_url = request.retry_callback_url or MAIN_SERVER_RETRY_URL
        else:
            callback_data = {
                "session_id": request.session_id,
                "success": True,
                "device_termination_status": "all_terminated",
                "other_devices_terminated": terminated,
                "message": f"All other devices terminated ({terminated} devices)"
            }
            callback_url = request.callback_url or MAIN_SERVER_CALLBACK_URL
        
        # Send callback to main server
        await send_callback(callback_url, callback_data)
        
        return {
            "success": True,
            "session_id": request.session_id,
            "device_termination_status": termination_status,
            "other_devices_terminated": terminated,
            "total_devices": total_devices,
            "processing_time_ms": processing_time
        }
        
    except SessionExpired:
        logger.error("Session expired")
        callback_data = {
            "session_id": request.session_id,
            "success": False,
            "error": "SessionExpired",
            "message": "Session expired"
        }
        await send_callback(request.callback_url or MAIN_SERVER_CALLBACK_URL, callback_data)
        raise HTTPException(status_code=401, detail="Session expired")
        
    except SessionRevoked:
        logger.error("Session revoked")
        callback_data = {
            "session_id": request.session_id,
            "success": False,
            "error": "SessionRevoked",
            "message": "Session revoked"
        }
        await send_callback(request.callback_url or MAIN_SERVER_CALLBACK_URL, callback_data)
        raise HTTPException(status_code=401, detail="Session revoked")
        
    except Unauthorized:
        logger.error("Unauthorized access")
        callback_data = {
            "session_id": request.session_id,
            "success": False,
            "error": "Unauthorized",
            "message": "Unauthorized access"
        }
        await send_callback(request.callback_url or MAIN_SERVER_CALLBACK_URL, callback_data)
        raise HTTPException(status_code=401, detail="Unauthorized")
        
    except AuthKeyUnregistered:
        logger.error("Auth key unregistered")
        callback_data = {
            "session_id": request.session_id,
            "success": False,
            "error": "AuthKeyUnregistered",
            "message": "Auth key unregistered"
        }
        await send_callback(request.callback_url or MAIN_SERVER_CALLBACK_URL, callback_data)
        raise HTTPException(status_code=401, detail="Auth key unregistered")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        callback_data = {
            "session_id": request.session_id,
            "success": False,
            "error": str(e),
            "message": "Internal server error"
        }
        await send_callback(request.callback_url or MAIN_SERVER_CALLBACK_URL, callback_data)
        raise HTTPException(status_code=500, detail="Internal server error")
        
    finally:
        # Disconnect client
        if client:
            try:
                await client.stop()
                logger.info("Client disconnected")
            except:
                pass

# API endpoints
@app.post("/process")
async def process_endpoint(
    request: ProcessRequest,
    x_internal_key: str = Depends(verify_internal_key)
):
    """Main processing endpoint"""
    try:
        result = await process_device_check(request)
        return JSONResponse(content=result, status_code=200)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_data = {
        "status": "healthy",
        "service": "device-check-server",
        "timestamp": datetime.utcnow().isoformat(),
        "database_connected": db_pool is not None
    }
    
    # Check database connectivity
    if db_pool:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("SELECT 1")
            health_data["database_status"] = "connected"
        except:
            health_data["database_status"] = "disconnected"
            health_data["status"] = "degraded"
    
    return JSONResponse(content=health_data, status_code=200 if health_data["status"] == "healthy" else 503)

@app.get("/stats")
async def get_stats(
    x_internal_key: str = Depends(verify_internal_key)
):
    """Get device check statistics"""
    try:
        async with db_pool.acquire() as conn:
            total_checks = await conn.fetchval("SELECT COUNT(*) FROM device_checks")
            total_terminated = await conn.fetchval(
                "SELECT COALESCE(SUM(other_devices_terminated), 0) FROM device_checks"
            )
            waiting_24h = await conn.fetchval(
                "SELECT COUNT(*) FROM device_checks WHERE termination_status = 'waiting_24h'"
            )
            avg_time = await conn.fetchval(
                "SELECT AVG(processing_time_ms) FROM device_checks"
            )
            
            return {
                "total_checks": total_checks,
                "total_devices_terminated": total_terminated,
                "waiting_24h_count": waiting_24h,
                "average_processing_time_ms": int(avg_time) if avg_time else 0
            }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get stats")

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    await init_database()
    logger.info("Device Check Server started")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("Database pool closed")

# For testing
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8003,
        log_level="info"
    )
