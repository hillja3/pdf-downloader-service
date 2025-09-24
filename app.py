import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from playwright.async_api import async_playwright, Browser, BrowserContext
import json
from uuid import uuid4

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Version
VERSION = "3.0-PRODUCTION-S3"
logger.info(f"PDF Downloader Service v{VERSION} starting...")

app = FastAPI(
    title="PDF Downloader Service",
    default_response_class=JSONResponse
)

# Initialize S3 client
s3_client = None
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "county-documents")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    # Test connection
    s3_client.head_bucket(Bucket=S3_BUCKET)
    logger.info(f"✅ S3 connected to bucket: {S3_BUCKET}")
except Exception as e:
    logger.error(f"❌ S3 connection failed: {e}")
    logger.warning("Will save PDFs locally as fallback")

# Initialize Supabase for metadata only (not file storage)
supabase = None
try:
    from supabase import create_client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if supabase_url and supabase_key:
        supabase = create_client(supabase_url, supabase_key)
        logger.info("✅ Supabase connected for metadata")
except Exception as e:
    logger.warning(f"Supabase not available for metadata: {e}")

# Global browser instance for memory efficiency
BROWSER: Optional[Browser] = None
BROWSER_CONTEXT: Optional[BrowserContext] = None
PLAYWRIGHT = None

# Request models
class DownloadRequest(BaseModel):
    document_id: str
    document_url: str
    charter_num: str
    document_type: Optional[str] = "other"
    priority: Optional[int] = 5
    county: Optional[str] = "montgomery"

class DownloadStatus(BaseModel):
    document_id: str
    status: str  # pending, processing, completed, failed
    storage_path: Optional[str] = None
    s3_url: Optional[str] = None
    file_size: Optional[int] = None
    error: Optional[str] = None
    attempts: int = 0
    last_attempt: Optional[datetime] = None

# Browser management
async def init_browser():
    """Initialize global browser instance"""
    global BROWSER, BROWSER_CONTEXT, PLAYWRIGHT
    
    if not BROWSER:
        logger.info("🌐 Initializing browser...")
        PLAYWRIGHT = await async_playwright().start()
        BROWSER = await PLAYWRIGHT.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--single-process',
                '--disable-extensions',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding'
            ]
        )
        BROWSER_CONTEXT = await BROWSER.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
        )
        logger.info("✅ Browser initialized")

async def cleanup_browser():
    """Cleanup browser resources"""
    global BROWSER, BROWSER_CONTEXT, PLAYWRIGHT
    
    try:
        if BROWSER_CONTEXT:
            await BROWSER_CONTEXT.close()
        if BROWSER:
            await BROWSER.close()
        if PLAYWRIGHT:
            await PLAYWRIGHT.stop()
    except:
        pass
    finally:
        BROWSER = None
        BROWSER_CONTEXT = None
        PLAYWRIGHT = None
    logger.info("🧹 Browser cleaned up")

async def download_pdf(url: str) -> bytes:
    """Download PDF from OnBase URL"""
    
    if not BROWSER_CONTEXT:
        await init_browser()
    
    page = None
    try:
        page = await BROWSER_CONTEXT.new_page()
        pdf_data = None
        
        # Setup response interceptor
        async def handle_response(response):
            nonlocal pdf_data
            content_type = response.headers.get('content-type', '')
            if response.url.endswith('.pdf') or 'application/pdf' in content_type:
                logger.info(f"🎯 Intercepted PDF: {response.url}")
                pdf_data = await response.body()
        
        page.on('response', handle_response)
        
        logger.info(f"📄 Navigating to: {url}")
        await page.goto(url, wait_until='domcontentloaded', timeout=30000)
        
        # Wait for document to load
        await page.wait_for_timeout(3000)
        
        # Try to find PDF elements (reduced timeouts)
        strategies = [
            ('iframe[src*=".pdf"], embed[type="application/pdf"]', 5000),
            ('.document-page, #documentViewer', 5000)
        ]
        
        for selector, timeout in strategies:
            try:
                await page.wait_for_selector(selector, timeout=timeout, state='visible')
                logger.info(f"✅ Found: {selector}")
                await page.wait_for_timeout(2000)
                break
            except:
                continue
        
        # If no PDF intercepted, print to PDF
        if not pdf_data:
            logger.info("📸 Printing page to PDF...")
            await page.wait_for_timeout(3000)
            pdf_data = await page.pdf(
                format='A4',
                print_background=True,
                display_header_footer=False,
                margin={'top': '0', 'bottom': '0', 'left': '0', 'right': '0'}
            )
        
        if pdf_data and len(pdf_data) > 1000:
            logger.info(f"✅ Downloaded PDF: {len(pdf_data)} bytes")
            return pdf_data
        else:
            raise ValueError("Failed to capture valid PDF")
            
    except Exception as e:
        logger.error(f"Download error: {e}")
        raise
    finally:
        if page:
            await page.close()

# Storage functions
async def store_to_s3(document_id: str, charter_num: str, pdf_content: bytes, county: str = "montgomery") -> Dict[str, str]:
    """Store PDF to S3"""
    if not s3_client:
        return None
    
    try:
        # Create S3 key
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        s3_key = f"{county}/{timestamp}/{charter_num}/{document_id}.pdf"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=pdf_content,
            ContentType='application/pdf',
            Metadata={
                'document_id': document_id,
                'charter_num': charter_num,
                'county': county,
                'download_date': datetime.now(timezone.utc).isoformat()
            }
        )
        
        # Generate URL
        s3_url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
        
        logger.info(f"✅ Uploaded to S3: {s3_key}")
        return {
            "s3_key": s3_key,
            "s3_url": s3_url
        }
        
    except Exception as e:
        logger.error(f"S3 upload error: {e}")
        return None

async def save_metadata_to_supabase(document_id: str, charter_num: str, document_url: str, 
                                   s3_data: Dict, file_size: int, county: str = "montgomery"):
    """Save metadata to Supabase (not the file itself)"""
    if not supabase:
        return
    
    try:
        table_name = f"{county}_documents"
        metadata = {
            "document_id": document_id,
            "charter_num": charter_num,
            "document_url": document_url,
            "s3_key": s3_data.get("s3_key"),
            "s3_url": s3_data.get("s3_url"),
            "file_size": file_size,
            "processing_status": "completed",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        supabase.table(table_name).upsert(metadata, on_conflict="document_id").execute()
        logger.info(f"✅ Metadata saved for {document_id}")
        
    except Exception as e:
        logger.warning(f"Metadata save failed: {e}")

async def store_locally_as_backup(document_id: str, pdf_content: bytes, county: str = "montgomery") -> str:
    """Fallback local storage"""
    try:
        storage_dir = f"/tmp/pdfs/{county}"
        os.makedirs(storage_dir, exist_ok=True)
        
        filename = f"{document_id}.pdf"
        filepath = os.path.join(storage_dir, filename)
        
        with open(filepath, 'wb') as f:
            f.write(pdf_content)
        
        logger.info(f"💾 Saved locally (backup): {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Local storage error: {e}")
        return None

# Queue Manager (simplified in-memory)
class QueueManager:
    _memory_queue = []
    _memory_status = {}
    
    @staticmethod
    def add_to_queue(request: DownloadRequest):
        status = DownloadStatus(
            document_id=request.document_id,
            status="pending",
            attempts=0
        )
        QueueManager._memory_queue.append((request, request.priority))
        QueueManager._memory_status[request.document_id] = status
        logger.info(f"📝 Queued {request.document_id}")
        return status
    
    @staticmethod
    def get_next_job() -> Optional[DownloadRequest]:
        if QueueManager._memory_queue:
            QueueManager._memory_queue.sort(key=lambda x: x[1])
            request, _ = QueueManager._memory_queue.pop(0)
            return request
        return None
    
    @staticmethod
    def update_status(document_id: str, status: DownloadStatus):
        QueueManager._memory_status[document_id] = status
    
    @staticmethod
    def get_status(document_id: str) -> Optional[DownloadStatus]:
        return QueueManager._memory_status.get(document_id)

# Worker Process
async def process_download(request: DownloadRequest):
    """Process a single download request"""
    status = QueueManager.get_status(request.document_id) or DownloadStatus(
        document_id=request.document_id,
        status="processing",
        attempts=0
    )
    
    try:
        status.status = "processing"
        status.last_attempt = datetime.now(timezone.utc)
        status.attempts += 1
        QueueManager.update_status(request.document_id, status)
        
        # Download PDF
        logger.info(f"⬇️ Downloading {request.document_id}")
        pdf_data = await download_pdf(request.document_url)
        
        # Store to S3
        s3_data = await store_to_s3(
            request.document_id,
            request.charter_num,
            pdf_data,
            request.county
        )
        
        if s3_data:
            # Save metadata to Supabase
            await save_metadata_to_supabase(
                request.document_id,
                request.charter_num,
                request.document_url,
                s3_data,
                len(pdf_data),
                request.county
            )
            
            status.status = "completed"
            status.storage_path = s3_data["s3_key"]
            status.s3_url = s3_data["s3_url"]
            status.file_size = len(pdf_data)
            logger.info(f"✅ Completed {request.document_id} - Stored in S3")
        else:
            # Fallback to local storage
            local_path = await store_locally_as_backup(
                request.document_id,
                pdf_data,
                request.county
            )
            status.status = "completed_local"
            status.storage_path = local_path
            status.file_size = len(pdf_data)
            logger.info(f"⚠️ Completed {request.document_id} - Stored locally")
        
        QueueManager.update_status(request.document_id, status)
        return status
        
    except Exception as e:
        logger.error(f"❌ Error processing {request.document_id}: {e}")
        status.status = "failed"
        status.error = str(e)
        QueueManager.update_status(request.document_id, status)
        
        # Retry logic
        if status.attempts < 3:
            request.priority = min(request.priority + 2, 10)
            QueueManager.add_to_queue(request)
            logger.info(f"🔄 Requeued {request.document_id} (attempt {status.attempts}/3)")
        
        return status

# Global flags
stop_flag = False
MAX_DOWNLOADS_BEFORE_RESTART = 25
download_counter = 0

# Background worker loop
async def worker_loop():
    """Process queue with memory management"""
    global download_counter
    
    logger.info("🚀 Starting worker loop")
    await init_browser()
    
    while not stop_flag:
        try:
            job = QueueManager.get_next_job()
            
            if job:
                logger.info(f"📋 Processing job #{download_counter+1}: {job.document_id}")
                await process_download(job)
                download_counter += 1
                
                # Restart browser periodically
                if download_counter >= MAX_DOWNLOADS_BEFORE_RESTART:
                    logger.info("🔄 Restarting browser (memory management)...")
                    await cleanup_browser()
                    await asyncio.sleep(2)
                    await init_browser()
                    download_counter = 0
            else:
                await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"Worker error: {e}")
            # Try to recover
            await cleanup_browser()
            await asyncio.sleep(5)
            await init_browser()
    
    await cleanup_browser()
    logger.info("🛑 Worker stopped")

# API Endpoints
@app.on_event("startup")
async def startup_event():
    logger.info(f"🚀 Starting PDF Downloader v{VERSION}")
    logger.info(f"📦 S3 Bucket: {S3_BUCKET if s3_client else 'Not configured'}")
    logger.info(f"💾 Supabase: {'Connected' if supabase else 'Not configured'}")
    asyncio.create_task(worker_loop())
    logger.info("✅ Service started")

@app.on_event("shutdown")
async def shutdown_event():
    global stop_flag
    stop_flag = True
    await cleanup_browser()
    logger.info("👋 Service stopped")

@app.get("/version")
async def version():
    return {
        "version": VERSION,
        "status": "running",
        "storage": {
            "s3_configured": s3_client is not None,
            "s3_bucket": S3_BUCKET if s3_client else None,
            "supabase_metadata": supabase is not None
        },
        "queue": {
            "size": len(QueueManager._memory_queue),
            "download_counter": download_counter
        }
    }

@app.get("/")
async def root():
    return {
        "service": "PDF Downloader",
        "version": VERSION,
        "status": "healthy"
    }

@app.post("/download")
async def download_endpoint(request: DownloadRequest):
    try:
        status = QueueManager.add_to_queue(request)
        return {
            "status": "success",
            "message": "Download queued",
            "document_id": request.document_id
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/download/batch")
async def download_batch(requests: List[DownloadRequest]):
    results = []
    for request in requests:
        status = QueueManager.add_to_queue(request)
        results.append({"document_id": request.document_id})
    return {
        "status": "success",
        "message": f"Queued {len(results)} downloads",
        "results": results
    }

@app.get("/status/{document_id}")
async def get_status(document_id: str):
    status = QueueManager.get_status(document_id)
    if status:
        return status.dict()
    return {"status": "unknown"}

@app.get("/queue/stats")
async def queue_stats():
    statuses = QueueManager._memory_status.values()
    return {
        "queued": len(QueueManager._memory_queue),
        "processing": len([s for s in statuses if s.status == "processing"]),
        "completed": len([s for s in statuses if s.status == "completed"]),
        "completed_local": len([s for s in statuses if s.status == "completed_local"]),
        "failed": len([s for s in statuses if s.status == "failed"]),
        "total": len(QueueManager._memory_status)
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
