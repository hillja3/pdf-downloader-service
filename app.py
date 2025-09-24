import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from supabase import create_client, Client
import httpx
from playwright.async_api import async_playwright
import redis
import json
from uuid import uuid4

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add version marker to your code
VERSION = "2.3-FINAL-FIXED"
logger.info(f"PDF Downloader Service v{VERSION} starting...")

app = FastAPI(
    title="PDF Downloader Service",
    default_response_class=JSONResponse  # Always JSON
)

# Initialize Supabase client with error handling
supabase: Optional[Client] = None
try:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if supabase_url and supabase_key:
        supabase = create_client(supabase_url, supabase_key)
        logger.info("✅ Supabase client initialized")
    else:
        logger.warning("⚠️ Supabase credentials not found in environment variables")
        logger.warning("⚠️ PDFs will be downloaded but not stored. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
except Exception as e:
    logger.error(f"❌ Failed to initialize Supabase client: {e}")
    supabase = None

# Initialize Redis for queue management
redis_client = None
try:
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    
    redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=0,
        decode_responses=True
    )
    # Test connection
    redis_client.ping()
    logger.info(f"✅ Redis connected successfully at {redis_host}:{redis_port}")
except Exception as e:
    logger.error(f"❌ Redis connection failed: {e}")
    logger.info("💡 Running without Redis - using in-memory queue")
    redis_client = None

# Request models
class DownloadRequest(BaseModel):
    document_id: str
    document_url: str
    charter_num: str
    document_type: Optional[str] = "other"
    priority: Optional[int] = 5  # 1-10, lower is higher priority
    county: Optional[str] = "montgomery"

class BatchDownloadRequest(BaseModel):
    documents: List[DownloadRequest]
    callback_url: Optional[str] = None

# Response models
class DownloadStatus(BaseModel):
    document_id: str
    status: str  # pending, processing, completed, failed
    storage_path: Optional[str] = None
    file_size: Optional[int] = None
    error: Optional[str] = None
    attempts: int = 0
    last_attempt: Optional[datetime] = None

# OnBase PDF Downloader
class OnBasePDFDownloader:
    def __init__(self):
        self.browser = None
        self.context = None
        
    async def initialize(self):
        """Initialize browser instance"""
        if not self.browser:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            )
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0'
            )
    
    async def cleanup(self):
        """Clean up browser resources"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
    
    async def download_pdf(self, url: str) -> bytes:
        """Download PDF from OnBase URL"""
        await self.initialize()
        
        page = await self.context.new_page()
        pdf_data = None
        
        try:
            # Setup response interceptor for PDFs
            async def handle_response(response):
                nonlocal pdf_data
                content_type = response.headers.get('content-type', '')
                if response.url.endswith('.pdf') or 'application/pdf' in content_type:
                    logger.info(f"🎯 Intercepted PDF: {response.url}")
                    pdf_data = await response.body()
            
            page.on('response', handle_response)
            
            logger.info(f"📄 Navigating to: {url}")
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # Wait for OnBase to load the document
            logger.info("⏳ Waiting for document to load...")
            
            # Try multiple strategies to find the PDF - FIXED INDENTATION
            strategies = [
                # Strategy 1: Wait for PDF iframe/embed
                ('iframe[src*=".pdf"], iframe#docFrame, embed[type="application/pdf"]', 15000),
                # Strategy 2: Wait for document viewer
                ('.document-page, #documentViewer, .page-container', 10000),
                # Strategy 3: Wait for canvas (rendered PDF)
                ('canvas.textLayer, canvas#page1', 10000)
            ]
            
            for selector, timeout in strategies:
                try:
                    element = await page.wait_for_selector(
                        selector,
                        timeout=timeout,
                        state='visible'
                    )
                    
                    if element:
                        logger.info(f"✅ Found element: {selector}")
                        
                        # If it's an iframe/embed, get the PDF URL
                        if 'iframe' in selector or 'embed' in selector:
                            pdf_url = await element.get_attribute('src') or await element.get_attribute('data')
                            if pdf_url:
                                if pdf_url.startswith('/'):
                                    from urllib.parse import urljoin
                                    pdf_url = urljoin(url, pdf_url)
                                
                                logger.info(f"📥 Downloading from: {pdf_url}")
                                response = await page.request.get(pdf_url)
                                pdf_data = await response.body()
                                break
                        
                        # Wait a bit more for full load
                        await page.wait_for_timeout(3000)
                        break
                except Exception as e:
                    logger.error(f"Strategy {selector} failed: {e}")
                    continue  # Now properly inside the for loop
            
            # If no PDF intercepted, try to extract it
            if not pdf_data:
                # Check for direct PDF response
                if page.url.endswith('.pdf'):
                    logger.info("📥 Page URL is a PDF, downloading...")
                    response = await page.request.get(page.url)
                    pdf_data = await response.body()
                
                # Last resort: Print to PDF
                if not pdf_data:
                    logger.info("📸 Printing page to PDF...")
                    await page.wait_for_timeout(5000)  # Final wait
                    pdf_data = await page.pdf(
                        format='A4',
                        print_background=True,
                        display_header_footer=False,
                        margin={'top': '0', 'bottom': '0', 'left': '0', 'right': '0'}
                    )
            
            # Validate PDF data
            if pdf_data:
                if isinstance(pdf_data, bool):
                    logger.error("PDF content unexpectedly boolean")
                    raise ValueError("Invalid PDF content type (bool)")
                
                if len(pdf_data) < 1000:
                    raise ValueError(f"PDF too small: {len(pdf_data)} bytes")
                
                if not pdf_data.startswith(b'%PDF'):
                    logger.warning("File doesn't start with %PDF header, but continuing...")
                
                # Check for placeholder
                pdf_header = pdf_data[:1000].decode('latin-1', errors='ignore')
                if 'Logo' in pdf_header and len(pdf_data) < 20000:
                    raise ValueError("PDF is a placeholder (contains only 'Logo')")
                
                logger.info(f"✅ Downloaded valid PDF: {len(pdf_data)} bytes")
                return pdf_data
            else:
                raise ValueError("Failed to capture PDF from OnBase")
                
        except Exception as e:
            logger.error(f"Download error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
        finally:
            await page.close()

# Queue Manager with fallback for no Redis
class QueueManager:
    QUEUE_KEY = "pdf_download_queue"
    STATUS_KEY_PREFIX = "pdf_status:"
    
    # In-memory fallback when Redis not available
    _memory_queue = []
    _memory_status = {}
    
    @staticmethod
    def add_to_queue(request: DownloadRequest):
        """Add download request to queue"""
        status = DownloadStatus(
            document_id=request.document_id,
            status="pending",
            attempts=0
        )
        
        if redis_client:
            try:
                # Store request in Redis
                status_key = f"{QueueManager.STATUS_KEY_PREFIX}{request.document_id}"
                redis_client.setex(
                    status_key,
                    86400,  # 24 hour TTL
                    json.dumps(status.dict())
                )
                
                # Add to priority queue
                redis_client.zadd(
                    QueueManager.QUEUE_KEY,
                    {json.dumps(request.dict()): request.priority}
                )
                logger.info(f"📝 Added {request.document_id} to Redis queue with priority {request.priority}")
            except Exception as e:
                logger.error(f"Redis error, falling back to memory: {e}")
                QueueManager._memory_queue.append((request, request.priority))
                QueueManager._memory_status[request.document_id] = status
                logger.info(f"📝 Added {request.document_id} to memory queue with priority {request.priority}")
        else:
            # Use in-memory queue
            QueueManager._memory_queue.append((request, request.priority))
            QueueManager._memory_status[request.document_id] = status
            logger.info(f"📝 Added {request.document_id} to memory queue with priority {request.priority}")
        
        return status
    
    @staticmethod
    def get_next_job() -> Optional[DownloadRequest]:
        """Get next job from queue"""
        if redis_client:
            try:
                # Pop lowest score (highest priority) from Redis
                result = redis_client.zpopmin(QueueManager.QUEUE_KEY, 1)
                if result:
                    job_data, _ = result[0]
                    return DownloadRequest(**json.loads(job_data))
            except Exception as e:
                logger.error(f"Redis error, using memory queue: {e}")
        
        # Use in-memory queue
        if QueueManager._memory_queue:
            # Sort by priority and get highest priority job
            QueueManager._memory_queue.sort(key=lambda x: x[1])
            request, _ = QueueManager._memory_queue.pop(0)
            return request
        
        return None
    
    @staticmethod
    def update_status(document_id: str, status: DownloadStatus):
        """Update job status"""
        if redis_client:
            try:
                status_key = f"{QueueManager.STATUS_KEY_PREFIX}{document_id}"
                redis_client.setex(
                    status_key,
                    86400,
                    json.dumps(status.dict(default=str))
                )
                return
            except Exception as e:
                logger.error(f"Redis error, using memory status: {e}")
        
        # Use in-memory status
        QueueManager._memory_status[document_id] = status
    
    @staticmethod
    def get_status(document_id: str) -> Optional[DownloadStatus]:
        """Get job status"""
        if redis_client:
            try:
                status_key = f"{QueueManager.STATUS_KEY_PREFIX}{document_id}"
                data = redis_client.get(status_key)
                if data:
                    return DownloadStatus(**json.loads(data))
            except Exception as e:
                logger.error(f"Redis error, using memory status: {e}")
        
        # Use in-memory status
        return QueueManager._memory_status.get(document_id)

# Storage helper function with better error handling
async def store_document(document_id: str, charter_num: str, document_url: str, 
                         document_type: str, pdf_content: bytes, county: str = "montgomery"):
    """Store document using Supabase Storage and save metadata in table"""
    
    # Check if Supabase is properly configured
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        logger.warning(f"⚠️ Supabase credentials not configured, cannot store {document_id}")
        logger.warning("⚠️ PDF was downloaded successfully but not stored. Configure SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        return None
    
    if not supabase:
        logger.warning(f"⚠️ Supabase client not initialized, cannot store {document_id}")
        return None
    
    try:
        filename = f"{county}/{document_id}-{uuid4().hex}.pdf"
        
        # Create bucket if it doesn't exist (handle silently if exists)
        try:
            supabase.storage.create_bucket("county-records", public=False)
            logger.info("Created storage bucket: county-records")
        except Exception as e:
            # Bucket likely already exists, which is fine
            if "already exists" not in str(e).lower():
                logger.debug(f"Bucket creation note: {e}")
        
        # Upload to storage bucket
        logger.info(f"📤 Uploading {document_id} to storage...")
        response = supabase.storage.from_("county-records").upload(
            file=pdf_content, 
            path=filename, 
            file_options={"content-type": "application/pdf", "upsert": True}
        )
        
        # Save metadata in table
        table_name = f"{county}_documents"
        metadata = {
            "document_id": document_id,
            "charter_num": charter_num,
            "document_url": document_url,
            "document_type": document_type,
            "storage_path": filename,
            "file_size": len(pdf_content),
            "mime_type": "application/pdf",
            "processing_status": "completed",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "crawled_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Try to upsert to database
        logger.info(f"📊 Saving metadata for {document_id} to {table_name}")
        supabase.table(table_name).upsert(metadata, on_conflict="document_id").execute()
        
        logger.info(f"✅ Successfully stored {document_id}: {filename} ({len(pdf_content)} bytes)")
        return filename
        
    except Exception as e:
        error_msg = str(e)
        
        # Check for specific error types
        if "'bool' object has no attribute 'encode'" in error_msg:
            logger.error(f"❌ Storage API configuration error for {document_id}")
            logger.error("This usually means Supabase credentials are invalid or the service is not properly configured")
        elif "relation" in error_msg and "does not exist" in error_msg:
            logger.error(f"❌ Database table '{county}_documents' does not exist")
            logger.error("Please create the table in Supabase first")
        else:
            logger.error(f"❌ Storage error for {document_id}: {error_msg}")
        
        import traceback
        logger.debug(traceback.format_exc())
        
        # Don't raise the error - return None to indicate storage failed
        # The PDF was still downloaded successfully
        return None

# Worker Process
async def process_download(request: DownloadRequest):
    """Process a single download request"""
    downloader = OnBasePDFDownloader()
    status = QueueManager.get_status(request.document_id) or DownloadStatus(
        document_id=request.document_id,
        status="processing",
        attempts=0
    )
    
    try:
        # Update status to processing
        status.status = "processing"
        status.last_attempt = datetime.now(timezone.utc)
        status.attempts += 1
        QueueManager.update_status(request.document_id, status)
        
        # Download PDF
        logger.info(f"⬇️ Downloading {request.document_id} from {request.document_url}")
        pdf_data = await downloader.download_pdf(request.document_url)
        
        # Store document using Supabase Storage
        county = getattr(request, 'county', 'montgomery')
        storage_path = await store_document(
            document_id=request.document_id,
            charter_num=request.charter_num,
            document_url=request.document_url,
            document_type=request.document_type,
            pdf_content=pdf_data,
            county=county
        )
        
        # Update status based on whether storage succeeded
        if storage_path:
            status.status = "completed"
            status.storage_path = storage_path
            logger.info(f"✅ Successfully processed and stored {request.document_id}")
        else:
            status.status = "downloaded"  # New status: downloaded but not stored
            logger.info(f"⚠️ Downloaded {request.document_id} but storage unavailable")
        
        status.file_size = len(pdf_data)
        QueueManager.update_status(request.document_id, status)
        
        return status
        
    except Exception as e:
        logger.error(f"❌ Process download error for {request.document_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Update status
        status.status = "failed"
        status.error = str(e)
        QueueManager.update_status(request.document_id, status)
        
        # Retry logic
        if status.attempts < 3:
            # Re-add to queue with lower priority
            request.priority = min(request.priority + 2, 10)
            QueueManager.add_to_queue(request)
            logger.info(f"🔄 Requeued {request.document_id} for retry (attempt {status.attempts}/3)")
        
        return status
    finally:
        await downloader.cleanup()

# Global stop flag for graceful shutdown
stop_flag = False

# Background worker loop
async def worker_loop():
    """Continuously process queue"""
    logger.info("🚀 Starting worker loop")
    
    while not stop_flag:
        try:
            # Get next job
            job = QueueManager.get_next_job()
            
            if job:
                logger.info(f"📋 Processing job: {job.document_id}")
                await process_download(job)
            else:
                # No jobs, wait a bit
                await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"Worker loop error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)
    
    logger.info("🛑 Worker loop stopped")

# API Endpoints
@app.on_event("startup")
async def startup_event():
    """Start background worker"""
    logger.info(f"🚀 Starting PDF Downloader Service v{VERSION}")
    logger.info(f"📍 Environment: Supabase={'✅ Configured' if supabase else '❌ Not configured'}, Redis={'✅ Connected' if redis_client else '❌ Using memory queue'}")
    asyncio.create_task(worker_loop())
    logger.info("✅ PDF Downloader Service started")

@app.get("/version")
async def version():
    """Version check endpoint"""
    return {
        "version": VERSION, 
        "status": "running",
        "supabase_configured": supabase is not None,
        "redis_connected": redis_client is not None
    }

@app.get("/")
async def root():
    """Health check root endpoint"""
    return {
        "service": "PDF Downloader",
        "version": VERSION,
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/download")
async def download_pdf(request: DownloadRequest, background_tasks: BackgroundTasks):
    """Queue a PDF for download"""
    try:
        status = QueueManager.add_to_queue(request)
        # Return plain dict - FastAPI will JSONify it
        return {
            "status": "success",
            "message": "Download queued",
            "id": request.document_id,
            "document_id": request.document_id
        }
    except Exception as e:
        logger.error(f"Endpoint error: {e}")
        # Return dict on error too
        return {"status": "error", "message": str(e)}

@app.post("/download/batch")
async def download_batch(requests: list[DownloadRequest]):
    """Queue multiple PDFs for download"""
    try:
        results = []
        for request in requests:
            status = QueueManager.add_to_queue(request)
            results.append({"document_id": request.document_id, "queue_id": request.document_id})
        # Return plain dict
        return {
            "status": "success",
            "message": f"Queued {len(results)} downloads",
            "results": results
        }
    except Exception as e:
        logger.error(f"Batch endpoint error: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/status/{document_id}")
async def get_status(document_id: str):
    """Get download status"""
    status = QueueManager.get_status(document_id)
    if status:
        return status.dict()
    return {"status": "unknown", "message": "Download not found"}

@app.get("/queue/stats")
async def queue_stats():
    """Get queue statistics"""
    stats = {
        "processing": len(QueueManager._memory_status),
        "queued": len(QueueManager._memory_queue),
        "use_redis": redis_client is not None,
        "supabase_configured": supabase is not None
    }
    
    if redis_client:
        try:
            stats["queued"] = redis_client.zcard(QueueManager.QUEUE_KEY)
        except:
            pass
    
    return stats

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.post("/stop")
async def stop_service():
    """Stop the PDF downloader service"""
    global stop_flag
    stop_flag = True
    return {"status": "success", "message": "Stop signal sent"}

# Global exception handler
@app.exception_handler(Exception)
async def all_exception_handler(request: Request, exc: Exception):
    """Catch all unhandled exceptions and return JSON"""
    logger.exception(f"Unhandled error: {exc}")
    return JSONResponse(
        status_code=500, 
        content={"status": "error", "message": str(exc)}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
