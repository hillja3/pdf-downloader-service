# app.py - COMPLETE FIXED VERSION for OnBase document extraction
import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from dotenv import load_dotenv
import boto3
from playwright.async_api import async_playwright, Browser, BrowserContext
import base64
from io import BytesIO
import re
from PIL import Image
import json

load_dotenv()

# Enhanced logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

VERSION = "5.2-IFRAME-FIX"
logger.info(f"PDF Downloader Service v{VERSION} starting...")

# Create FastAPI app instance - THIS IS REQUIRED
app = FastAPI(title="PDF Downloader Service")

# Initialize S3 client
s3_client = boto3.client(
    's3',
    region_name=os.getenv('AWS_REGION', 'us-east-1'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
S3_BUCKET = os.getenv('S3_BUCKET_NAME', 'county-documents')

# Global browser instance
BROWSER: Optional[Browser] = None
BROWSER_CONTEXT: Optional[BrowserContext] = None
PLAYWRIGHT = None

class DownloadRequest(BaseModel):
    document_id: str
    document_url: str
    charter_num: str
    document_type: Optional[str] = "other"
    county: Optional[str] = "montgomery"

async def init_browser():
    """Initialize browser with proper settings for OnBase"""
    global BROWSER, BROWSER_CONTEXT, PLAYWRIGHT
    
    if not BROWSER:
        logger.info("🌐 Initializing browser for OnBase...")
        PLAYWRIGHT = await async_playwright().start()
        
        BROWSER = await PLAYWRIGHT.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--allow-running-insecure-content',
                '--disable-blink-features=AutomationControlled',
                '--window-size=1920,1080'
            ]
        )
        
        BROWSER_CONTEXT = await BROWSER.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36',
            ignore_https_errors=True,
            java_script_enabled=True,
            accept_downloads=True
        )
        logger.info("✅ Browser initialized for OnBase")

async def debug_page_content(page):
    """Debug helper to log page structure"""
    try:
        url = page.url
        title = await page.title()
        logger.debug(f"📍 Current URL: {url}")
        logger.debug(f"📍 Page Title: {title}")
        
        all_images = await page.query_selector_all('img')
        logger.debug(f"🖼️ Found {len(all_images)} total images on page")
        
        for i, img in enumerate(all_images[:10]):
            try:
                src = await img.get_attribute('src')
                alt = await img.get_attribute('alt')
                class_name = await img.get_attribute('class')
                id_attr = await img.get_attribute('id')
                
                box = await img.bounding_box()
                width = box['width'] if box else 0
                height = box['height'] if box else 0
                
                logger.debug(f"  Image {i}: class='{class_name}', id='{id_attr}', alt='{alt}', "
                           f"dimensions={width}x{height}, src_preview='{src[:100] if src else 'None'}'")
            except:
                pass
        
        iframes = await page.query_selector_all('iframe')
        logger.debug(f"📋 Found {len(iframes)} iframes")
        for i, frame in enumerate(iframes):
            src = await frame.get_attribute('src')
            id_attr = await frame.get_attribute('id')
            logger.debug(f"  Iframe {i}: id='{id_attr}', src='{src[:100] if src else 'None'}'")
            
    except Exception as e:
        logger.error(f"Debug logging error: {e}")

async def extract_onbase_multipage_document(page) -> bytes:
    """Extract all pages from OnBase viewer handling iframe structure"""
    logger.info("🔍 Starting OnBase multi-page extraction...")
    
    await debug_page_content(page)
    
    try:
        await page.wait_for_timeout(5000)
        
        view_doc_frame = None
        try:
            logger.info("Waiting for ViewDocumentEx frame...")
            
            all_frames = page.frames
            logger.debug(f"Found {len(all_frames)} frames total")
            
            for frame in all_frames:
                frame_url = frame.url
                logger.debug(f"Frame URL: {frame_url}")
                
                if 'ViewDocumentEx' in frame_url or 'GuidDocumentSelect' in frame_url:
                    view_doc_frame = frame
                    logger.info(f"✅ Found document frame: {frame_url}")
                    break
        except Exception as e:
            logger.debug(f"Frame search error: {e}")
        
        logger.info("Waiting for document images to load via ImageProvider...")
        
        document_images = []
        image_urls = []
        
        async def capture_image_responses(response):
            try:
                url = response.url
                if 'ImageProvider.ashx' in url and 'getPage' in url:
                    logger.info(f"📸 Captured document page: {url}")
                    image_data = await response.body()
                    if image_data:
                        document_images.append(image_data)
                        image_urls.append(url)
            except Exception as e:
                logger.debug(f"Response capture error: {e}")
        
        page.on('response', capture_image_responses)
        
        await page.evaluate('''
            if (typeof RefreshDocument === 'function') RefreshDocument();
            if (typeof LoadDocument === 'function') LoadDocument();
            if (typeof ShowDocument === 'function') ShowDocument();
            window.scrollTo(0, document.body.scrollHeight);
        ''')
        
        await page.wait_for_timeout(3000)
        
        if len(document_images) == 0:
            logger.info("Searching DOM for document images...")
            
            image_strategies = [
                '''
                    let images = [];
                    const iframes = document.querySelectorAll('iframe');
                    for (let iframe of iframes) {
                        try {
                            const iframeDoc = iframe.contentDocument || iframe.contentWindow.document;
                            const iframeImages = iframeDoc.querySelectorAll('img');
                            for (let img of iframeImages) {
                                if (img.src && (img.src.includes('ImageProvider') || 
                                    img.src.includes('getPage') || 
                                    img.width > 200)) {
                                    images.push({
                                        src: img.src,
                                        width: img.width,
                                        height: img.height,
                                        alt: img.alt
                                    });
                                }
                            }
                        } catch(e) {}
                    }
                    return images;
                ''',
                '''
                    let canvases = [];
                    document.querySelectorAll('canvas').forEach(canvas => {
                        if (canvas.width > 200 && canvas.height > 200) {
                            canvases.push({
                                width: canvas.width,
                                height: canvas.height,
                                data: canvas.toDataURL('image/png')
                            });
                        }
                    });
                    document.querySelectorAll('iframe').forEach(iframe => {
                        try {
                            const iframeDoc = iframe.contentDocument || iframe.contentWindow.document;
                            iframeDoc.querySelectorAll('canvas').forEach(canvas => {
                                if (canvas.width > 200 && canvas.height > 200) {
                                    canvases.push({
                                        width: canvas.width,
                                        height: canvas.height,
                                        data: canvas.toDataURL('image/png')
                                    });
                                }
                            });
                        } catch(e) {}
                    });
                    return canvases;
                '''
            ]
            
            for strategy_code in image_strategies:
                try:
                    result = await page.evaluate(strategy_code)
                    logger.debug(f"Strategy result: {len(result) if result else 0} items found")
                    
                    if result and len(result) > 0:
                        logger.info(f"✅ Found {len(result)} document elements")
                        
                        for item in result:
                            if 'data' in item:
                                img_data = item['data'].split(',')[1]
                                img_bytes = base64.b64decode(img_data)
                                document_images.append(img_bytes)
                            elif 'src' in item and item['src']:
                                img_bytes = await download_image_from_page(page, item['src'])
                                if img_bytes:
                                    document_images.append(img_bytes)
                        
                        if len(document_images) > 0:
                            break
                            
                except Exception as e:
                    logger.debug(f"Strategy failed: {e}")
        
        if document_images:
            logger.info(f"Converting {len(document_images)} images to PDF...")
            
            pil_images = []
            for img_bytes in document_images:
                try:
                    img = Image.open(BytesIO(img_bytes))
                    if img.mode != 'RGB':
                        img = img.convert('RGB')
                    pil_images.append(img)
                except Exception as e:
                    logger.debug(f"Failed to process image: {e}")
            
            if pil_images:
                pdf_buffer = BytesIO()
                pil_images[0].save(
                    pdf_buffer,
                    'PDF',
                    save_all=True,
                    append_images=pil_images[1:] if len(pil_images) > 1 else [],
                    resolution=100.0
                )
                pdf_buffer.seek(0)
                pdf_bytes = pdf_buffer.read()
                logger.info(f"✅ Created PDF: {len(pdf_bytes)} bytes")
                return pdf_bytes
        
        logger.warning("No document images found, attempting viewer screenshot...")
        
        viewer_selectors = [
            '#DocSelectPage',
            '.document-viewer',
            '#documentContent',
            '.viewer-content',
            'iframe[src*="ViewDocumentEx"]',
            'iframe[src*="GuidDocument"]'
        ]
        
        for selector in viewer_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    logger.info(f"Taking screenshot of {selector}")
                    screenshot = await element.screenshot()
                    if screenshot and len(screenshot) > 1000:
                        img = Image.open(BytesIO(screenshot))
                        pdf_buffer = BytesIO()
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        img.save(pdf_buffer, 'PDF')
                        pdf_buffer.seek(0)
                        return pdf_buffer.read()
            except Exception as e:
                logger.debug(f"Screenshot of {selector} failed: {e}")
        
        logger.error("❌ No document content could be extracted")
        return None
        
    except Exception as e:
        logger.error(f"❌ Multi-page extraction failed: {e}", exc_info=True)
        return None

async def download_image_from_page(page, src: str) -> bytes:
    """Download image using page context to preserve cookies/auth"""
    try:
        if not src or src == 'about:blank':
            return None
            
        if src.startswith('//'):
            src = 'https:' + src
        elif src.startswith('/'):
            base_url = await page.evaluate('window.location.origin')
            src = base_url + src
        
        logger.debug(f"Downloading image: {src[:100]}")
        
        image_data = await page.evaluate('''
            async (url) => {
                try {
                    const response = await fetch(url);
                    const blob = await response.blob();
                    return new Promise((resolve) => {
                        const reader = new FileReader();
                        reader.onloadend = () => resolve(reader.result);
                        reader.readAsDataURL(blob);
                    });
                } catch(e) {
                    return null;
                }
            }
        ''', src)
        
        if image_data and image_data.startswith('data:'):
            img_data = image_data.split(',')[1]
            return base64.b64decode(img_data)
            
    except Exception as e:
        logger.debug(f"Failed to download image: {e}")
    
    return None

async def download_pdf_onbase(url: str) -> bytes:
    """Enhanced OnBase PDF downloader with multi-page support"""
    logger.info(f"📄 Starting OnBase download for URL: {url}")
    
    if not BROWSER_CONTEXT:
        await init_browser()
    
    page = await BROWSER_CONTEXT.new_page()
    
    page.on('console', lambda msg: logger.debug(f"Browser console: {msg.text}"))
    page.on('pageerror', lambda err: logger.error(f"Browser error: {err}"))
    
    try:
        pdf_data = None
        
        async def handle_response(response):
            nonlocal pdf_data
            try:
                content_type = response.headers.get('content-type', '').lower()
                url_lower = response.url.lower()
                
                logger.debug(f"Response: {response.status} {response.url[:100]} [{content_type}]")
                
                if 'application/pdf' in content_type or url_lower.endswith('.pdf'):
                    logger.info(f"🎯 Intercepted PDF response: {response.url}")
                    pdf_data = await response.body()
            except Exception as e:
                logger.debug(f"Response handler error: {e}")
        
        page.on('response', handle_response)
        
        logger.info(f"📄 Navigating to OnBase URL: {url}")
        
        response = await page.goto(url, wait_until='networkidle', timeout=60000)
        logger.info(f"Navigation complete: {response.status if response else 'No response'}")
        
        await page.wait_for_timeout(5000)
        
        if pdf_data and len(pdf_data) > 10000:
            logger.info("✅ Got direct PDF from response")
            return pdf_data
        
        pdf_data = await extract_onbase_multipage_document(page)
        
        if pdf_data and len(pdf_data) > 5000:
            logger.info(f"✅ Successfully extracted document: {len(pdf_data)} bytes")
            return pdf_data
        
        logger.warning("⚠️ Using fallback full-page capture")
        await page.evaluate('''
            const uiSelectors = ['.toolbar', '.header', '.navigation', '[class*="menu"]'];
            uiSelectors.forEach(sel => {
                document.querySelectorAll(sel).forEach(el => el.style.display = 'none');
            });
        ''')
        
        pdf_data = await page.pdf(
            format='Letter',
            print_background=True,
            display_header_footer=False,
            margin={'top': '0in', 'right': '0in', 'bottom': '0in', 'left': '0in'}
        )
        
        return pdf_data
            
    except Exception as e:
        logger.error(f"OnBase download error: {e}", exc_info=True)
        raise
    finally:
        await page.close()

async def store_to_s3(document_id: str, charter_num: str, pdf_content: bytes, county: str = "montgomery") -> Dict[str, str]:
    """Store PDF to S3"""
    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        s3_key = f"{county}/{timestamp}/{charter_num}/{document_id}.pdf"
        
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
        
        s3_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}"
        logger.info(f"✅ Uploaded to S3: {s3_key}")
        
        return {"s3_key": s3_key, "s3_url": s3_url}
        
    except Exception as e:
        logger.error(f"S3 upload error: {e}")
        return None

# API Endpoints
@app.on_event("startup")
async def startup_event():
    logger.info(f"🚀 Starting OnBase PDF Downloader v{VERSION}")
    await init_browser()

@app.on_event("shutdown")
async def shutdown_event():
    global BROWSER, BROWSER_CONTEXT, PLAYWRIGHT
    if BROWSER_CONTEXT:
        await BROWSER_CONTEXT.close()
    if BROWSER:
        await BROWSER.close()
    if PLAYWRIGHT:
        await PLAYWRIGHT.stop()
    logger.info("👋 Service stopped")

@app.post("/download")
async def download_endpoint(request: DownloadRequest):
    try:
        logger.info(f"📥 Download request: doc_id={request.document_id}, url={request.document_url[:100]}")
        
        pdf_data = await download_pdf_onbase(request.document_url)
        
        if not pdf_data:
            logger.error("No PDF data received")
            raise HTTPException(status_code=404, detail="Could not extract document")
        
        s3_data = await store_to_s3(
            request.document_id,
            request.charter_num,
            pdf_data,
            request.county
        )
        
        return {
            "status": "success",
            "document_id": request.document_id,
            "s3_path": s3_data["s3_key"] if s3_data else None,
            "size": len(pdf_data),
            "pages": "multi"
        }
        
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "healthy", "version": VERSION}

# This is required for uvicorn to find the app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
