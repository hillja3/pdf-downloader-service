# app.py - FIXED VERSION for OnBase multi-page document extraction
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
import requests

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "5.0-ONBASE-MULTIPAGE"
logger.info(f"PDF Downloader Service v{VERSION} starting...")

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
        
        # Use Chromium with specific settings for OnBase
        BROWSER = await PLAYWRIGHT.chromium.launch(
            headless=True,  # Can run headless now that we're extracting properly
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

async def extract_onbase_multipage_document(page) -> bytes:
    """
    Extract all pages from OnBase viewer and combine into single PDF
    """
    logger.info("🔍 Starting OnBase multi-page extraction...")
    
    try:
        # Wait for OnBase viewer to load
        await page.wait_for_timeout(3000)
        
        # Method 1: Try to find page count from UI
        total_pages = await get_total_pages(page)
        logger.info(f"📄 Document has {total_pages} pages")
        
        # Collect all page images
        page_images = []
        
        for page_num in range(1, total_pages + 1):
            logger.info(f"📖 Extracting page {page_num} of {total_pages}")
            
            # Navigate to specific page
            await navigate_to_page(page, page_num)
            await page.wait_for_timeout(1500)  # Wait for page to render
            
            # Extract the current page image
            page_image = await extract_current_page_image(page)
            
            if page_image:
                page_images.append(page_image)
            else:
                logger.warning(f"⚠️ Could not extract page {page_num}")
        
        if page_images:
            # Convert all images to a single PDF
            pdf_bytes = combine_images_to_pdf(page_images)
            logger.info(f"✅ Created PDF with {len(page_images)} pages: {len(pdf_bytes)} bytes")
            return pdf_bytes
        else:
            logger.error("❌ No pages could be extracted")
            return None
            
    except Exception as e:
        logger.error(f"❌ Multi-page extraction failed: {e}")
        return None

async def get_total_pages(page) -> int:
    """Get total number of pages in document"""
    try:
        # Look for "Page X of Y" text
        page_indicators = [
            'text=/Page.*\\d+.*of.*\\d+/i',
            'text=/\\d+.*of.*\\d+/i',
            '[class*="page-count"]',
            '[class*="total-pages"]'
        ]
        
        for indicator in page_indicators:
            try:
                element = await page.query_selector(indicator)
                if element:
                    text = await element.text_content()
                    # Extract total pages from "Page 1 of 3" format
                    match = re.search(r'of\s*(\d+)', text, re.IGNORECASE)
                    if match:
                        return int(match.group(1))
            except:
                continue
        
        # Fallback: Count thumbnail images
        thumbnails = await page.query_selector_all('img[class*="thumb"], .thumbnail img, [class*="preview"] img')
        if thumbnails and len(thumbnails) > 0:
            return len(thumbnails)
        
        # Default to 1 if we can't determine
        return 1
        
    except Exception as e:
        logger.error(f"Error getting page count: {e}")
        return 1

async def navigate_to_page(page, page_num: int):
    """Navigate to a specific page in OnBase viewer"""
    try:
        # Method 1: Click on page navigation buttons
        nav_selectors = [
            f'[aria-label*="Page {page_num}"]',
            f'button:has-text("{page_num}")',
            f'a:has-text("{page_num}")',
            f'.page-{page_num}',
            f'[data-page="{page_num}"]'
        ]
        
        for selector in nav_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    await element.click()
                    return
            except:
                continue
        
        # Method 2: Click on thumbnail
        thumbnails = await page.query_selector_all('.thumbnail, [class*="thumb"]')
        if thumbnails and len(thumbnails) >= page_num:
            await thumbnails[page_num - 1].click()
            return
            
        # Method 3: Use JavaScript navigation
        await page.evaluate(f'''
            // Try OnBase navigation functions
            if (typeof goToPage === 'function') goToPage({page_num});
            else if (typeof navigateToPage === 'function') navigateToPage({page_num});
            else if (typeof setCurrentPage === 'function') setCurrentPage({page_num});
            else if (window.viewer && window.viewer.goToPage) window.viewer.goToPage({page_num});
        ''')
        
        # Method 4: Use keyboard navigation
        if page_num > 1:
            for _ in range(page_num - 1):
                await page.keyboard.press('PageDown')
                await page.wait_for_timeout(500)
                
    except Exception as e:
        logger.debug(f"Navigation to page {page_num} failed: {e}")

async def extract_current_page_image(page):
    """Extract the currently visible page as an image"""
    try:
        # Find the main document image container
        image_selectors = [
            '#documentViewer img',
            '.document-page img',
            '.page-image img',
            'img[class*="page"]',
            '.viewer-content img',
            '#pageContainer img',
            'img[src*="docpop"]',
            'img[src*="DocPop"]',
            '.document-container img'
        ]
        
        for selector in image_selectors:
            try:
                img_element = await page.query_selector(selector)
                if img_element:
                    # Check if image is visible and has reasonable size
                    is_visible = await img_element.is_visible()
                    if not is_visible:
                        continue
                    
                    box = await img_element.bounding_box()
                    if box and box['width'] > 100 and box['height'] > 100:
                        # Method 1: Get image source and download
                        src = await img_element.get_attribute('src')
                        if src:
                            image_data = await download_image_from_src(page, src)
                            if image_data:
                                return image_data
                        
                        # Method 2: Screenshot the image element
                        img_bytes = await img_element.screenshot()
                        if img_bytes and len(img_bytes) > 1000:
                            return Image.open(BytesIO(img_bytes))
            except:
                continue
        
        # Fallback: Screenshot the main content area
        content_area = await page.query_selector('.document-viewer, #documentContent, .viewer-content')
        if content_area:
            screenshot = await content_area.screenshot()
            if screenshot:
                return Image.open(BytesIO(screenshot))
                
    except Exception as e:
        logger.error(f"Failed to extract current page: {e}")
        
    return None

async def download_image_from_src(page, src: str):
    """Download image from source URL"""
    try:
        if src.startswith('data:image'):
            # Handle base64 encoded images
            img_data = src.split(',')[1]
            img_bytes = base64.b64decode(img_data)
            return Image.open(BytesIO(img_bytes))
        else:
            # Download from URL
            if src.startswith('//'):
                src = 'https:' + src
            elif src.startswith('/'):
                # Get base URL from page
                base_url = await page.evaluate('window.location.origin')
                src = base_url + src
            
            # Use page context to download (preserves cookies/auth)
            response = await page.evaluate('''
                async (url) => {
                    const response = await fetch(url);
                    const blob = await response.blob();
                    return new Promise((resolve) => {
                        const reader = new FileReader();
                        reader.onloadend = () => resolve(reader.result);
                        reader.readAsDataURL(blob);
                    });
                }
            ''', src)
            
            if response and response.startswith('data:image'):
                img_data = response.split(',')[1]
                img_bytes = base64.b64decode(img_data)
                return Image.open(BytesIO(img_bytes))
                
    except Exception as e:
        logger.debug(f"Failed to download image from {src}: {e}")
    
    return None

def combine_images_to_pdf(images: List[Image.Image]) -> bytes:
    """Combine multiple images into a single PDF"""
    if not images:
        return b''
    
    # Convert all images to RGB (PDF doesn't support RGBA)
    rgb_images = []
    for img in images:
        if img.mode != 'RGB':
            rgb_img = Image.new('RGB', img.size, (255, 255, 255))
            rgb_img.paste(img, mask=img.split()[3] if img.mode == 'RGBA' else None)
            rgb_images.append(rgb_img)
        else:
            rgb_images.append(img)
    
    # Save as PDF
    pdf_buffer = BytesIO()
    rgb_images[0].save(
        pdf_buffer, 
        'PDF', 
        save_all=True, 
        append_images=rgb_images[1:] if len(rgb_images) > 1 else [],
        resolution=100.0
    )
    
    pdf_buffer.seek(0)
    return pdf_buffer.read()

async def download_pdf_onbase(url: str) -> bytes:
    """
    Enhanced OnBase PDF downloader with multi-page support
    """
    if not BROWSER_CONTEXT:
        await init_browser()
    
    page = await BROWSER_CONTEXT.new_page()
    
    try:
        pdf_data = None
        
        # Set up response interceptor for direct PDF downloads
        async def handle_response(response):
            nonlocal pdf_data
            try:
                content_type = response.headers.get('content-type', '').lower()
                url_lower = response.url.lower()
                
                if 'application/pdf' in content_type or url_lower.endswith('.pdf'):
                    logger.info(f"🎯 Intercepted PDF response: {response.url}")
                    pdf_data = await response.body()
            except Exception as e:
                logger.debug(f"Response handler error: {e}")
        
        page.on('response', handle_response)
        
        logger.info(f"📄 Navigating to OnBase URL: {url}")
        
        # Navigate with extended timeout
        await page.goto(url, wait_until='networkidle', timeout=60000)
        
        # Wait for OnBase viewer to load
        await page.wait_for_timeout(5000)
        
        # Check if we got a direct PDF
        if pdf_data and len(pdf_data) > 10000:
            logger.info("✅ Got direct PDF from response")
            return pdf_data
        
        # Try multi-page extraction
        pdf_data = await extract_onbase_multipage_document(page)
        
        if pdf_data and len(pdf_data) > 5000:
            logger.info(f"✅ Successfully extracted multi-page document: {len(pdf_data)} bytes")
            return pdf_data
        
        # Last resort: Try to trigger print/download
        pdf_data = await try_onbase_download_button(page)
        
        if pdf_data and len(pdf_data) > 5000:
            return pdf_data
        
        # Final fallback: Full page PDF (but try to hide UI elements first)
        logger.warning("⚠️ Using fallback full-page capture")
        await page.evaluate('''
            // Hide OnBase UI elements
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
        logger.error(f"OnBase download error: {e}")
        raise
    finally:
        await page.close()

async def try_onbase_download_button(page) -> Optional[bytes]:
    """Try to use OnBase's built-in download/print functionality"""
    try:
        download_selectors = [
            'button[title*="Download"]',
            'button[title*="Print"]',
            'a[title*="Download"]',
            'a[title*="Print"]',
            '[aria-label*="Download"]',
            '[aria-label*="Print"]',
            'img[alt*="Download"]',
            'img[alt*="Print"]'
        ]
        
        for selector in download_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    # Set up download handler
                    download_data = None
                    
                    async def handle_download(download):
                        nonlocal download_data
                        download_data = await download.read()
                    
                    page.on('download', handle_download)
                    
                    await element.click()
                    await page.wait_for_timeout(3000)
                    
                    if download_data:
                        logger.info("✅ Got PDF from download button")
                        return download_data
            except:
                continue
                
    except Exception as e:
        logger.debug(f"Download button attempt failed: {e}")
    
    return None

# Storage functions remain the same
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
        # Download PDF with OnBase-specific handling
        pdf_data = await download_pdf_onbase(request.document_url)
        
        if not pdf_data:
            raise HTTPException(status_code=404, detail="Could not extract document")
        
        # Store to S3
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
            "pages": "multi"  # Indicator that we handled multi-page
        }
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "healthy", "version": VERSION}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
