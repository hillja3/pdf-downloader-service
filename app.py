# app.py - ENHANCED DEBUG VERSION for OnBase troubleshooting
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
import json

load_dotenv()

# Enhanced logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

VERSION = "5.1-DEBUG"
logger.info(f"PDF Downloader Service v{VERSION} starting with ENHANCED DEBUG...")

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
        # Get page URL and title
        url = page.url
        title = await page.title()
        logger.debug(f"📍 Current URL: {url}")
        logger.debug(f"📍 Page Title: {title}")
        
        # Get all images on page
        all_images = await page.query_selector_all('img')
        logger.debug(f"🖼️ Found {len(all_images)} total images on page")
        
        # Log details about each image
        for i, img in enumerate(all_images[:10]):  # First 10 images
            try:
                src = await img.get_attribute('src')
                alt = await img.get_attribute('alt')
                class_name = await img.get_attribute('class')
                id_attr = await img.get_attribute('id')
                
                # Get dimensions
                box = await img.bounding_box()
                width = box['width'] if box else 0
                height = box['height'] if box else 0
                
                logger.debug(f"  Image {i}: class='{class_name}', id='{id_attr}', alt='{alt}', "
                           f"dimensions={width}x{height}, src_preview='{src[:100] if src else 'None'}'")
            except:
                pass
        
        # Get all iframes
        iframes = await page.query_selector_all('iframe')
        logger.debug(f"📋 Found {len(iframes)} iframes")
        for i, frame in enumerate(iframes):
            src = await frame.get_attribute('src')
            id_attr = await frame.get_attribute('id')
            logger.debug(f"  Iframe {i}: id='{id_attr}', src='{src[:100] if src else 'None'}'")
        
        # Log page HTML structure (first 2000 chars)
        html = await page.content()
        logger.debug(f"📄 HTML preview (first 2000 chars): {html[:2000]}")
        
        # Check for OnBase specific elements
        onbase_elements = {
            'docViewer': await page.query_selector('#docViewer'),
            'documentViewer': await page.query_selector('#documentViewer'),
            'pageContainer': await page.query_selector('#pageContainer'),
            'viewer-content': await page.query_selector('.viewer-content'),
            'document-container': await page.query_selector('.document-container'),
            'UnityForm': await page.query_selector('[name*="UnityForm"]'),
            'docpop': await page.query_selector('[src*="docpop"]')
        }
        
        logger.debug("🔍 OnBase element detection:")
        for name, element in onbase_elements.items():
            logger.debug(f"  {name}: {'✅ FOUND' if element else '❌ NOT FOUND'}")
            
    except Exception as e:
        logger.error(f"Debug logging error: {e}")

async def extract_onbase_multipage_document(page) -> bytes:
    """
    Extract all pages from OnBase viewer and combine into single PDF
    """
    logger.info("🔍 Starting OnBase multi-page extraction...")
    
    # First, debug the page
    await debug_page_content(page)
    
    try:
        # Wait for OnBase viewer to load
        await page.wait_for_timeout(3000)
        
        # Try to detect if we're in an iframe-based viewer
        main_frame = page
        iframe_viewer = await page.query_selector('iframe[src*="docpop"], iframe[src*="UnityForm"], iframe#docViewer')
        if iframe_viewer:
            logger.info("📋 Found iframe viewer, switching context...")
            frame = await iframe_viewer.content_frame()
            if frame:
                main_frame = frame
                logger.info("✅ Switched to iframe context")
                await debug_page_content(main_frame)
        
        # Method 1: Try to find page count from UI
        total_pages = await get_total_pages(main_frame)
        logger.info(f"📄 Document has {total_pages} pages")
        
        # Collect all page images
        page_images = []
        
        for page_num in range(1, total_pages + 1):
            logger.info(f"📖 Extracting page {page_num} of {total_pages}")
            
            # Navigate to specific page
            await navigate_to_page(main_frame, page_num)
            await main_frame.wait_for_timeout(1500)  # Wait for page to render
            
            # Extract the current page image
            page_image = await extract_current_page_image(main_frame)
            
            if page_image:
                page_images.append(page_image)
                logger.info(f"✅ Successfully extracted page {page_num}")
            else:
                logger.warning(f"⚠️ Could not extract page {page_num}")
                
                # Try alternative extraction methods
                logger.debug("Trying alternative extraction methods...")
                
                # Method A: Screenshot the content area
                content_area = await main_frame.query_selector('.document-viewer, #documentContent, .viewer-content, #pageContainer')
                if content_area:
                    logger.debug("Taking screenshot of content area...")
                    screenshot = await content_area.screenshot()
                    if screenshot:
                        page_image = Image.open(BytesIO(screenshot))
                        page_images.append(page_image)
                        logger.info(f"✅ Got page {page_num} via content screenshot")
                        continue
                
                # Method B: Full page screenshot
                logger.debug("Taking full page screenshot...")
                screenshot = await main_frame.screenshot(full_page=True)
                if screenshot:
                    page_image = Image.open(BytesIO(screenshot))
                    page_images.append(page_image)
                    logger.info(f"✅ Got page {page_num} via full screenshot")
        
        if page_images:
            # Convert all images to a single PDF
            pdf_bytes = combine_images_to_pdf(page_images)
            logger.info(f"✅ Created PDF with {len(page_images)} pages: {len(pdf_bytes)} bytes")
            return pdf_bytes
        else:
            logger.error("❌ No pages could be extracted")
            return None
            
    except Exception as e:
        logger.error(f"❌ Multi-page extraction failed: {e}", exc_info=True)
        return None

async def get_total_pages(frame) -> int:
    """Get total number of pages in document"""
    try:
        logger.debug("Detecting page count...")
        
        # Look for "Page X of Y" text
        page_indicators = [
            'text=/Page.*\\d+.*of.*\\d+/i',
            'text=/\\d+.*of.*\\d+/i',
            '[class*="page-count"]',
            '[class*="total-pages"]',
            '[class*="pageNumber"]',
            '.page-info',
            '#pageInfo'
        ]
        
        for indicator in page_indicators:
            try:
                element = await frame.query_selector(indicator)
                if element:
                    text = await element.text_content()
                    logger.debug(f"Found page indicator: '{text}'")
                    # Extract total pages from "Page 1 of 3" format
                    match = re.search(r'of\s*(\d+)', text, re.IGNORECASE)
                    if match:
                        pages = int(match.group(1))
                        logger.debug(f"Extracted page count: {pages}")
                        return pages
            except Exception as e:
                logger.debug(f"Failed to check indicator {indicator}: {e}")
                continue
        
        # Fallback: Count thumbnail images
        thumbnails = await frame.query_selector_all('img[class*="thumb"], .thumbnail img, [class*="preview"] img')
        if thumbnails and len(thumbnails) > 0:
            logger.debug(f"Found {len(thumbnails)} thumbnail images")
            return len(thumbnails)
        
        # Check for page navigation buttons
        page_buttons = await frame.query_selector_all('button[data-page], a[data-page], [class*="page-button"]')
        if page_buttons:
            logger.debug(f"Found {len(page_buttons)} page buttons")
            return len(page_buttons)
        
        logger.warning("Could not determine page count, defaulting to 1")
        return 1
        
    except Exception as e:
        logger.error(f"Error getting page count: {e}", exc_info=True)
        return 1

async def navigate_to_page(frame, page_num: int):
    """Navigate to a specific page in OnBase viewer"""
    logger.debug(f"Navigating to page {page_num}...")
    
    try:
        # Method 1: Click on page navigation buttons
        nav_selectors = [
            f'[aria-label*="Page {page_num}"]',
            f'button:has-text("{page_num}")',
            f'a:has-text("{page_num}")',
            f'.page-{page_num}',
            f'[data-page="{page_num}"]',
            f'#page{page_num}'
        ]
        
        for selector in nav_selectors:
            try:
                element = await frame.query_selector(selector)
                if element:
                    logger.debug(f"Clicking navigation element: {selector}")
                    await element.click()
                    return
            except Exception as e:
                logger.debug(f"Nav selector {selector} failed: {e}")
                continue
        
        # Method 2: Click on thumbnail
        thumbnails = await frame.query_selector_all('.thumbnail, [class*="thumb"], img[class*="thumb"]')
        if thumbnails and len(thumbnails) >= page_num:
            logger.debug(f"Clicking thumbnail {page_num}")
            await thumbnails[page_num - 1].click()
            return
            
        # Method 3: Use JavaScript navigation
        logger.debug("Trying JavaScript navigation...")
        js_result = await frame.evaluate(f'''
            // Try OnBase navigation functions
            try {{
                if (typeof goToPage === 'function') {{ goToPage({page_num}); return 'goToPage'; }}
                if (typeof navigateToPage === 'function') {{ navigateToPage({page_num}); return 'navigateToPage'; }}
                if (typeof setCurrentPage === 'function') {{ setCurrentPage({page_num}); return 'setCurrentPage'; }}
                if (window.viewer && window.viewer.goToPage) {{ window.viewer.goToPage({page_num}); return 'viewer.goToPage'; }}
                return 'none';
            }} catch(e) {{
                return 'error: ' + e.toString();
            }}
        ''')
        logger.debug(f"JS navigation result: {js_result}")
        
        # Method 4: Use keyboard navigation
        if page_num > 1:
            logger.debug(f"Using keyboard navigation (PageDown x {page_num-1})")
            for _ in range(page_num - 1):
                await frame.keyboard.press('PageDown')
                await frame.wait_for_timeout(500)
                
    except Exception as e:
        logger.error(f"Navigation to page {page_num} failed: {e}", exc_info=True)

async def extract_current_page_image(frame):
    """Extract the currently visible page as an image"""
    logger.debug("Extracting current page image...")
    
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
            'img[src*="GetPage"]',
            'img[src*="getpage"]',
            '.document-container img',
            '#docImage',
            '.docImage',
            'img[alt*="Page"]',
            'img[alt*="Document"]'
        ]
        
        for selector in image_selectors:
            try:
                logger.debug(f"Checking selector: {selector}")
                img_element = await frame.query_selector(selector)
                
                if img_element:
                    # Check if image is visible and has reasonable size
                    is_visible = await img_element.is_visible()
                    logger.debug(f"  Image visible: {is_visible}")
                    
                    if not is_visible:
                        continue
                    
                    box = await img_element.bounding_box()
                    if box:
                        logger.debug(f"  Image dimensions: {box['width']}x{box['height']}")
                        if box['width'] > 100 and box['height'] > 100:
                            # Get image attributes for debugging
                            src = await img_element.get_attribute('src')
                            alt = await img_element.get_attribute('alt')
                            class_name = await img_element.get_attribute('class')
                            logger.debug(f"  Found viable image: class='{class_name}', alt='{alt}', src_preview='{src[:100] if src else 'None'}'")
                            
                            # Method 1: Get image source and download
                            if src:
                                image_data = await download_image_from_src(frame, src)
                                if image_data:
                                    logger.debug(f"  ✅ Downloaded image from source")
                                    return image_data
                            
                            # Method 2: Screenshot the image element
                            logger.debug("  Attempting element screenshot...")
                            img_bytes = await img_element.screenshot()
                            if img_bytes and len(img_bytes) > 1000:
                                logger.debug(f"  ✅ Got screenshot: {len(img_bytes)} bytes")
                                return Image.open(BytesIO(img_bytes))
            except Exception as e:
                logger.debug(f"  Selector {selector} error: {e}")
                continue
        
        logger.warning("No viable document image found with selectors")
        
        # Fallback: Screenshot the main content area
        content_selectors = [
            '.document-viewer',
            '#documentContent',
            '.viewer-content',
            '#pageContainer',
            '.page-container',
            '#viewerContainer'
        ]
        
        for selector in content_selectors:
            try:
                content_area = await frame.query_selector(selector)
                if content_area:
                    logger.debug(f"Fallback: screenshotting {selector}")
                    screenshot = await content_area.screenshot()
                    if screenshot:
                        return Image.open(BytesIO(screenshot))
            except:
                continue
                
    except Exception as e:
        logger.error(f"Failed to extract current page: {e}", exc_info=True)
        
    return None

async def download_image_from_src(frame, src: str):
    """Download image from source URL"""
    logger.debug(f"Downloading image from: {src[:100]}")
    
    try:
        if src.startswith('data:image'):
            # Handle base64 encoded images
            logger.debug("Processing base64 image...")
            img_data = src.split(',')[1]
            img_bytes = base64.b64decode(img_data)
            return Image.open(BytesIO(img_bytes))
        else:
            # Download from URL
            if src.startswith('//'):
                src = 'https:' + src
            elif src.startswith('/'):
                # Get base URL from page
                base_url = await frame.evaluate('window.location.origin')
                src = base_url + src
                logger.debug(f"Full URL: {src}")
            
            # Use page context to download (preserves cookies/auth)
            logger.debug("Fetching via page context...")
            response = await frame.evaluate('''
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
                        return 'error: ' + e.toString();
                    }
                }
            ''', src)
            
            if response and response.startswith('data:image'):
                logger.debug("Successfully fetched image data")
                img_data = response.split(',')[1]
                img_bytes = base64.b64decode(img_data)
                return Image.open(BytesIO(img_bytes))
            else:
                logger.debug(f"Fetch failed: {response[:100] if response else 'None'}")
                
    except Exception as e:
        logger.error(f"Failed to download image from {src[:50]}: {e}")
    
    return None

def combine_images_to_pdf(images: List[Image.Image]) -> bytes:
    """Combine multiple images into a single PDF"""
    if not images:
        return b''
    
    logger.debug(f"Combining {len(images)} images to PDF...")
    
    # Convert all images to RGB (PDF doesn't support RGBA)
    rgb_images = []
    for i, img in enumerate(images):
        logger.debug(f"  Processing image {i+1}: mode={img.mode}, size={img.size}")
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
    pdf_bytes = pdf_buffer.read()
    logger.debug(f"Created PDF: {len(pdf_bytes)} bytes")
    return pdf_bytes

async def download_pdf_onbase(url: str) -> bytes:
    """
    Enhanced OnBase PDF downloader with multi-page support
    """
    logger.info(f"📄 Starting OnBase download for URL: {url}")
    
    if not BROWSER_CONTEXT:
        await init_browser()
    
    page = await BROWSER_CONTEXT.new_page()
    
    # Enable console logging
    page.on('console', lambda msg: logger.debug(f"Browser console: {msg.text}"))
    page.on('pageerror', lambda err: logger.error(f"Browser error: {err}"))
    
    try:
        pdf_data = None
        
        # Set up response interceptor for direct PDF downloads
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
        
        # Navigate with extended timeout
        response = await page.goto(url, wait_until='networkidle', timeout=60000)
        logger.info(f"Navigation complete: {response.status if response else 'No response'}")
        
        # Wait for OnBase viewer to load
        logger.debug("Waiting for OnBase viewer to load...")
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
        logger.info("Attempting download button method...")
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
        logger.error(f"OnBase download error: {e}", exc_info=True)
        raise
    finally:
        await page.close()

async def try_onbase_download_button(page) -> Optional[bytes]:
    """Try to use OnBase's built-in download/print functionality"""
    logger.debug("Trying OnBase download/print buttons...")
    
    try:
        download_selectors = [
            'button[title*="Download"]',
            'button[title*="Print"]',
            'a[title*="Download"]',
            'a[title*="Print"]',
            '[aria-label*="Download"]',
            '[aria-label*="Print"]',
            'img[alt*="Download"]',
            'img[alt*="Print"]',
            '[onclick*="download"]',
            '[onclick*="print"]'
        ]
        
        for selector in download_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    logger.debug(f"Found download/print element: {selector}")
                    
                    # Set up download handler
                    download_data = None
                    
                    async def handle_download(download):
                        nonlocal download_data
                        logger.debug(f"Download triggered: {download.suggested_filename}")
                        download_data = await download.read()
                    
                    page.on('download', handle_download)
                    
                    await element.click()
                    await page.wait_for_timeout(3000)
                    
                    if download_data:
                        logger.info("✅ Got PDF from download button")
                        return download_data
            except Exception as e:
                logger.debug(f"Button {selector} failed: {e}")
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
        logger.info(f"📥 Download request: doc_id={request.document_id}, url={request.document_url[:100]}")
        
        # Download PDF with OnBase-specific handling
        pdf_data = await download_pdf_onbase(request.document_url)
        
        if not pdf_data:
            logger.error("No PDF data received")
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
            "pages": "multi"
        }
        
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "healthy", "version": VERSION}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
