import os
from fastapi import FastAPI
from pydantic import BaseModel
from supabase import create_client, Client
from datetime import datetime
from playwright.async_api import async_playwright
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="PDF Downloader Service")

supabase: Client = create_client(
    os.getenv("SUPABASE_URL", ""),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
)

class DownloadRequest(BaseModel):
    document_id: str
    document_url: str
    charter_num: str

@app.get("/")
def root():
    return {"status": "PDF Downloader Running", "version": "1.0"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/download")
async def download_pdf(request: DownloadRequest):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            logger.info(f"Downloading from: {request.document_url}")
            await page.goto(request.document_url, wait_until='domcontentloaded')
            await page.wait_for_timeout(5000)
            
            # Generate PDF
            pdf_data = await page.pdf(format='A4')
            await browser.close()
            
            # Save to Supabase
            storage_path = f"montgomery/2025/01/{request.document_id}.pdf"
            
            response = supabase.storage.from_("county-records").upload(
                storage_path,
                pdf_data,
                file_options={"content-type": "application/pdf", "upsert": True}
            )
            
            logger.info(f"Uploaded to: {storage_path}")
            
            return {
                "status": "success",
                "document_id": request.document_id,
                "storage_path": storage_path,
                "size": len(pdf_data)
            }
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {"status": "error", "message": str(e)}
