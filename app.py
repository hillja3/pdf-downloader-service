# Add this function to your app.py to properly handle OnBase iframe structure

async def extract_onbase_multipage_document(page) -> bytes:
    """
    Extract all pages from OnBase viewer handling iframe structure
    """
    logger.info("🔍 Starting OnBase multi-page extraction...")
    
    # First, debug the page
    await debug_page_content(page)
    
    try:
        # Wait for OnBase viewer to load
        await page.wait_for_timeout(5000)  # Increased wait time
        
        # Method 1: Check if document is loaded in main ViewDocumentEx iframe
        view_doc_frame = None
        try:
            # Wait for ViewDocumentEx to appear in network traffic or DOM
            logger.info("Waiting for ViewDocumentEx frame...")
            
            # OnBase often loads documents in a complex iframe structure
            # Try to find the actual document viewer frame
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
        
        # Method 2: Wait for ImageProvider.ashx images to load
        logger.info("Waiting for document images to load via ImageProvider...")
        
        # Intercept image responses
        document_images = []
        image_urls = []
        
        # Set up response handler to catch document images
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
        
        # Trigger document rendering if needed
        await page.evaluate('''
            // Try to trigger OnBase document rendering
            if (typeof RefreshDocument === 'function') RefreshDocument();
            if (typeof LoadDocument === 'function') LoadDocument();
            if (typeof ShowDocument === 'function') ShowDocument();
            
            // Scroll to trigger lazy loading
            window.scrollTo(0, document.body.scrollHeight);
        ''')
        
        # Wait for images to load
        await page.wait_for_timeout(3000)
        
        # Method 3: Extract visible document images from the DOM
        if len(document_images) == 0:
            logger.info("Searching DOM for document images...")
            
            # Try multiple strategies to find document images
            image_strategies = [
                # Strategy A: Find images in iframes
                '''
                    let images = [];
                    // Check all iframes
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
                
                # Strategy B: Find canvas elements (OnBase might render to canvas)
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
                    // Check iframes for canvas too
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
                ''',
                
                # Strategy C: Get all images with ImageProvider in src
                '''
                    let allImages = Array.from(document.querySelectorAll('img'));
                    // Also check all iframes
                    document.querySelectorAll('iframe').forEach(iframe => {
                        try {
                            const iframeDoc = iframe.contentDocument || iframe.contentWindow.document;
                            allImages = allImages.concat(Array.from(iframeDoc.querySelectorAll('img')));
                        } catch(e) {}
                    });
                    
                    return allImages
                        .filter(img => img.src && (
                            img.src.includes('ImageProvider') || 
                            img.src.includes('docpop') ||
                            img.src.includes('getPage')
                        ))
                        .map(img => ({
                            src: img.src,
                            width: img.width,
                            height: img.height
                        }));
                '''
            ]
            
            for strategy_code in image_strategies:
                try:
                    result = await page.evaluate(strategy_code)
                    logger.debug(f"Strategy result: {len(result) if result else 0} items found")
                    
                    if result and len(result) > 0:
                        logger.info(f"✅ Found {len(result)} document elements")
                        
                        # Process the results
                        for item in result:
                            if 'data' in item:  # Canvas data
                                # Convert base64 to bytes
                                img_data = item['data'].split(',')[1]
                                img_bytes = base64.b64decode(img_data)
                                document_images.append(img_bytes)
                            elif 'src' in item and item['src']:  # Image src
                                # Download the image
                                img_bytes = await download_image_from_page(page, item['src'])
                                if img_bytes:
                                    document_images.append(img_bytes)
                        
                        if len(document_images) > 0:
                            break
                            
                except Exception as e:
                    logger.debug(f"Strategy failed: {e}")
        
        # Method 4: Check for a PDF viewer embed
        pdf_embed = await page.query_selector('embed[type="application/pdf"], object[type="application/pdf"]')
        if pdf_embed:
            src = await pdf_embed.get_attribute('src')
            if src:
                logger.info(f"Found embedded PDF: {src}")
                # Download the PDF directly
                pdf_response = await page.evaluate('''
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
                
                if pdf_response and pdf_response.startswith('data:'):
                    pdf_data = pdf_response.split(',')[1]
                    return base64.b64decode(pdf_data)
        
        # Convert collected images to PDF
        if document_images:
            logger.info(f"Converting {len(document_images)} images to PDF...")
            from PIL import Image
            import io
            
            pil_images = []
            for img_bytes in document_images:
                try:
                    img = Image.open(io.BytesIO(img_bytes))
                    if img.mode != 'RGB':
                        img = img.convert('RGB')
                    pil_images.append(img)
                except Exception as e:
                    logger.debug(f"Failed to process image: {e}")
            
            if pil_images:
                pdf_buffer = io.BytesIO()
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
        
        # Last resort: Take screenshots of the viewer area
        logger.warning("No document images found, attempting viewer screenshot...")
        
        # Find the main content area
        viewer_selectors = [
            '#DocSelectPage',  # The iframe we see in logs
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
                        img = Image.open(io.BytesIO(screenshot))
                        pdf_buffer = io.BytesIO()
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
            
        # Handle relative URLs
        if src.startswith('//'):
            src = 'https:' + src
        elif src.startswith('/'):
            base_url = await page.evaluate('window.location.origin')
            src = base_url + src
        
        logger.debug(f"Downloading image: {src[:100]}")
        
        # Download using page context
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
