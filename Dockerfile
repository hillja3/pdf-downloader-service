FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl ca-certificates fonts-liberation libasound2 libatspi2.0-0 \
    libatk1.0-0 libatk-bridge2.0-0 libatomic1 libbrotli1 libc6 libcap2 \
    libcups2 libdbus-1-3 libdrm2 libexpat1 libgbm1 libglib2.0-0 \
    libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 libx11-6 libx11-xcb1 \
    libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 \
    libxi6 libxrandr2 libxrender1 libxss1 libxtst6 lsb-release \
    wget xdg-utils \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install chromium

COPY . .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
