FROM python:3.11-slim

# Working directory
WORKDIR /app

# Install ffmpeg + build tools
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy files
COPY . .

# Upgrade pip
RUN pip install --no-cache-dir -U pip

# Install requirements
RUN pip install --no-cache-dir \
    pyrogram \
    tgcrypto \
    motor \
    httpx \
    requests \
    flask \
    uvloop

# Expose flask port
EXPOSE 5000

# Start bot
CMD ["python", "terabnr.py"]
