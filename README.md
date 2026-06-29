# TeraBox Downloader Bot (`terabnr.py`)

A high-speed Telegram bot that downloads videos from TeraBox links and delivers them to users via Telegram.

---

## Features

- Fetches and streams TeraBox HLS (m3u8) videos directly to Telegram
- Fast DB: caches uploaded files in MongoDB and copies from DB channel instead of re-downloading
- Multiple uploader bots (Bot_1 … Bot_4) for parallel uploads
- Real-time download & upload progress with speed, ETA, and progress bar
- Per-user task queue (up to 4 queued tasks)
- Auto-deletes user's video copy after 1 hour and notifies on last deletion
- Membership gate — users must join a channel to use the bot
- Cancel button during download/upload
- Retry logic (up to 4 attempts on download failure)

---

## Requirements

### System Dependencies

- **Python 3.10+**
- **ffmpeg** (with `ffprobe`) — must be available in `PATH`
  - Ubuntu/Debian: `sudo apt install ffmpeg`
  - Windows: download from https://ffmpeg.org/download.html and add to PATH

### Python Dependencies

Install via pip:

```bash
pip install -r requirements.txt
```

| Package | Purpose |
|---|---|
| `pyrogram` | Telegram MTProto bot framework |
| `TgCrypto` | Faster encryption for Pyrogram |
| `motor` | Async MongoDB driver |
| `httpx[http2]` | Async HTTP client (thumbnail fetching) |
| `requests` | Sync HTTP (TeraBox API calls) |
| `uvloop` | Faster async event loop (Linux only) |

---

## Configuration

Open `terabnr.py` and fill in the following constants at the top of the file:

```python
API_ID        = 123456            # Your Telegram API ID (from my.telegram.org)
API_HASH      = "your_api_hash"   # Your Telegram API Hash
BOT_TOKEN     = "your_bot_token"  # Main bot token (from @BotFather)

CHANNEL_USERNAME = "@your_channel" # Channel users must join (with @)
PHOTO_URL        = "https://..."   # Welcome photo URL
DUMMY_URL        = "https://..."   # Placeholder photo shown after video expires

DB_CHANNEL   = -100123456789      # Private channel ID for storing uploaded files
MONGO_DB_URI = "mongodb+srv://..." # MongoDB connection URI

SUPPORT_BOT_TOKENS = [
    "111111:AAAA...",   # Bot_1 token
    "222222:BBBB...",   # Bot_2 token
    # add more as needed
]
```

> **Important:** Add all bots (main + support bots) as **admins** in `DB_CHANNEL`.

---

## Running the Bot

```bash
python terabnr.py
```

The bot will:
1. Connect to MongoDB and create indexes
2. Start all supporting uploader bot clients
3. Start the main bot and begin polling

---

## Project Structure

```
terabnr.py          # Main bot script
requirements.txt    # Python dependencies
BOT_README.md       # This file
tmp_downloads/      # Temporary download directory (auto-created)
                    # Uses /dev/shm on Linux for faster I/O
```

---

## How It Works

1. User sends a TeraBox link
2. Bot checks MongoDB for a cached upload (`uid` match)
   - **Cache hit**: main bot copies from `DB_CHANNEL` to user instantly
   - **Cache miss**: proceeds to download
3. Bot fetches the m3u8 stream URL via the TeraBox API
4. `ffmpeg` downloads and muxes the HLS stream to MP4
5. A free supporting bot uploads the MP4 to `DB_CHANNEL`
6. Main bot copies the message from `DB_CHANNEL` to the user
7. MongoDB record is saved for future cache hits
8. After **1 hour**, the user's copy is replaced with a placeholder image

---

## Notes

- `uvloop` is Linux/macOS only and will be skipped on Windows automatically
- Temporary files are stored in `/dev/shm` (RAM disk) on Linux for performance, or `tmp_downloads/` otherwise
- Max supported file size: **1800 MB** (Telegram limit)
