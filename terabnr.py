#!/usr/bin/env python3
"""
TeraBox downloader bot (complete) — modified to support multiple uploader bots

New features added:
- Multiple supporting bots (Bot_1 .. Bot_4) that perform uploads to DB_CHANNEL when main bot is busy.
- If main bot is free it uses itself for upload. If main bot is busy, a free supporting bot will take the upload.
- If all uploaders are busy the task waits in queue until one becomes free.
- Upload progress displayed to the user includes which bot is uploading.
- Supporting bot uploads directly into DB_CHANNEL, inserts/updates MongoDB record (uid,msg_id,filename,size,added_at), and then main bot copies message from DB_CHANNEL to the user (Fast DB support tag is added).
- NEW: When bot sends a video to a user, that user's copy is deleted after 1 hour.
       The user receives the message "⌛ It's been 1 hour, your video has been deleted."
       ONLY when the deleted video was that user's *last* active vide

Notes: replace the SUPPORT_BOT_TOKENS list with actual bot tokens for Bot_1 .. Bot_4.
"""

import os
import time
import asyncio
import httpx
import requests
import signal
import re
import base64
from flask import Flask
from threading import Thread
from io import BytesIO
from math import floor
from urllib.parse import urlparse
from datetime import datetime
from collections import defaultdict, deque
# FIX BUG-5: config.py / db.py not present — inline fallbacks used instead
# ADMINS and DB are defined directly in this file

from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters, enums
from pyrogram.errors import UserNotParticipant
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    InputMediaPhoto,
    CallbackQuery,
)

# === CONFIG ===
API_ID = 37476811
API_HASH = "7aa60670b871050820086c6267371ee6"
BOT_TOKEN = "8694198519:AAHkfsd2hG584oC92jM-Ee2PJd2snDy49qM"

CHANNEL_USERNAME = "@log_ak_bots"   # used for membership check messages
PHOTO_URL = "https://d.uguu.se/VgRgbvSw.jpg"
DUMMY_URL = "https://o.uguu.se/smoyaziL.jpg"   # <- yaha apna dummy photo url daalna

# DB channel and Mongo URI (user provided)
DB_CHANNEL = -1003824246703
ADMINS = [8730393744]
MONGO_DB_URI = "mongodb+srv://Anujedit:Anujedit@cluster0.7cs2nhd.mongodb.net/?appName=Cluster0"

# limits and endpoints
MAX_FILE_SIZE_MB = 1800  # Telegram hard guard (MB)
ITERAPLAY_API_URL = "https://xapiverse.com/api/terabox-pro"
ITERAPLAY_API_KEY = "sk_6c79f22723800e417168d09eafa66565"   # jo key tu use kar raha tha

# Supporting bot tokens (replace with your real tokens)
SUPPORT_BOT_TOKENS = [
    # Example: "123456:AA...",
    "8903849115:AAGDXLU5J2KjUb0Z1J6bhCk2ulytajNXmlQ"
]

# prefer RAM-disk for faster I/O
if os.path.isdir("/dev/shm"):
    TMP_DIR = "/dev/shm/terabox_tmp"
else:
    TMP_DIR = "tmp_downloads"
os.makedirs(TMP_DIR, exist_ok=True)

# concurrency & queue limits
GLOBAL_ACTIVE_LIMIT = 8            # concurrent ffmpeg download slots
UPLOAD_ACTIVE_LIMIT = 6            # concurrent upload slots (logical limit)
MAX_USER_QUEUE = 4                 # per-user queued tasks allowed

global_active_sem = asyncio.Semaphore(GLOBAL_ACTIVE_LIMIT)
upload_active_sem = asyncio.Semaphore(UPLOAD_ACTIVE_LIMIT)

# trackers
active_tasks = {}      # user_id -> deque of task_ids (queued/running)
task_state = {}        # task_id -> state dict
active_uploads = {}    # task_id -> True while uploading

# uploader clients and state
uploader_clients = {}        # name -> Client instance
uploader_state = {}          # name -> bool (True if busy)
uploader_lock = asyncio.Lock()   # protect selection/writes

# Track user copies so we can delete after 1 hour and only notify when last removed
user_active_messages = {}    # user_id -> list of message_ids (their current active copies)
user_active_lock = asyncio.Lock()  # async lock for thread-safe updates

# build httpx client
def _build_httpx_client():
    try:
        return httpx.AsyncClient(http2=True, timeout=30.0)
    except Exception:
        return httpx.AsyncClient(http2=False, timeout=30.0)

httpx_client = _build_httpx_client()

# motor/mongo
mongo = AsyncIOMotorClient(MONGO_DB_URI)
db = mongo["terabox_bot"]
files_col = db["files"]   # document schema: { uid, msg_id, filename, size_mb, added_at }

# pyrogram client (main bot)
app = Client("terabox_dl_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

MAIN_KEYBOARD = ReplyKeyboardMarkup([["📥 Download", "🕹️ About"]], resize_keyboard=True)

# Regex to extract terabox links (simple, captures many terabox variants)
TERABOX_REGEX = re.compile(r"https?://[^\s)>\]]*?/s/[A-Za-z0-9_\-]+")

# ---------- Helpers ----------

def progress_bar(done, total, length=10):
    if not total or total <= 0:
        dots = int((time.time() * 2) % (length + 1))
        return "⬢" * dots + "⬡" * (length - dots)
    filled = min(length, floor(length * done / total))
    return "⬢" * filled + "⬡" * (length - filled)


def format_eta_speed(start_time, done, total):
    elapsed = max(0.001, time.time() - start_time)
    speed_bps = done / elapsed if elapsed > 0 else 0.0
    speed_kbps = speed_bps / 1024
    if speed_kbps >= 1024:
        speed = speed_kbps / 1024
        speed_str = f"{speed:.2f} MB/s"
    else:
        speed_str = f"{speed_kbps:.2f} KB/s"
    if total and total > 0 and speed_bps > 0:
        remaining = max(0, total - done)
        eta = remaining / speed_bps
    else:
        eta = 0
    eta_str = time.strftime("%H:%M:%S", time.gmtime(eta))
    return speed_str, eta_str


def safe_filename(name: str) -> str:
    bad = '\\/:*?"<>|'
    for ch in bad:
        name = name.replace(ch, "_")
    name = name.strip() or "video.mp4"
    return name[:200]


def kb_inline(task_id: str):
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{task_id}")]])


async def ensure_indexes():
    try:
        await files_col.create_index("uid", unique=True)
    except Exception:
        pass



# ---------- New: per-user scheduled deletion & notify only on last ----------
async def schedule_user_video_deletion(user_id: int, message_id: int, delay: int = 3600):
    """After 1 hour, replace user's video with dummy photo + caption.
    Notify only if this was their last active video.
    """
    await asyncio.sleep(delay)

    try:
        # ✅ Replace video with dummy photo
        await app.edit_message_media(
            chat_id=user_id,
            message_id=message_id,
            media=InputMediaPhoto(
                media=DUMMY_URL,
                caption="Your video / file has been deleted due to restriction.\n\nif you want to see it again please re download and save.\n\nआपका विडियो / फाइल डिलीट कर दी गयी है आपको फिर से देखनी है तो फिर से डाउनलोड कर सकते है धन्यवाद!"
            )
        )
    except Exception as e:
        print(f"[schedule_user_video_deletion] edit error user={user_id} msg={message_id}: {e}")

    # Track active messages
    try:
        async with user_active_lock:
            lst = user_active_messages.get(user_id)
            if lst and message_id in lst:
                try:
                    lst.remove(message_id)
                except ValueError:
                    pass
            if not lst:  # agar ye last tha
                user_active_messages.pop(user_id, None)
                try:
                    await app.send_message(user_id, "⌛️ It's been 1 hour — your video has been deleted.\n\nIf you want to watch again, please re-download.")
                except Exception as e:
                    print(f"[schedule_user_video_deletion] notify error user={user_id}: {e}")
    except Exception as e:
        print(f"[schedule_user_video_deletion] lock/update error user={user_id}: {e}")

# ---------- Membership check (FIX) ----------
async def check_membership(user_id: int) -> bool:
    try:
        member = await app.get_chat_member(CHANNEL_USERNAME, user_id)
        if member.status in (enums.ChatMemberStatus.MEMBER, enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER):
            return True
        return False
    except UserNotParticipant:
        return False
    except Exception as e:
        print("Membership check error:", e)
        return False

# -------- helper to decode base64 safely ----------
def b64decode_safe(data: str) -> str:
    try:
        return base64.b64decode(data).decode("utf-8", errors="ignore")
    except Exception:
        return data


# -------- fetch m3u8 items from API ----------
def fetch_m3u8_items(terabox_url: str):
    """
    POST https://xapiverse.com/api/terabox-pro
    Headers: Content-Type: application/json, xAPIverse-Key: <key>
    Body:    {"url": "<terabox_link>"}
    Response: {"status":"success","list":[{name,stream_url,fast_stream_url,normal_dlink,thumbnail,size,size_formatted,...}]}
    """
    # FIX BUG-1: correct header name is 'xAPIverse-Key' not x-rapidapi-key
    # FIX BUG-2: POST with JSON body, not GET with params
    headers = {
        "Content-Type": "application/json",
        "xAPIverse-Key": ITERAPLAY_API_KEY,
    }

    # FIX BUG-4: body key is "url" not "link"
    payload = {"url": terabox_url}

    try:
        r = requests.post(
            ITERAPLAY_API_URL,
            headers=headers,
            json=payload,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        print("[API Response]", data)

        # FIX BUG-3: response has "list" key, not "data"
        if data.get("status") != "success" or not data.get("list"):
            print("API error or empty list:", data.get("status"))
            return []

        items = []
        for file_data in data["list"]:
            # stream_url = direct m3u8 (best quality)
            # fast_stream_url = dict of {360p, 480p, 720p, 1080p} m3u8 links
            # normal_dlink = direct download URL (non-stream)

            m3u8_url = (
                file_data.get("stream_url")
                or (file_data.get("fast_stream_url") or {}).get("1080p")
                or (file_data.get("fast_stream_url") or {}).get("720p")
                or (file_data.get("fast_stream_url") or {}).get("480p")
                or (file_data.get("fast_stream_url") or {}).get("360p")
                or file_data.get("fast_dlink")      # terabox-pro field
                or file_data.get("normal_dlink")    # terabox field (fallback)
                or ""
            )

            # size_bytes for accurate progress bar
            size_bytes = int(file_data.get("size", 0) or 0)
            size_mb    = size_bytes / (1024 * 1024) if size_bytes else 0.0
            size_str   = file_data.get("size_formatted") or (f"{size_mb:.2f} MB" if size_mb else "Unknown")

            items.append({
                "name":           file_data.get("name") or "video",
                "m3u8":           m3u8_url,
                "normal_dlink":   file_data.get("fast_dlink", "") or file_data.get("normal_dlink", ""),
                "size_formatted": size_str,
                "size_mb":        size_mb,         # FIX BUG-6: actual size for progress
                "thumbnail":      file_data.get("thumbnail", ""),
                "quality":        file_data.get("quality", ""),
                "duration":       file_data.get("duration", ""),
                "fs_id":          file_data.get("fs_id", ""),
            })

        return items

    except Exception as e:
        print("API error:", e)
        return []

# ---------- ffprobe helper ----------

def probe_duration_seconds(input_url: str) -> float:
    import subprocess
    try:
        cmd = f'ffprobe -v error -show_entries format=duration -of default=nk=1:nw=1 "{input_url}"'
        out = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, timeout=25)
        s = out.decode().strip()
        dur = float(s)
        if dur > 0 and dur < 1e7:
            return dur
    except Exception:
        pass
    return 0.0


def format_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

# ---------- ffmpeg download with progress ----------

async def download_m3u8_ffmpeg(task_id: str, m3u8_url: str, out_path: str, status_msg: Message, approx_size_bytes: int = 0):
    st = task_state.get(task_id, {})
    st["stage"] = "downloading"
    st["file_path"] = out_path
    st["status_message"] = status_msg
    task_state[task_id] = st

    try:
        if os.path.exists(out_path):
            os.remove(out_path)
    except Exception:
        pass

    duration = await asyncio.get_running_loop().run_in_executor(None, probe_duration_seconds, m3u8_url)
    start_time = time.time()
    last_edit = 0.0
    bytes_written = 0

    cmd = [
        "ffmpeg", "-y",
        "-threads", "4",
        "-user_agent", "Mozilla/5.0",
        "-i", m3u8_url,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "+faststart",
        "-progress", "pipe:1",
        "-loglevel", "error",
        out_path
    ]

    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

    percent = 0.0
    try:
        while True:
            if st.get("canceled"):
                try:
                    proc.send_signal(signal.SIGTERM)
                except Exception:
                    pass
                await proc.wait()
                try:
                    await status_msg.edit_text("❌ Download cancelled.")
                except Exception:
                    pass
                return False

            line = await proc.stdout.readline()
            if not line:
                if proc.returncode is not None:
                    break
                await asyncio.sleep(0.2)
                continue

            line = line.decode(errors="ignore").strip()
            if not line:
                continue

            if line.startswith("out_time_ms="):
                out_ms = float(line.split("=", 1)[1] or 0)
                current_sec = out_ms / 1_000_000.0
                if duration > 0:
                    percent = min(100.0, (current_sec / duration) * 100.0)
            elif line.startswith("total_size="):
                try:
                    bytes_written = int(line.split("=", 1)[1] or 0)
                except Exception:
                    pass

            now = time.time()
            if now - last_edit >= 1.2:
                denom = approx_size_bytes or (bytes_written if bytes_written > 0 else 0)
                speed, eta = format_eta_speed(start_time, bytes_written, denom)
                bar = progress_bar(percent, 100.0 if duration > 0 else 0)
                size_str = f"{bytes_written/1024/1024:.2f} MB" if bytes_written else "—"
                text = (
                    "📥 **Downloading (HLS → MP4)**\n\n"
                    "╭━━━━❰Progress❱━➣\n"
                    f"┣⪼ [{bar}]\n"
                    f"┣⪼ ✅ Done: {percent:.2f}%\n"
                    f"┣⪼ 📦 Written: {size_str}\n"
                    f"┣⪼ ⚡ Speed: {speed}\n"
                    f"┣⪼ ⏳ ETA: {eta if duration > 0 else '—'}\n"
                    f"┣⪼ ⌚ Duration: {format_time(duration) if duration > 0 else 'Unknown'}\n"
                    "╰━━━━━━━━━━━━━━━➣"
                )
                try:
                    await status_msg.edit_text(text, reply_markup=kb_inline(task_id))
                except Exception:
                    pass
                last_edit = now

        rc = await proc.wait()
        if rc != 0:
            stderr = (await proc.stderr.read()).decode(errors="ignore")
            try:
                await status_msg.edit_text(f"❌ Download error (ffmpeg): {stderr or rc}")
            except Exception:
                pass
            return False

        return True

    except Exception as e:
        try:
            await status_msg.edit_text(f"❌ Download error: {e}")
        except Exception:
            pass
        try:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
        except Exception:
            pass
        return False

# ---------- Uploader selection & upload via supporting bots ----------

async def init_uploader_clients():
    """Create and start uploader clients from SUPPORT_BOT_TOKENS."""
    idx = 1
    for token in SUPPORT_BOT_TOKENS:
        name = f"Dc_{idx}"
        # each uploader gets its own session name
        c = Client(name, api_id=API_ID, api_hash=API_HASH, bot_token=token)
        uploader_clients[name] = c
        uploader_state[name] = False
        idx += 1

    # Start all uploader clients
    for name, c in uploader_clients.items():
        try:
            await c.start()
            print(f"Started uploader client: {name}")
        except Exception as e:
            print(f"Failed to start uploader {name}: {e}")


async def stop_uploader_clients():
    for name, c in uploader_clients.items():
        try:
            await c.stop()
        except Exception:
            pass


async def get_free_uploader(wait: bool = True):
    """Return an available uploader client name and client object.
    If none free and wait=True, waits until someone becomes free.
    If wait=False returns (None,None) immediately when no free uploader.
    """
    while True:
        async with uploader_lock:
            for name, busy in uploader_state.items():
                if not busy:
                    uploader_state[name] = True
                    return name, uploader_clients[name]
        if not wait:
            return None, None
        # wait a bit and retry
        await asyncio.sleep(1.0)


async def mark_uploader_free(name: str):
    async with uploader_lock:
        uploader_state[name] = False


async def send_with_upload_progress_via_uploader(task_id: str, video_path: str, caption: str, thumb, status_msg: Message):
    """Pick a free uploader (or wait), upload file into DB_CHANNEL using that uploader and report progress to user's status_msg.
    Returns the uploaded Message (in DB_CHANNEL) or None on failure.
    """
    # choose an uploader (this will mark it `busy`)
    uploader_name, uploader_client = await get_free_uploader(wait=True)
    if uploader_client is None:
        await status_msg.edit_text("⚠️ No uploader available right now. Please wait...")
        return None

    st = task_state.get(task_id, {})
    st["stage"] = "uploading"
    st["uploader"] = uploader_name
    task_state[task_id] = st

    file_size = os.path.getsize(video_path)
    start_time = time.time()
    last_edit = 0.0

    async def progress(current: int, total: int):
        nonlocal last_edit
        if st.get("canceled"):
            raise RuntimeError("Upload cancelled by user")
        now = time.time()
        if now - last_edit >= 1.2:
            denom = total or max(current, 1)
            speed, eta = format_eta_speed(start_time, current, denom)
            percent = (current / denom * 100) if denom else 0
            bar = progress_bar(current, denom)
            text = (
                f"📤 **Uploading... (by {uploader_name})**\n\n"
                "╭━━━━❰Progress❱━➣\n"
                f"┣⪼ [{bar}]\n"
                f"┣⪼ ✅ Uploaded: {percent:.2f}%\n"
                f"┣⪼ 📦 Size: {file_size/1024/1024:.2f} MB\n"
                f"┣⪼ ⚡ Speed: {speed}\n"
                f"┣⪼ ⏳ ETA: {eta}\n"
                f"┣⪼ 👥 Active Uploads: {len(active_uploads)}/{UPLOAD_ACTIVE_LIMIT}\n"
                "╰━━━━━━━━━━━━━━━➣"
            )
            try:
                await status_msg.edit_text(text, reply_markup=kb_inline(task_id))
            except Exception:
                pass
            last_edit = now

    active_uploads[task_id] = True
    try:
        sent = await uploader_client.send_video(
            chat_id=DB_CHANNEL,
            video=video_path,
            caption=caption,
            thumb=thumb,
            supports_streaming=True,
            progress=progress
        )

        # ✅ Ab upload poora hone ke baad status message delete karo
        try:
            await status_msg.delete()
        except Exception:
            pass

        return sent
    except Exception as e:
        try:
            await status_msg.edit_text(f"❌ Upload error ({uploader_name}): {e}")
        except Exception:
            pass
        return None
    finally:
        active_uploads.pop(task_id, None)
        await mark_uploader_free(uploader_name)

# ---------- Full pipeline (download -> supporting upload -> DB save -> main copy to user) ----------

async def download_and_upload_m3u8(task_id: str, user_id: int, m3u8_url: str, filename: str, thumb_url: str, size_hint_mb: float = 0.0):
    async with global_active_sem:
        st = task_state.get(task_id, {})
        status_msg: Message = st.get("status_message")
        filename = safe_filename(filename)
        if not filename.lower().endswith(".mp4"):
            filename = filename + ".mp4"
        out_file = os.path.join(TMP_DIR, f"{task_id}_{filename}")
        st["file_path"] = out_file
        task_state[task_id] = st

        approx_bytes = int(size_hint_mb * 1024 * 1024) if size_hint_mb else 0

        ok = False
        max_attempts = 4
        for attempt in range(max_attempts):
            ok = await download_m3u8_ffmpeg(task_id, m3u8_url, out_file, status_msg, approx_size_bytes=approx_bytes)
            if ok:
                break
            else:
                if attempt < max_attempts - 1:
                    try:
                        await status_msg.edit_text(f"⚠️ Download failed, retrying... attempt {attempt+2}/{max_attempts}")
                    except Exception:
                        pass
                    await asyncio.sleep(3)
        if not ok:
            try:
                await status_msg.edit_text("❌ Download failed after 4 attempts. Please try again later.")
            except Exception:
                pass
            if os.path.exists(out_file):
                try:
                    os.remove(out_file)
                except Exception:
                    pass
            return None

        try:
            await status_msg.edit_text("📤 Preparing upload...", reply_markup=kb_inline(task_id))
        except Exception:
            pass

        thumb = None
        if thumb_url:
            try:
                r = await httpx_client.get(thumb_url, timeout=15.0)
                if r.status_code == 200:
                    thumb = BytesIO(r.content)
                    thumb.name = "thumbnail.jpg"
            except Exception:
                thumb = None

        size_mb = os.path.getsize(out_file) / (1024 * 1024)

        if size_mb > MAX_FILE_SIZE_MB:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬇️ Download Now", url="https://t.me/TeraBox_Support_Anuj_Bot")]])
            try:
                await status_msg.edit_text(f"📂 {filename}\n💾 {size_mb:.2f} MB\n⚠️ File too large to upload.", reply_markup=kb)
            except Exception:
                pass
            try:
                os.remove(out_file)
            except Exception:
                pass
            return None

        caption = f"🎬 {filename}\n\n📦 {size_mb:.2f} MB\n🎞️ Quality: HD (Streaming Quality)\n\n⚠️ This file will auto-delete from here in 1 Hour.\n📤 Please forward it to any other chat to save it permanently.\n\n❤️ Powered By @Thestarbots, @terababu1_bot & @Terabnrbot"

        # Use supporting uploader clients to upload into DB_CHANNEL
        try:
            sent = await send_with_upload_progress_via_uploader(task_id, out_file, caption, thumb, status_msg)
            if not sent:
                return None

            # After supporting bot uploaded to DB_CHANNEL (sent is message in DB_CHANNEL), save record to MongoDB
            try:
                doc = {
                    "uid": st.get("unique_id"),
                    "msg_id": sent.id,
                    "filename": filename,
                    "size_mb": size_mb,
                    "added_at": datetime.utcnow()
                }
                await files_col.update_one({"uid": st.get("unique_id")}, {"$set": doc}, upsert=True)
            except Exception as e:
                print("Error saving DB record after supporting upload:", e)

            # Now main bot copies message from DB_CHANNEL to the user (Fast DB support) and notifies user
            try:
                # add extra caption note for fast db support
                try:
                    orig_caption = sent.caption or ""
                except Exception:
                    orig_caption = ""
                new_caption = (orig_caption + "\n\n🌩 hyper speed download").strip()

                # copy message and get returned Message object (so we can schedule deletion for the user's copy)
                user_msg = await app.copy_message(chat_id=user_id, from_chat_id=DB_CHANNEL, message_id=sent.id, caption=new_caption)

                # === Track the user's copy for scheduled deletion ===
                try:
                    async with user_active_lock:
                        lst = user_active_messages.get(user_id)
                        if not lst:
                            user_active_messages[user_id] = []
                        user_active_messages[user_id].append(user_msg.id)
                except Exception as e:
                    print(f"[download_and_upload_m3u8] error tracking user_msg user={user_id} msg={getattr(user_msg,'id',None)}: {e}")

                # Schedule deletion after 1 hour (3600 seconds)
                asyncio.create_task(schedule_user_video_deletion(user_id, user_msg.id, delay=3600))

            except Exception as e:
                try:
                    await status_msg.edit_text("⚠️ Error delivering file to you after upload.")
                except Exception:
                    pass
                print("Error copying from DB_CHANNEL to user:", e)

            return sent
        except Exception as e:
            try:
                await status_msg.edit_text(f"❌ Upload error: {e}")
            except Exception:
                pass
            return None
        finally:
            if os.path.exists(out_file):
                try:
                    os.remove(out_file)
                except Exception:
                    pass

# ---------- Commands & Handlers (unchanged except minor integration) ----------

@app.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message: Message):
    if await check_membership(message.from_user.id):
        await message.reply_photo(photo=PHOTO_URL, caption= "✨ Welcome to TeraBox Downloader Bot ✨\n\n"
    "🚀 Fast • Secure • Unlimited Downloads\n"
    "📥 Send any TeraBox link and get direct video access instantly.\n\n"
    "👨‍💻 Developed By: @anujedits76")
           
        await message.reply_text(
    "🎬 Send your TeraBox link below\n"
    "🔗 Supported format: /s/<uid>\n\n"
    "⚡ I’ll fetch the video and deliver it instantly.",
            reply_markup=MAIN_KEYBOARD,
        )
    else:
        await message.reply_photo(
            photo=PHOTO_URL,
            caption="👋 Please join the channel to use this bot.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Join channel", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
                [InlineKeyboardButton("🔍 Verify", callback_data="verify")]
            ]),
                               )

@app.on_message(filters.private & filters.text & filters.regex(r"^📥 Download$"))
async def btn_download(client: Client, message: Message):
    if not await check_membership(message.from_user.id):
        await message.reply_text(
            "❗ Please join the channel and verify first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Join channel", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
                [InlineKeyboardButton("🔍 Verify", callback_data="verify")]
            ]),
        )
        return
    await message.reply_text(
    "📤 Send Your TeraBox URL\n\n"
    "⚡ Direct download supported\n"
    "🎬 HD videos available instantly\n\n"
    "📎 Example:\n"
    "`https://terabox.com/s/abc123`\n\n"
    "⏳ Processing starts automatically..."
)

@app.on_message(filters.private & filters.text & filters.regex(r"^🕹️ About$"))
async def btn_about(client: Client, message: Message):
    if not await check_membership(message.from_user.id):
        await message.reply_text(
            "❗ Please join the channel and verify first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Join channel", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
                [InlineKeyboardButton("🔍 Verify", callback_data="verify")]
            ]),
        )
        return
    await message.reply_text(
    "✨ TeraBox Downloader Bot ✨\n\n"
    "🚀 Ultra-fast TeraBox downloading\n"
    "📥 Instant direct video uploads\n"
    "🎬 HD quality supported\n"
    "⚙️ Optimized upload engine\n\n"
    "🔗 Official Bots:\n"
    "@Terabox_Video_Downloader_AK_Bot\n"
    "@Terabox_Support_AK_Bot\n\n"
    "👨‍💻 Developer: @anujedits76\n"
    "⚡ Powered By: @anujedits76\n\n"
    "📦 Version: v1.1.0\n"
    f"📊 Active Uploads: {len(active_uploads)}/{UPLOAD_ACTIVE_LIMIT}"
       )

@app.on_callback_query()
async def on_callback(client: Client, cq: CallbackQuery):
    data = cq.data or ""
    if data == "verify":
        if await check_membership(cq.from_user.id):
            try:
                await cq.message.edit_text("✅ Verification successful!\n\nNow send me a TeraBox link.", reply_markup=MAIN_KEYBOARD)
            except Exception:
                pass
        else:
            await cq.answer("❗ Please join the channel first!", show_alert=True)
        return

    if data.startswith("cancel:"):
        task_id = data.split(":", 1)[1]
        st = task_state.get(task_id)
        if not st:
            await cq.answer("Task not found or already finished.", show_alert=True)
            return
        st["canceled"] = True
        task_state[task_id] = st
        await cq.answer("❌ Cancelled", show_alert=True)
        try:
            await st["status_message"].edit_text("⏹️ Cancelling... please wait.")
        except Exception:
            pass
        return

# ---------- New: process_link() used for each extracted terabox link ----------
async def process_link(user_id: int, terabox_link: str, orig_message: Message):
    """Process a single terabox link for a user (used when multiple links found)."""
    # re-check membership just in case
    if not await check_membership(user_id):
        try:
            await orig_message.reply_text(
                "❗ Please join the channel and verify first.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Join channel", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
                    [InlineKeyboardButton("🔍 Verify", callback_data="verify")]
                ]),
            )
        except Exception:
            pass
        return

    # extract uid from link
    tbx_uid_matches = re.findall(r"/s/([A-Za-z0-9_\-]+)", terabox_link)
    if not tbx_uid_matches:
        try:
            await orig_message.reply_text("⚠️ No valid TeraBox UID found in the link.")
        except Exception:
            pass
        return
    unique_id = tbx_uid_matches[0]

    # per-user queue check
    user_queue = active_tasks.get(user_id)
    if user_queue is None:
        user_queue = deque()
        active_tasks[user_id] = user_queue

    if len(user_queue) >= MAX_USER_QUEUE:
        try:
            await orig_message.reply_text(f"⚠️ Queue full. You can only have up to {MAX_USER_QUEUE} tasks queued. Please wait.")
        except Exception:
            pass
        return

    # Fast DB check
    try:
        rec = await files_col.find_one({"uid": unique_id})
    except Exception as e:
        rec = None
        print("DB lookup error:", e)

    # prepare reply/status message
    try:
        status = await orig_message.reply_text("⏳ Fetching Media details...")
    except Exception:
        # fallback: try sending directly to user
        try:
            status = await app.send_message(user_id, "⏳ Fetching Media details...")
        except Exception:
            return

    if rec:
        msg_id = rec.get("msg_id")
        orig_caption = ""
        try:
            orig_msg = await app.get_messages(DB_CHANNEL, msg_id)
            orig_caption = orig_msg.caption or ""
        except Exception:
            orig_caption = ""
        new_caption = (orig_caption + "\n\n⚡️ Super sonic download").strip()
        try:
            # copy and track user's message copy for scheduled deletion
            user_msg = await app.copy_message(chat_id=user_id, from_chat_id=DB_CHANNEL, message_id=msg_id, caption=new_caption)
            try:
                async with user_active_lock:
                    lst = user_active_messages.get(user_id)
                    if not lst:
                        user_active_messages[user_id] = []
                    user_active_messages[user_id].append(user_msg.id)
            except Exception as e:
                print(f"[process_link-fastdb] track error user={user_id} msg={getattr(user_msg,'id',None)}: {e}")

            asyncio.create_task(schedule_user_video_deletion(user_id, user_msg.id, delay=3600))
            try:
                await status.delete()
            except Exception:
                pass
            return
        except Exception as e:
            try:
                await status.edit_text("⚠️ Fast DB send failed, will try to download & send instead...")
            except Exception:
                pass
            print("Copy from DB_CHANNEL failed:", e)

    # Not cached or copy failed -> proceed to queue download
    loop = asyncio.get_running_loop()
    items = await loop.run_in_executor(None, fetch_m3u8_items, terabox_link)

    if not items:
        try:
            await status.edit_text("⚠️ Failed to fetch m link from the provided TeraBox URL.")
        except Exception:
            pass
        return

    file_info = items[0]
    filename = safe_filename(file_info.get("name", "video"))
    size_hint = file_info.get("size_formatted") or "Unknown"
    size_mb   = file_info.get("size_mb", 0.0)   # FIX BUG-6: use actual size from API
    thumb_url = file_info.get("thumbnail")
    m3u8_url  = file_info.get("m3u8")

    if not m3u8_url:
        try:
            await status.edit_text("⚠️ This link download is not available please send me another link")
        except Exception:
            pass
        return

    task_id = f"{user_id}_{int(time.time() * 1000)}"
    # append to user's queue
    user_queue.append(task_id)
    task_state[task_id] = {
        "user_id": user_id,
        "status_message": status,
        "canceled": False,
        "stage": "queued",
        "file_path": None,
        "unique_id": unique_id,
        "terabox_link": terabox_link,
    }

    async def pipeline():
        try:
            sent = await download_and_upload_m3u8(task_id, user_id, m3u8_url, filename, thumb_url, size_hint_mb=size_mb)  # FIX BUG-6
            if not sent:
                try:
                    await status.edit_text("i got some pet gadbad while downloading your video\nif you think bot has problem please send any another video link to check\n\nif you think your video link is valid then maybe  this is a error due to overload please use any another bot from this @starbotlist\n\nOur official support bot for report bugs and request features @Thestargc")
                except Exception:
                    pass
                return
        finally:
            # remove task from user's deque
            uq = active_tasks.get(user_id)
            if uq and task_id in uq:
                try:
                    uq.remove(task_id)
                except Exception:
                    pass
            task_state.pop(task_id, None)

    # start pipeline as background task
    asyncio.create_task(pipeline())

    try:
        await status.edit_text(
            f"⏳ Task started...\n\n🎬 **{filename}**\n📦 ~{size_hint}\n👥 Active Uploads: {len(active_uploads)}/{UPLOAD_ACTIVE_LIMIT}",
            reply_markup=kb_inline(task_id)
        )
    except Exception:
        pass

# ---------- Multi-link handler (replaces single-link handler) ----------
@app.on_message(
    filters.private
    & (filters.text | filters.caption)
    & ~filters.command("start")
    & ~filters.regex(r"^📥 Download$")
    & ~filters.regex(r"^🕹️ About$")
)
async def handle_links(client: Client, message: Message):
    user_id = message.from_user.id
    text = message.text or message.caption or ""

    if not await check_membership(user_id):
        await message.reply_text(
            "❗ Please join the channel and verify first.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Join channel", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
                [InlineKeyboardButton("🔍 Verify", callback_data="verify")]
            ]),
        )
        return

    # extract all terabox links
    links = TERABOX_REGEX.findall(text)
    if not links:
        await message.reply_text("⚠️ No valid TeraBox link found in your message.")
        return

    # process each link (run in background to allow parallel processing)
    for link in links:
        # normalize link (strip punctuation)
        link = link.strip().rstrip(".,;)")
        asyncio.create_task(process_link(user_id, link, message))

app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "✅ TeraBox Downloader Bot Running!"

def run_flask():
    app_flask.run(host="0.0.0.0", port=5000)   

# ---------- Startup / Run ----------

async def main():
    await ensure_indexes()

    try:
        await init_uploader_clients()
    except Exception as e:
        print("Warning: failed to start some uploader clients:", e)

    try:
        await app.start()
        print("🤖 Bot is running...")
        # Send startup message to admin
        try:
            for admin_id in ADMINS:
                await app.send_message(admin_id, "🤖 **Bot Started Successfully!**\n\n✅ All systems running.")
        except Exception as e:
            print(f"Could not send startup message: {e}")
        await asyncio.Event().wait()  # keep running until interrupted
    finally:
        try:
            await stop_uploader_clients()
        except Exception:
            pass
        try:
            await httpx_client.aclose()
        except Exception:
            pass
        try:
            mongo.close()
        except Exception:
            pass
        try:
            await app.stop()
        except Exception:
            pass


if __name__ == "__main__":
    print("🚀 TeraBox Pyrogram Bot starting with multiple uploaders...")
    print(f"📤 Upload Concurrency: {UPLOAD_ACTIVE_LIMIT} simultaneous uploads")

    Thread(target=run_flask).start()

    asyncio.run(main())
