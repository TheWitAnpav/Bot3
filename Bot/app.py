"""
Telegram bot that downloads a file from a given URL and sends it to the user.

Requirements:
    pip install pyTelegramBotAPI requests

Setup:
    1. Get a bot token from @BotFather on Telegram.
    2. Set it in the BOT_TOKEN variable below (or as an environment variable TELEGRAM_BOT_TOKEN).
    3. Run: python telegram_download_bot.py

Usage:
    - Send /start to the bot to see instructions.
    - Send /download to have the bot fetch the preconfigured DEFAULT_FILE_URL.
    - Send /newlink (or tap the button) to give it any other direct URL.

Reliability notes:
    - Downloads use multiple parallel connections when the server supports
      byte-range requests, and each connection auto-retries/resumes on
      hiccups instead of failing the whole transfer.
    - The Telegram long-polling loop auto-restarts on any crash.
    - If you're hosting this on a free platform that sleeps idle processes
      (Render/Replit/Railway free tiers), set ENABLE_KEEPALIVE_SERVER=1 in
      the environment and point an external uptime-pinger (e.g. UptimeRobot,
      cron-job.org) at the process's URL every 5 minutes to keep it awake.
"""

import os
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
import telebot
from telebot import apihelper
from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8594272808:AAHkG843n-21nvKBCUmoP7YzRXM55DHlW1o")

# The default file URL used when the user taps "Download default file"
DEFAULT_FILE_URL = (
    "https://dl-worker.teraboxdl.site/?token=3kHhKIGeze1RXtekablJs0QRvXhK5G66sRNbw1oEb0fOKABeBCXM4dbGxnBmtw_rvU74Rp9PL7Z2qaCIGrqWbrWn7fsyzfW0ygL3iZet8AjQjyd0coZPqY0eiA9EzME97B2rpMjn2efwcF0RQUupet93yQv2jW8VNPe3DHZkONqT91xm7ebnpQvzumfU7-Q_NP7-ucuGbxhQjBQaLwhblCClGUC4WsFfaAJI14apVdggILNZuVvzrY-ignsaupRt0BpDNC-OdI2uRDgLclSXLlH8OqoD5fyUiADYYALi08zlIyMRVRJYTTo40knOfpFkZ1-h45hqnXPzIc2J7AlNwjN4BK8o0oWG0QoVrZC8zSGLSAL_DyjiI6E4lJodDJbCf5Yh6mG69JcY9LHN8Sj6z3Gqi0Cip5FGuj6JS_pYOjwPK4dwijz8LY-QZF49NIMPr9IvJIOuvZIGMENBjSbjmMZWGBMLG5pb_vC7j3Tu11cAuKLge5ISrfdBmhjVlUYxEnFwl_TwRFUFOhyyqUyKmJCuPbUw2cs98wwyard48ZczApjKbZanOF-sMYsvbZrDnm74SmazvDuRQuUOVrdfe9dmkujq7m_luIg4PknxNmeL3nk_AInxurZ8poP-E0nptnbJndADaim7HtkLW6W4BffisJtqIWFy3TtC9A7k705x-hEoSyGBwGZ5O9jyvlNIWSW6Z-2M-g"
)

# Local folder to temporarily store downloaded files before uploading to Telegram
DOWNLOAD_DIR = "downloads"

# Telegram bot API limit for sending files via bot uploads (in bytes).
# Regular bots are limited to 50 MB uploads.
MAX_TELEGRAM_FILE_SIZE = 50 * 1024 * 1024

# How often (in seconds) to refresh the "downloading..." progress message.
# Telegram rate-limits message edits, so don't go below ~1s.
PROGRESS_UPDATE_INTERVAL = 2.0

# ---- High-speed download tuning -------------------------------------------
# Number of parallel connections used to fetch a file when the server
# supports byte-range requests (most direct-download / CDN links do).
NUM_CONNECTIONS = 8

# Don't bother splitting small files into parallel chunks — the overhead
# isn't worth it below this size.
MIN_SIZE_FOR_PARALLEL = 5 * 1024 * 1024  # 5 MB

# Chunk size used when reading each connection's stream.
STREAM_CHUNK_SIZE = 512 * 1024  # 512 KB

# ---- Reliability tuning -----------------------------------------------------
# How many times a single range/segment retries after a transient network
# error (timeout, connection reset, etc.) before the whole download fails.
MAX_SEGMENT_RETRIES = 6

# How many times the *entire* download (probe + fetch) is retried if it
# fails outright (e.g. server rejected the connection before any bytes came
# in). Backs off exponentially between attempts.
MAX_DOWNLOAD_ATTEMPTS = 3

# Optional tiny HTTP server so free hosts (Render/Replit/Railway free tiers,
# UptimeRobot-style pingers) can keep the process from being put to sleep.
# Set ENABLE_KEEPALIVE_SERVER=1 in the environment to turn it on.
ENABLE_KEEPALIVE_SERVER = os.environ.get("ENABLE_KEEPALIVE_SERVER", "0") == "1"
KEEPALIVE_PORT = int(os.environ.get("PORT", "8080"))

# ----------------------------------------------------------------------------
# Setup
# ----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Give the underlying HTTP client more slack so a slow/flaky mobile network
# doesn't blow up with ReadTimeout during long-polling (this was the cause
# of the "Infinity polling exception ... Read timed out" crash).
apihelper.CONNECT_TIMEOUT = 15
apihelper.READ_TIMEOUT = 60

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)


class _LoggingExceptionHandler(telebot.ExceptionHandler):
    """Ensures an error inside any message/callback handler is logged
    instead of silently killing that handler's thread."""

    def handle(self, exception):
        logger.exception("Unhandled exception in a bot handler: %s", exception)
        return True  # tell telebot the exception was handled


bot.exception_handler = _LoggingExceptionHandler()

# Shared HTTP session with a bigger connection pool so multiple parallel
# range-requests don't queue up behind each other, plus automatic retries
# (with backoff) at the transport layer for transient network/server errors.
from urllib3.util.retry import Retry  # noqa: E402

http_session = requests.Session()
_retry = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=frozenset(["GET", "HEAD"]),
    raise_on_status=False,
)
_adapter = HTTPAdapter(
    pool_connections=NUM_CONNECTIONS,
    pool_maxsize=NUM_CONNECTIONS,
    max_retries=_retry,
)
http_session.mount("http://", _adapter)
http_session.mount("https://", _adapter)

# Tracks chats that are currently expected to send a URL as their next message
# (i.e. they tapped "Provide new link").
awaiting_link = set()


def main_menu_markup() -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("⬇️ Download default file", callback_data="download_default")
    )
    markup.add(
        InlineKeyboardButton("🔗 Provide new link", callback_data="provide_link")
    )
    return markup


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def get_filename_from_response(response: requests.Response, fallback: str = "downloaded_file") -> str:
    """Try to figure out a sensible filename from the response headers/URL."""
    # 1. Try Content-Disposition header
    cd = response.headers.get("content-disposition")
    if cd and "filename=" in cd:
        filename = cd.split("filename=")[-1].strip().strip('"').strip("'")
        if filename:
            return filename

    # 2. Try the last segment of the URL path (ignoring query string)
    path = response.url.split("?")[0]
    name = os.path.basename(path)
    if name:
        return name

    # 3. Fallback
    return fallback


def format_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def format_mb(n: int) -> str:
    """Always express a byte count in MB, e.g. 12.34 MB — used for the
    download progress status since users generally think in MB."""
    return f"{n / (1024 * 1024):.2f} MB"


def _probe_url(url: str):
    """
    Issue a HEAD (falling back to a ranged GET) to find the total file size
    and whether the server supports byte-range requests (needed for
    parallel/multi-connection downloading).

    Returns (total_size: int, supports_range: bool, filename: str).
    """
    try:
        resp = http_session.head(url, allow_redirects=True, timeout=(15, 30))
        headers = resp.headers
        # Some servers don't implement HEAD properly (0 length / no headers).
        if resp.status_code >= 400 or "content-length" not in headers:
            raise requests.exceptions.RequestException("HEAD not usable")
    except requests.exceptions.RequestException:
        # Fall back to a tiny ranged GET just to read headers.
        resp = http_session.get(
            url, headers={"Range": "bytes=0-0"}, stream=True, timeout=(15, 30)
        )
        headers = resp.headers
        resp.close()

    total_size = int(headers.get("content-length", 0))
    # If we got a 206 partial response, or the server declares byte-range
    # support explicitly, we can parallelize.
    supports_range = (
        headers.get("accept-ranges", "").lower() == "bytes"
        or resp.status_code == 206
    )
    filename = get_filename_from_response(resp)
    return total_size, supports_range, filename


def _download_single_stream(url: str, local_path: str, total_size: int, supports_range: bool, progress_callback=None):
    """Fallback path: one connection, streamed straight to disk, with
    resumable retries if the connection drops mid-transfer (resume only
    works if the server actually honors Range requests)."""
    downloaded = 0
    last_update = 0.0
    attempt = 0

    # Truncate/create the file fresh before we start.
    open(local_path, "wb").close()

    while True:
        attempt += 1
        try:
            headers = {"Range": f"bytes={downloaded}-"} if (downloaded and supports_range) else {}
            with http_session.get(url, headers=headers, stream=True, timeout=(15, 60)) as response:
                response.raise_for_status()
                resumed = downloaded and headers and response.status_code == 206
                if downloaded and not resumed:
                    # Server ignored our Range header and sent the full body
                    # again — start the file over instead of corrupting it.
                    downloaded = 0
                mode = "r+b" if resumed else "wb"
                with open(local_path, mode) as f:
                    if resumed:
                        f.seek(downloaded)
                    for chunk in response.iter_content(chunk_size=STREAM_CHUNK_SIZE):
                        if not chunk:
                            continue
                        f.write(chunk)
                        downloaded += len(chunk)

                        if progress_callback:
                            now = time.monotonic()
                            if now - last_update >= PROGRESS_UPDATE_INTERVAL:
                                last_update = now
                                try:
                                    progress_callback(downloaded, total_size)
                                except Exception:
                                    logger.exception("Progress callback failed")
            break  # completed without error
        except (requests.exceptions.RequestException, OSError) as e:
            if attempt >= MAX_SEGMENT_RETRIES or (total_size and downloaded >= total_size):
                raise
            wait = min(2 ** attempt, 30)
            logger.warning(
                "Download hiccuped at byte %d (%s), retrying in %ds (attempt %d/%d)",
                downloaded, e, wait, attempt, MAX_SEGMENT_RETRIES,
            )
            time.sleep(wait)

    if progress_callback:
        try:
            progress_callback(downloaded, total_size)
        except Exception:
            logger.exception("Final progress callback failed")


def _download_parallel(url: str, local_path: str, total_size: int, num_connections: int, progress_callback=None):
    """
    High-speed path: split the file into `num_connections` byte ranges and
    fetch them concurrently, each thread writing directly into its slice of
    the pre-allocated output file.
    """
    # Pre-allocate the file so every thread can seek+write independently.
    with open(local_path, "wb") as f:
        if total_size > 0:
            f.truncate(total_size)

    part_size = total_size // num_connections
    ranges = []
    start = 0
    for i in range(num_connections):
        end = total_size - 1 if i == num_connections - 1 else start + part_size - 1
        ranges.append((start, end))
        start = end + 1

    downloaded_total = 0
    lock = threading.Lock()
    last_update = [0.0]

    def report_progress():
        if not progress_callback:
            return
        now = time.monotonic()
        if now - last_update[0] >= PROGRESS_UPDATE_INTERVAL:
            last_update[0] = now
            try:
                progress_callback(downloaded_total, total_size)
            except Exception:
                logger.exception("Progress callback failed")

    def fetch_range(range_start: int, range_end: int):
        nonlocal downloaded_total
        offset = range_start
        attempt = 0

        while offset <= range_end:
            attempt += 1
            try:
                headers = {"Range": f"bytes={offset}-{range_end}"}
                with http_session.get(url, headers=headers, stream=True, timeout=(15, 60)) as resp:
                    resp.raise_for_status()
                    # Open a private file handle per attempt so seeks don't race.
                    with open(local_path, "r+b") as f:
                        f.seek(offset)
                        for chunk in resp.iter_content(chunk_size=STREAM_CHUNK_SIZE):
                            if not chunk:
                                continue
                            f.write(chunk)
                            offset += len(chunk)
                            with lock:
                                downloaded_total += len(chunk)
                                report_progress()
                attempt = 0  # segment finished cleanly
            except (requests.exceptions.RequestException, OSError) as e:
                if attempt >= MAX_SEGMENT_RETRIES:
                    raise
                wait = min(2 ** attempt, 30)
                logger.warning(
                    "Segment [%d-%d] hiccuped at byte %d (%s), retrying in %ds (attempt %d/%d)",
                    range_start, range_end, offset, e, wait, attempt, MAX_SEGMENT_RETRIES,
                )
                time.sleep(wait)
                # Loop resumes the GET starting at `offset` — already-written
                # bytes are kept, so we don't redownload the whole segment.

    with ThreadPoolExecutor(max_workers=num_connections) as executor:
        futures = [executor.submit(fetch_range, s, e) for s, e in ranges]
        for future in as_completed(futures):
            future.result()  # re-raise any exception from the worker thread

    if progress_callback:
        try:
            progress_callback(downloaded_total, total_size)
        except Exception:
            logger.exception("Final progress callback failed")


def download_file(url: str, dest_folder: str, progress_callback=None) -> str:
    """
    Download a file from `url` into `dest_folder`, using multiple parallel
    connections when the server supports byte-range requests (much faster
    for large files on CDNs/direct-download links), and falling back to a
    single streamed connection otherwise. Returns the local file path.

    Retries the whole operation (with backoff) up to MAX_DOWNLOAD_ATTEMPTS
    times if it fails outright (e.g. couldn't even connect). Individual
    segments already retry/resume on their own for in-flight hiccups.

    progress_callback(downloaded_bytes, total_bytes) is called periodically
    if provided, so the caller can update a "downloading..." status message.
    """
    last_error = None
    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            total_size, supports_range, filename = _probe_url(url)
            local_path = os.path.join(dest_folder, filename)

            use_parallel = supports_range and total_size >= MIN_SIZE_FOR_PARALLEL

            if use_parallel:
                logger.info(
                    "Downloading %s with %d parallel connections (%s)",
                    url, NUM_CONNECTIONS, format_bytes(total_size),
                )
                _download_parallel(url, local_path, total_size, NUM_CONNECTIONS, progress_callback)
            else:
                logger.info(
                    "Downloading %s with a single connection (range support: %s, size: %s)",
                    url, supports_range, format_bytes(total_size) if total_size else "unknown",
                )
                _download_single_stream(url, local_path, total_size, supports_range, progress_callback)

            downloaded = os.path.getsize(local_path)
            logger.info("Downloaded %s (%s bytes, expected %s)", local_path, downloaded, total_size or "unknown")
            return local_path

        except (requests.exceptions.RequestException, OSError) as e:
            last_error = e
            if attempt >= MAX_DOWNLOAD_ATTEMPTS:
                break
            wait = min(3 * attempt, 20)
            logger.warning(
                "Download attempt %d/%d failed (%s), retrying whole download in %ds",
                attempt, MAX_DOWNLOAD_ATTEMPTS, e, wait,
            )
            time.sleep(wait)

    raise last_error


# ----------------------------------------------------------------------------
# Handlers
# ----------------------------------------------------------------------------

@bot.message_handler(commands=["start", "help"])
def handle_start(message: Message):
    awaiting_link.discard(message.chat.id)
    bot.reply_to(
        message,
        "👋 Hi! I can download a file and send it to you here.\n\n"
        "Commands:\n"
        "/download - Download the configured default file\n"
        "/newlink - Give me a direct URL to download\n"
        "/help - Show this message",
        reply_markup=main_menu_markup(),
    )


@bot.message_handler(commands=["download"])
def handle_download_command(message: Message):
    awaiting_link.discard(message.chat.id)
    run_download(message.chat.id, DEFAULT_FILE_URL, reply_to=message.message_id)


@bot.message_handler(commands=["newlink"])
def handle_newlink_command(message: Message):
    awaiting_link.add(message.chat.id)
    bot.reply_to(message, "🔗 Send me the direct download URL now.")


@bot.callback_query_handler(func=lambda call: call.data in ("download_default", "provide_link"))
def handle_menu_buttons(call):
    bot.answer_callback_query(call.id)

    if call.data == "download_default":
        awaiting_link.discard(call.message.chat.id)
        run_download(call.message.chat.id, DEFAULT_FILE_URL)
    elif call.data == "provide_link":
        awaiting_link.add(call.message.chat.id)
        bot.send_message(call.message.chat.id, "🔗 Send me the direct download URL now.")


@bot.message_handler(
    func=lambda message: message.chat.id in awaiting_link,
    content_types=["text"],
)
def handle_custom_link(message: Message):
    url = message.text.strip()
    awaiting_link.discard(message.chat.id)

    if not (url.startswith("http://") or url.startswith("https://")):
        bot.reply_to(
            message,
            "⚠️ That doesn't look like a valid URL. Please try /newlink again "
            "with a link starting with http:// or https://",
        )
        return

    run_download(message.chat.id, url, reply_to=message.message_id)


def run_download(chat_id: int, url: str, reply_to: int = None):
    """Download `url` and send the resulting file to `chat_id`, with a live
    progress message (in MB) along the way."""
    status_msg = bot.send_message(
        chat_id, "⏳ Downloading file... 0.00 MB", reply_to_message_id=reply_to
    )

    state = {"last_text": None}
    progress_start = time.monotonic()

    def update_progress(downloaded: int, total: int):
        elapsed = max(time.monotonic() - progress_start, 0.001)
        speed_mbps = (downloaded / (1024 * 1024)) / elapsed  # MB/s

        if total:
            pct = downloaded * 100 // total
            bar_filled = pct // 10
            bar = "▓" * bar_filled + "░" * (10 - bar_filled)
            text = (
                f"⏳ Downloading file... {pct}%\n"
                f"[{bar}]\n"
                f"{format_mb(downloaded)} / {format_mb(total)}  •  {speed_mbps:.2f} MB/s"
            )
        else:
            text = (
                f"⏳ Downloading file...\n"
                f"{format_mb(downloaded)} downloaded  •  {speed_mbps:.2f} MB/s"
            )

        # Avoid useless "message is not modified" API errors
        if text == state["last_text"]:
            return
        state["last_text"] = text

        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=status_msg.message_id)
        except telebot.apihelper.ApiTelegramException as e:
            # Telegram complains if we edit too fast or with identical content; ignore those.
            if "message is not modified" not in str(e):
                logger.warning("Could not update progress message: %s", e)

    local_path = None
    try:
        local_path = download_file(url, DOWNLOAD_DIR, progress_callback=update_progress)
        file_size = os.path.getsize(local_path)

        if file_size > MAX_TELEGRAM_FILE_SIZE:
            bot.edit_message_text(
                f"⚠️ The file was downloaded ({format_mb(file_size)}) "
                f"but exceeds Telegram's 50 MB bot upload limit, so it can't be sent here.",
                chat_id=chat_id,
                message_id=status_msg.message_id,
            )
            return

        bot.edit_message_text(
            "✅ Download complete. Uploading to Telegram...",
            chat_id=chat_id,
            message_id=status_msg.message_id,
        )

        with open(local_path, "rb") as f:
            bot.send_document(
                chat_id,
                f,
                visible_file_name=os.path.basename(local_path),
                caption=f"📄 {os.path.basename(local_path)}",
            )

        bot.delete_message(chat_id, status_msg.message_id)
        bot.send_message(chat_id, "🎉 Done! What next?", reply_markup=main_menu_markup())

    except requests.exceptions.RequestException as e:
        logger.exception("Download failed")
        bot.edit_message_text(
            f"❌ Failed to download the file:\n<code>{e}</code>",
            chat_id=chat_id,
            message_id=status_msg.message_id,
        )
    except Exception as e:
        logger.exception("Unexpected error")
        bot.edit_message_text(
            f"❌ Something went wrong:\n<code>{e}</code>",
            chat_id=chat_id,
            message_id=status_msg.message_id,
        )
    finally:
        # Clean up local file after sending (comment this out if you want to keep a cache)
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except OSError:
                pass


def start_keepalive_server():
    """
    Some free hosting platforms (Render, Replit, Railway, etc.) put a
    process to sleep after a period of no *inbound HTTP* traffic, which
    looks like "the bot just stops after a while" even though nothing
    crashed. Binding a tiny HTTP server and pinging it externally (e.g.
    with UptimeRobot / cron-job.org every 5 minutes) keeps the process alive.

    Enable with: ENABLE_KEEPALIVE_SERVER=1 (and PORT=<port> if your host
    requires a specific one).
    """
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class _PingHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - bot is alive")

        def log_message(self, format, *args):
            pass  # silence per-request logging noise

    def _serve():
        server = ThreadingHTTPServer(("0.0.0.0", KEEPALIVE_PORT), _PingHandler)
        logger.info("Keep-alive server listening on port %d", KEEPALIVE_PORT)
        server.serve_forever()

    threading.Thread(target=_serve, daemon=True).start()


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Bot is starting...")

    if ENABLE_KEEPALIVE_SERVER:
        start_keepalive_server()

    # infinity_polling already retries on most exceptions, but on some
    # platforms (e.g. Pydroid/Termux on flaky mobile data, or hosts that
    # briefly reset idle connections) an error can still bubble up and stop
    # the loop entirely. Wrap it in our own restart loop with a heartbeat so
    # the bot always comes back up instead of dying silently, and so you can
    # see in the logs that it's still alive.
    consecutive_failures = 0
    while True:
        try:
            logger.info("Polling started.")
            bot.infinity_polling(timeout=30, long_polling_timeout=30, skip_pending=True)
            # infinity_polling only returns on a clean stop; treat that as
            # unexpected too and restart rather than exiting.
            logger.warning("Polling loop exited unexpectedly, restarting...")
            consecutive_failures = 0
        except Exception:
            consecutive_failures += 1
            wait = min(5 * consecutive_failures, 60)
            logger.exception(
                "Polling crashed (failure #%d), restarting in %ds...",
                consecutive_failures, wait,
            )
            time.sleep(wait)
