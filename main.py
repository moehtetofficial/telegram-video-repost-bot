#!/usr/bin/env python3
"""
Telegram Video Repost Bot (MTProto + Bot API)
==============================================

Architecture
------------
1. **User client (Telethon / MTProto)** — monitors one or more public
   Telegram channels.  A user account is required because the Bot API
   cannot read channels where the bot is not an admin.  MTProto has no
   such limitation: any channel that is publicly accessible (or that the
   user account has joined) can be read.

2. **Bot client (Telethon, using a Bot Token)** — posts to the target
   channel where the bot has admin/post permissions.

Why re-upload instead of forward/copy?
--------------------------------------
Telegram's `forwardMessages` RPC always attaches a "Forwarded from …"
header.  The Bot API's `copyMessage` removes the visual forward header
but **still embeds the source channel reference** in the message metadata
(visible in some clients and via the API).  The **only** reliable way to
strip ALL forward attribution is to:

    1. Download the media file from the source channel.
    2. Upload it as a **brand-new** message through the bot.

This is the industry-standard workaround used by every repost bot.

Protected channels
------------------
Channels with "Restrict saving content" enabled block
`messages.getMessages` / media download for normal users.  Telethon will
raise `ChatForwardsRestrictedError`.  We catch this and log a warning
because there is no API-level bypass (intentionally so by Telegram).

FloodWait
---------
Telegram rate-limits all API calls.  When a FloodWaitError is raised the
bot sleeps for the required duration (plus a small buffer) and retries,
up to MAX_RETRIES times.

HuggingFace Spaces compatibility
---------------------------------
HF Spaces kills processes that don't bind to port 7860 within ~5 min.
A lightweight `aiohttp` health-check server runs on that port so the
Space stays alive.
"""

import os
import re
import sys
import asyncio
import logging
import tempfile
from pathlib import Path
from threading import Thread

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, ChatForwardsRestrictedError
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaDocument,
    DocumentAttributeVideo,
    DocumentAttributeAnimated,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("repost-bot")

# ---------------------------------------------------------------------------
# Configuration — every setting comes from environment variables
# ---------------------------------------------------------------------------
API_ID: int = int(os.environ.get("TELEGRAM_API_ID", 0))
API_HASH: str = os.environ.get("TELEGRAM_API_HASH", "")

# Telethon session string (generated once via generate_session.py)
SESSION_STRING: str = os.environ.get("TELETHON_SESSION_STRING", "")

# Standard Bot API token for the posting bot
BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# Numeric chat ID of the target channel (bot must be admin there)
TARGET_CHANNEL_ID: int = int(os.environ.get("TARGET_CHANNEL_ID", 0))

# Comma-separated list of source channels.
# Accepts numeric IDs (-100…) or usernames (with or without @).
_raw_sources: str = os.environ.get("SOURCE_CHANNEL_ID", "")
SOURCE_CHANNELS: list = []
for _ch in _raw_sources.split(","):
    _ch = _ch.strip()
    if not _ch:
        continue
    try:
        SOURCE_CHANNELS.append(int(_ch))
    except ValueError:
        SOURCE_CHANNELS.append(_ch.lstrip("@"))

# Comma-separated blacklist keywords — case-insensitive matching
BLACKLIST_KEYWORDS: list[str] = [
    kw.strip().lower()
    for kw in os.environ.get("BLACKLIST_KEYWORDS", "").split(",")
    if kw.strip()
]

# Optional text appended to every reposted caption
CUSTOM_TEXT: str = os.environ.get("CUSTOM_TEXT", "")

# Health-check server port (HuggingFace Spaces expects 7860)
HEALTH_PORT: int = int(os.environ.get("PORT", 7860))

# Retry settings
MAX_RETRIES: int = 3
FLOOD_WAIT_BUFFER: int = 2  # extra seconds added to Telegram's wait time


# ---------------------------------------------------------------------------
# Caption cleaning
# ---------------------------------------------------------------------------
def clean_caption(text: str | None) -> str | None:
    """
    Sanitise a post caption:
      - Remove @username mentions
      - Remove t.me/ links
      - Remove all http/https URLs
      - Collapse excessive blank lines
      - Optionally append CUSTOM_TEXT
    Returns None if the result is empty and no CUSTOM_TEXT is set.
    """
    if not text:
        return CUSTOM_TEXT or None

    text = re.sub(r"@[\w]+", "", text)              # @username
    text = re.sub(r"https?://t\.me/\S*", "", text)  # t.me links
    text = re.sub(r"https?://\S*", "", text)         # any remaining URLs
    text = re.sub(r"\n{3,}", "\n\n", text)           # collapse newlines
    text = text.strip()

    if CUSTOM_TEXT:
        text = f"{text}\n\n{CUSTOM_TEXT}" if text else CUSTOM_TEXT

    return text if text else None


def is_blacklisted(text: str | None) -> bool:
    """Return True if *text* contains any blacklisted keyword."""
    if not text or not BLACKLIST_KEYWORDS:
        return False
    lower = text.lower()
    return any(kw in lower for kw in BLACKLIST_KEYWORDS)


# ---------------------------------------------------------------------------
# Video detection
# ---------------------------------------------------------------------------
def message_has_video(message) -> bool:
    """
    Return True if the Telethon message contains a real video.
    Excludes GIFs (DocumentAttributeAnimated) and photos.
    """
    media = message.media
    if not isinstance(media, MessageMediaDocument):
        return False
    doc = media.document
    if doc is None:
        return False
    has_video = any(isinstance(a, DocumentAttributeVideo) for a in doc.attributes)
    is_gif = any(isinstance(a, DocumentAttributeAnimated) for a in doc.attributes)
    return has_video and not is_gif


# ---------------------------------------------------------------------------
# Telethon clients
# ---------------------------------------------------------------------------
# User client — reads source channels via MTProto (no admin needed)
user_client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH,
    device_model="VideoRepostBot",
    system_version="1.0",
)

# Bot client — posts to target channel (bot must be admin there)
bot_client = TelegramClient(
    StringSession(),   # ephemeral session; re-authenticates each start
    API_ID,
    API_HASH,
)


# ---------------------------------------------------------------------------
# Download / upload with retry + FloodWait handling
# ---------------------------------------------------------------------------
async def _download_with_retry(message, retries: int = MAX_RETRIES) -> str | None:
    """Download media from *message* into a temp .mp4 file. Returns path or None."""
    for attempt in range(1, retries + 1):
        try:
            path = await user_client.download_media(
                message,
                file=tempfile.mktemp(suffix=".mp4"),
            )
            return path
        except FloodWaitError as e:
            wait = e.seconds + FLOOD_WAIT_BUFFER
            log.warning(
                "FloodWait on download (attempt %d/%d) — sleeping %ds.",
                attempt, retries, wait,
            )
            await asyncio.sleep(wait)
        except Exception:
            log.exception("Download attempt %d/%d failed.", attempt, retries)
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
    return None


async def _upload_with_retry(
    file_path: str,
    caption: str | None,
    original_message,
    retries: int = MAX_RETRIES,
) -> None:
    """
    Upload *file_path* to the target channel via the bot client.

    Re-uploading (instead of forwarding or copying) is the ONLY way to
    guarantee that no forward attribution metadata survives.  Telegram's
    API does not expose a flag to strip the "forwarded from" header.
    """
    # Preserve original video attributes (duration, dimensions) so
    # Telegram renders the file as a proper video, not a generic document.
    attrs: list = []
    if original_message.media and original_message.media.document:
        for a in original_message.media.document.attributes:
            if isinstance(a, DocumentAttributeVideo):
                attrs.append(a)

    for attempt in range(1, retries + 1):
        try:
            await bot_client.send_file(
                TARGET_CHANNEL_ID,
                file=file_path,
                caption=caption,
                attributes=attrs or None,
                supports_streaming=True,
            )
            return
        except FloodWaitError as e:
            wait = e.seconds + FLOOD_WAIT_BUFFER
            log.warning(
                "FloodWait on upload (attempt %d/%d) — sleeping %ds.",
                attempt, retries, wait,
            )
            await asyncio.sleep(wait)
        except Exception:
            log.exception("Upload attempt %d/%d failed.", attempt, retries)
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)

    log.error("All %d upload attempts failed for %s.", retries, file_path)


# ---------------------------------------------------------------------------
# Core event handler
# ---------------------------------------------------------------------------
@user_client.on(events.NewMessage(chats=SOURCE_CHANNELS))
async def on_new_post(event):
    """Fires for every new message in any monitored source channel."""
    message = event.message

    # ── Only process video posts ──────────────────────────────────────
    if not message_has_video(message):
        return

    raw_caption: str = message.text or message.message or ""

    # ── Blacklist filter (checked against the ORIGINAL caption) ───────
    if is_blacklisted(raw_caption):
        log.info("Skipped post %d — blacklisted keyword.", message.id)
        return

    caption = clean_caption(raw_caption)

    # ── Download → re-upload pipeline ─────────────────────────────────
    tmp_path: str | None = None
    try:
        tmp_path = await _download_with_retry(message)
        if tmp_path is None:
            log.warning("Could not download video from post %d.", message.id)
            return

        await _upload_with_retry(tmp_path, caption, message)
        log.info("Reposted post %d to target channel.", message.id)

    except ChatForwardsRestrictedError:
        # The source channel has "Restrict saving content" enabled.
        # Telethon cannot download media from these channels — this is
        # enforced server-side and there is no legitimate workaround.
        log.warning(
            "Post %d is from a protected channel; download blocked by Telegram.",
            message.id,
        )
    except Exception:
        log.exception("Unexpected error processing post %d.", message.id)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)


# ---------------------------------------------------------------------------
# Health-check HTTP server (keeps HuggingFace Spaces alive)
# ---------------------------------------------------------------------------
async def _health_server():
    """Minimal HTTP server that responds 200 OK on any request."""
    from aiohttp import web

    async def _handle(_request):
        return web.Response(text="Telegram Repost Bot is running.")

    app = web.Application()
    app.router.add_get("/", _handle)
    app.router.add_get("/health", _handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    log.info("Health-check server listening on port %d.", HEALTH_PORT)


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
async def main():
    # ── Validate required environment variables ───────────────────────
    missing: list[str] = []
    if not API_ID:
        missing.append("TELEGRAM_API_ID")
    if not API_HASH:
        missing.append("TELEGRAM_API_HASH")
    if not SESSION_STRING:
        missing.append("TELETHON_SESSION_STRING")
    if not BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TARGET_CHANNEL_ID:
        missing.append("TARGET_CHANNEL_ID")
    if not SOURCE_CHANNELS:
        missing.append("SOURCE_CHANNEL_ID")

    if missing:
        log.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    # ── Start health-check server (HuggingFace Spaces) ───────────────
    await _health_server()

    # ── Connect both clients ──────────────────────────────────────────
    log.info("Connecting user client (MTProto)…")
    await user_client.start()

    log.info("Connecting bot client…")
    await bot_client.start(bot_token=BOT_TOKEN)

    user_me = await user_client.get_me()
    bot_me = await bot_client.get_me()
    log.info(
        "User client: %s (id=%d)", user_me.first_name, user_me.id,
    )
    log.info(
        "Bot client: @%s (id=%d)", bot_me.username, bot_me.id,
    )
    log.info(
        "Monitoring %d source channel(s): %s",
        len(SOURCE_CHANNELS), SOURCE_CHANNELS,
    )
    log.info("Target channel: %s", TARGET_CHANNEL_ID)
    log.info("Blacklist: %s", BLACKLIST_KEYWORDS or "(none)")
    log.info("Custom text: %r", CUSTOM_TEXT or "(none)")
    log.info("Bot is running. Press Ctrl+C to stop.")

    # ── Block until disconnected ──────────────────────────────────────
    await user_client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")
