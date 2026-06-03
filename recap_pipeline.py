#!/usr/bin/env python3
"""
Movie Recap Automation Pipeline
================================

Takes a video, splits it into N equal parts, and for each part:
  1. Extract audio              (FFmpeg)
  2. Transcribe speech          (faster-whisper, runs on CPU)
  3. Summarise transcript       (BART via HuggingFace transformers)
  4. Generate voiceover         (Edge TTS — free, no API key)
  5. Merge narration with video (FFmpeg)

Three ways to trigger the pipeline:
  A. CLI       — python recap_pipeline.py /path/to/video.mp4
  B. Telegram  — bot auto-processes new videos in TARGET_CHANNEL_ID
  C. Webhook   — POST http://host:7861/process {"video_path":"/tmp/v.mp4"}
                  (designed for n8n "HTTP Request" node over SSH tunnel)

Resource guidance (CPU-only, no GPU)
------------------------------------
  Whisper base   ~1 GB RAM,  processes ~5× real-time on 2-core VPS
  Whisper small  ~2 GB RAM,  processes ~3× real-time
  BART-large-cnn ~1.5 GB RAM
  ─────────────────────────────────────────────────────────────
  Minimum recommended: 4 GB RAM, 2 vCPU (Hostinger KVM 2 or higher)

  Models are lazy-loaded on first request, so idle memory is low.
  Processing is sequential (one part at a time) to avoid OOM on
  small VPS instances.  Set MAX_CONCURRENT_PARTS > 1 only if you
  have ≥ 8 GB RAM and 4+ cores.

Failure modes & mitigations
-----------------------------
  • No speech detected        → part kept as-is (original audio)
  • Transcript too short      → raw transcript used instead of summary
  • TTS generation fails      → part kept as-is
  • FFmpeg split/merge fails  → logged, pipeline aborts for that video
  • Telegram FloodWait        → exponential back-off retry (up to 3×)
  • Disk full                 → temp files cleaned in finally blocks
  • OOM                       → use smaller Whisper model via env var

File naming convention
-----------------------
  /tmp/recap/<job_id>/
      source.mp4
      part_1.mp4  … part_N.mp4      (split segments)
      audio_1.wav … audio_N.wav      (extracted audio)
      narration_1.mp3 … narration_N.mp3
      recap_1.mp4 … recap_N.mp4      (final output)
  Entire job directory is deleted after successful upload.
"""

import os
import re
import sys
import asyncio
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaDocument,
    DocumentAttributeVideo,
    DocumentAttributeAnimated,
)
from aiohttp import web

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("recap")

# ---------------------------------------------------------------------------
# Configuration (environment variables)
# ---------------------------------------------------------------------------
API_ID: int = int(os.environ.get("TELEGRAM_API_ID", 0))
API_HASH: str = os.environ.get("TELEGRAM_API_HASH", "")
BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# Channel where reposted videos arrive (the bot must be admin here)
TARGET_CHANNEL_ID: int = int(os.environ.get("TARGET_CHANNEL_ID", 0))

# Channel where finished recap parts are posted (defaults to TARGET)
RECAP_CHANNEL_ID: int = int(os.environ.get("RECAP_CHANNEL_ID", 0)) or TARGET_CHANNEL_ID

# ML model selection — tune for your VPS resources
WHISPER_MODEL: str = os.environ.get("WHISPER_MODEL", "base")
SUMMARIZER_MODEL: str = os.environ.get("SUMMARIZER_MODEL", "facebook/bart-large-cnn")
TTS_VOICE: str = os.environ.get("TTS_VOICE", "en-US-ChristopherNeural")

# Pipeline settings
NUM_PARTS: int = int(os.environ.get("NUM_PARTS", 4))
WEBHOOK_PORT: int = int(os.environ.get("WEBHOOK_PORT", 7861))
WORK_DIR: Path = Path(os.environ.get("WORK_DIR", "/tmp/recap"))

MAX_RETRIES: int = 3
FLOOD_WAIT_BUFFER: int = 2

WORK_DIR.mkdir(parents=True, exist_ok=True)

# Point model caches into the persistent Docker volume so downloads
# survive container restarts (the volume is mounted at /tmp/recap).
_MODEL_CACHE = str(WORK_DIR / "models")
os.makedirs(_MODEL_CACHE, exist_ok=True)
os.environ.setdefault("HF_HOME", _MODEL_CACHE)
os.environ.setdefault("HUGGINGFACE_HUB_CACHE", _MODEL_CACHE)
os.environ.setdefault("XDG_CACHE_HOME", _MODEL_CACHE)

# ---------------------------------------------------------------------------
# Lazy-loaded ML models (initialised on first call to save idle memory)
# ---------------------------------------------------------------------------
_whisper_model = None
_summarizer = None


def get_whisper():
    """Load faster-whisper model on first use."""
    global _whisper_model
    if _whisper_model is None:
        from faster_whisper import WhisperModel

        log.info("Loading Whisper model '%s' (this may take a moment)…", WHISPER_MODEL)
        _whisper_model = WhisperModel(
            WHISPER_MODEL,
            device="cpu",
            compute_type="int8",  # quantised — halves RAM usage on CPU
        )
        log.info("Whisper model loaded.")
    return _whisper_model


def get_summarizer():
    """Load HuggingFace summarisation pipeline on first use."""
    global _summarizer
    if _summarizer is None:
        from transformers import pipeline as hf_pipeline

        log.info("Loading summariser '%s'…", SUMMARIZER_MODEL)
        _summarizer = hf_pipeline(
            "summarization",
            model=SUMMARIZER_MODEL,
            device=-1,  # -1 = CPU
        )
        log.info("Summariser loaded.")
    return _summarizer


# ═══════════════════════════════════════════════════════════════════════════
#  FFmpeg utilities
# ═══════════════════════════════════════════════════════════════════════════

def _run_ff(cmd: list[str], label: str = "ffmpeg") -> subprocess.CompletedProcess:
    """Run an FFmpeg/ffprobe command; raise on failure with stderr."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error("[%s] stderr: %s", label, result.stderr[-500:])
        result.check_returncode()  # raises CalledProcessError
    return result


def get_duration(video_path: str) -> float:
    """Return video duration in seconds."""
    r = _run_ff(
        [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path,
        ],
        label="ffprobe",
    )
    return float(r.stdout.strip())


def split_video(video_path: str, num_parts: int, output_dir: str) -> list[str]:
    """
    Split *video_path* into *num_parts* equal segments.

    Uses stream-copy (no re-encode) for speed.  Splits may be a few
    frames longer/shorter due to keyframe alignment — acceptable for
    recap purposes.
    """
    duration = get_duration(video_path)
    part_dur = duration / num_parts
    paths: list[str] = []

    for i in range(num_parts):
        start = i * part_dur
        out = os.path.join(output_dir, f"part_{i + 1}.mp4")
        _run_ff(
            [
                "ffmpeg", "-y",
                "-ss", f"{start:.3f}",
                "-i", video_path,
                "-t", f"{part_dur:.3f}",
                "-c", "copy",
                "-avoid_negative_ts", "make_zero",
                out,
            ],
            label=f"split-{i + 1}",
        )
        paths.append(out)
        log.info("Split part %d/%d  [%.1fs–%.1fs]", i + 1, num_parts, start, start + part_dur)

    return paths


def extract_audio(video_path: str, audio_path: str) -> str:
    """Extract audio as 16 kHz mono WAV (the format Whisper expects)."""
    _run_ff(
        [
            "ffmpeg", "-y",
            "-i", video_path,
            "-vn",
            "-acodec", "pcm_s16le",
            "-ar", "16000",
            "-ac", "1",
            audio_path,
        ],
        label="extract-audio",
    )
    return audio_path


def merge_narration(video_path: str, narration_path: str, output_path: str) -> str:
    """
    Replace the audio track of *video_path* with *narration_path*.

    • Video stream is copied (no re-encode).
    • Narration is encoded as AAC 128 kbps.
    • If narration is shorter than video, the remainder is silent.
    • If narration is longer, it is truncated to the video length.
    """
    dur = get_duration(video_path)
    _run_ff(
        [
            "ffmpeg", "-y",
            "-i", video_path,
            "-i", narration_path,
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "128k",
            "-t", f"{dur:.3f}",
            "-shortest",
            output_path,
        ],
        label="merge",
    )
    return output_path


# ═══════════════════════════════════════════════════════════════════════════
#  Speech-to-Text  (faster-whisper / CTranslate2)
# ═══════════════════════════════════════════════════════════════════════════

def transcribe(audio_path: str) -> str:
    """Transcribe *audio_path* and return the full text."""
    model = get_whisper()
    segments, info = model.transcribe(audio_path, beam_size=5, language=None)
    text = " ".join(seg.text.strip() for seg in segments)
    log.info(
        "Transcribed %s  lang=%s  prob=%.2f  chars=%d",
        os.path.basename(audio_path), info.language,
        info.language_probability, len(text),
    )
    return text


# ═══════════════════════════════════════════════════════════════════════════
#  Summarisation  (HuggingFace transformers — BART)
# ═══════════════════════════════════════════════════════════════════════════

def summarize(text: str, max_length: int = 150, min_length: int = 40) -> str:
    """
    Summarise *text* using the configured model.

    BART tokeniser has a 1 024-token input limit.  Longer transcripts
    are chunked, each chunk summarised, and the summaries merged.
    """
    if not text or len(text.split()) < 20:
        return text or ""

    summarizer = get_summarizer()
    max_tokens = 1024

    words = text.split()
    if len(words) <= max_tokens:
        result = summarizer(text, max_length=max_length, min_length=min_length, do_sample=False)
        return result[0]["summary_text"]

    # Chunk → summarise each → combine
    chunks: list[str] = []
    for i in range(0, len(words), max_tokens):
        chunk = " ".join(words[i : i + max_tokens])
        r = summarizer(chunk, max_length=max_length, min_length=min_length, do_sample=False)
        chunks.append(r[0]["summary_text"])

    combined = " ".join(chunks)
    if len(combined.split()) > max_tokens:
        r = summarizer(combined, max_length=max_length, min_length=min_length, do_sample=False)
        return r[0]["summary_text"]
    return combined


# ═══════════════════════════════════════════════════════════════════════════
#  Text-to-Speech  (Edge TTS — Microsoft, free, no key)
# ═══════════════════════════════════════════════════════════════════════════

async def generate_tts(text: str, output_path: str, voice: str | None = None) -> str:
    """Generate narration audio and save to *output_path*."""
    import edge_tts

    voice = voice or TTS_VOICE
    comm = edge_tts.Communicate(text, voice)
    await comm.save(output_path)
    log.info("TTS saved: %s  (%d chars)", os.path.basename(output_path), len(text))
    return output_path


# ═══════════════════════════════════════════════════════════════════════════
#  Full pipeline orchestrator
# ═══════════════════════════════════════════════════════════════════════════

async def process_video(video_path: str, job_id: str = "job") -> list[str]:
    """
    End-to-end recap pipeline.

    Returns a list of recap video file paths (one per part).
    Caller is responsible for uploading and cleaning up the job dir.

    Edge cases
    ----------
    • Silent / music-only part  → no transcript → original part kept as-is.
    • Transcript < 20 words     → raw text used as narration (no summary).
    • TTS or merge failure      → original part kept as-is.
    """
    job_dir = WORK_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    recap_videos: list[str] = []

    # ── Step 1: split video ───────────────────────────────────────────
    log.info("[%s] Splitting into %d parts…", job_id, NUM_PARTS)
    parts = split_video(video_path, NUM_PARTS, str(job_dir))

    # ── Step 2: process each part sequentially ────────────────────────
    for i, part_path in enumerate(parts, 1):
        tag = f"{job_id}/part{i}"
        log.info("[%s] Processing…", tag)

        audio_path = str(job_dir / f"audio_{i}.wav")
        tts_path = str(job_dir / f"narration_{i}.mp3")
        recap_path = str(job_dir / f"recap_{i}.mp4")

        try:
            # 2a — extract audio
            extract_audio(part_path, audio_path)

            # 2b — transcribe
            transcript = transcribe(audio_path)
            if not transcript.strip():
                log.warning("[%s] No speech detected — keeping original.", tag)
                recap_videos.append(part_path)
                continue

            # 2c — summarise
            summary = summarize(transcript)
            log.info("[%s] Summary: %.120s…", tag, summary)

            # 2d — generate voiceover
            narration_text = f"Part {i}. {summary}"
            await generate_tts(narration_text, tts_path)

            # 2e — merge narration with video
            merge_narration(part_path, tts_path, recap_path)
            recap_videos.append(recap_path)
            log.info("[%s] Done.", tag)

        except Exception:
            log.exception("[%s] Failed — falling back to original part.", tag)
            recap_videos.append(part_path)

    return recap_videos


# ═══════════════════════════════════════════════════════════════════════════
#  Telegram bot — auto-processes videos landing in TARGET_CHANNEL_ID
# ═══════════════════════════════════════════════════════════════════════════

bot_client = TelegramClient(StringSession(), API_ID, API_HASH)


def _is_video(message) -> bool:
    """True if the message contains a real video (not a GIF)."""
    media = message.media
    if not isinstance(media, MessageMediaDocument):
        return False
    doc = media.document
    if doc is None:
        return False
    has_vid = any(isinstance(a, DocumentAttributeVideo) for a in doc.attributes)
    is_gif = any(isinstance(a, DocumentAttributeAnimated) for a in doc.attributes)
    return has_vid and not is_gif


async def _upload_with_retry(file_path, caption, retries=MAX_RETRIES):
    """Upload a file to RECAP_CHANNEL_ID with FloodWait retry."""
    for attempt in range(1, retries + 1):
        try:
            await bot_client.send_file(
                RECAP_CHANNEL_ID,
                file=file_path,
                caption=caption,
                supports_streaming=True,
            )
            return
        except FloodWaitError as e:
            wait = e.seconds + FLOOD_WAIT_BUFFER
            log.warning("FloodWait on upload (attempt %d/%d) — %ds", attempt, retries, wait)
            await asyncio.sleep(wait)
        except Exception:
            log.exception("Upload attempt %d/%d failed.", attempt, retries)
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
    log.error("All upload attempts exhausted for %s.", file_path)


@bot_client.on(events.NewMessage(chats=[TARGET_CHANNEL_ID]))
async def on_target_video(event):
    """Fires when a new message appears in the target channel."""
    if not _is_video(event.message):
        return

    job_id = f"tg_{event.message.id}"
    job_dir = WORK_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    video_path = str(job_dir / "source.mp4")

    log.info("New video (msg_id=%d) — starting recap pipeline…", event.message.id)

    try:
        # Download source video
        await bot_client.download_media(event.message, file=video_path)

        # Run pipeline
        recap_parts = await process_video(video_path, job_id)

        # Upload recap parts
        for i, rp in enumerate(recap_parts, 1):
            await _upload_with_retry(rp, f"Recap — Part {i}/{len(recap_parts)}")
            await asyncio.sleep(2)  # polite delay between posts

        log.info("[%s] All %d recap parts posted.", job_id, len(recap_parts))

    except Exception:
        log.exception("[%s] Pipeline failed.", job_id)
    finally:
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════
#  HTTP webhook server  (for n8n / external triggers)
# ═══════════════════════════════════════════════════════════════════════════
#
#  n8n setup:
#    1. SSH tunnel:  ssh -L 7861:localhost:7861 user@your-vps
#    2. HTTP Request node → POST http://localhost:7861/process
#       Body: {"video_path": "/tmp/recap/my_video.mp4"}
#    3. The pipeline runs asynchronously; results are posted to Telegram.
#
#  Alternatively, use n8n's "Execute Command" node:
#    python /opt/telegram-repost-bot/recap_pipeline.py /path/to/video.mp4

async def _webhook_handler(request):
    """Accept a video path and start processing in the background."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON body"}, status=400)

    video_path = data.get("video_path")
    if not video_path or not os.path.exists(video_path):
        return web.json_response(
            {"error": "video_path is required and the file must exist on disk"},
            status=400,
        )

    job_id = data.get("job_id", f"wh_{Path(video_path).stem}")
    asyncio.create_task(_webhook_pipeline(video_path, job_id))
    return web.json_response({"status": "processing", "job_id": job_id})


async def _webhook_pipeline(video_path: str, job_id: str):
    """Background task spawned by webhook."""
    try:
        recap_parts = await process_video(video_path, job_id)

        if bot_client.is_connected():
            for i, rp in enumerate(recap_parts, 1):
                await _upload_with_retry(rp, f"Recap — Part {i}/{len(recap_parts)}")
                await asyncio.sleep(2)

        log.info("[%s] Webhook pipeline done.", job_id)
    except Exception:
        log.exception("[%s] Webhook pipeline failed.", job_id)
    finally:
        job_dir = WORK_DIR / job_id
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)


async def _health(_request):
    return web.Response(text="Recap pipeline running.")


async def start_webhook_server():
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/health", _health)
    app.router.add_post("/process", _webhook_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEBHOOK_PORT)
    await site.start()
    log.info("Webhook server listening on port %d.", WEBHOOK_PORT)


# ═══════════════════════════════════════════════════════════════════════════
#  Entry points
# ═══════════════════════════════════════════════════════════════════════════

async def cli_main(video_path: str):
    """CLI mode — process a single file, print output paths."""
    job_id = f"cli_{Path(video_path).stem}"
    parts = await process_video(video_path, job_id)
    for i, p in enumerate(parts, 1):
        print(f"Part {i}: {p}")
    return parts


async def daemon_main():
    """Daemon mode — Telegram listener + webhook server."""
    missing: list[str] = []
    if not API_ID:
        missing.append("TELEGRAM_API_ID")
    if not API_HASH:
        missing.append("TELEGRAM_API_HASH")
    if not BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TARGET_CHANNEL_ID:
        missing.append("TARGET_CHANNEL_ID")
    if missing:
        log.error("Missing env vars: %s", ", ".join(missing))
        sys.exit(1)

    await start_webhook_server()

    log.info("Connecting bot client…")
    await bot_client.start(bot_token=BOT_TOKEN)
    me = await bot_client.get_me()
    log.info("Bot: @%s (id=%d)", me.username, me.id)
    log.info("Watching channel %s for new videos.", TARGET_CHANNEL_ID)
    log.info("Recap output channel: %s", RECAP_CHANNEL_ID)
    log.info("Whisper=%s  Summariser=%s  Voice=%s", WHISPER_MODEL, SUMMARIZER_MODEL, TTS_VOICE)
    log.info("Pipeline ready.  Ctrl+C to stop.")

    await bot_client.run_until_disconnected()


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            # CLI: python recap_pipeline.py /path/to/video.mp4
            path = sys.argv[1]
            if not os.path.exists(path):
                log.error("File not found: %s", path)
                sys.exit(1)
            asyncio.run(cli_main(path))
        else:
            # Daemon: Telegram monitor + webhook
            asyncio.run(daemon_main())
    except KeyboardInterrupt:
        log.info("Shutting down.")
