---
title: Telegram Video Repost Bot
emoji: 📺
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
---

# Telegram Video Repost Bot

Monitors public Telegram channels via MTProto (user account) and reposts video content to your own channel — with forward tags stripped, captions cleaned, and blacklist filtering.

## Deployment on HuggingFace Spaces

### 1. Create the Space

1. Go to [huggingface.co/new-space](https://huggingface.co/new-space)
2. Select **Docker** as the SDK
3. Push this repo to the Space (or link your GitHub repo)

### 2. Generate a session string (one-time, on your local machine)

```bash
pip install telethon
python generate_session.py
```

This will prompt for your phone number and OTP. Copy the printed session string.

### 3. Set Secrets in the Space

Go to **Settings > Repository secrets** and add:

| Secret | Value |
|---|---|
| `TELEGRAM_API_ID` | From [my.telegram.org](https://my.telegram.org) |
| `TELEGRAM_API_HASH` | From [my.telegram.org](https://my.telegram.org) |
| `TELETHON_SESSION_STRING` | Output of `generate_session.py` |
| `TELEGRAM_BOT_TOKEN` | From [@BotFather](https://t.me/BotFather) |
| `SOURCE_CHANNEL_ID` | Comma-separated channel IDs or usernames |
| `TARGET_CHANNEL_ID` | Numeric ID of your target channel |
| `BLACKLIST_KEYWORDS` | Comma-separated (optional) |
| `CUSTOM_TEXT` | Text appended to captions (optional) |

### 4. Make sure your bot is admin in the target channel

The bot (from `TELEGRAM_BOT_TOKEN`) must have **post messages** permission in the target channel.

## How it works

```
Source channel (public)
        |
        v
  User client (MTProto) listens for new posts
        |
        v
  Is it a video?  ──No──> skip
        |
       Yes
        |
        v
  Caption contains blacklisted word?  ──Yes──> skip
        |
       No
        |
        v
  Download video file
        |
        v
  Clean caption (remove @mentions, links)
        |
        v
  Bot client re-uploads to target channel
  (no forward tag — fresh upload)
```
