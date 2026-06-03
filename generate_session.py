#!/usr/bin/env python3
"""
Generate a Telethon StringSession for headless deployment.

Run this ONCE on your local machine:

    pip install telethon
    python generate_session.py

You will be prompted for:
  1. Your TELEGRAM_API_ID   (from https://my.telegram.org)
  2. Your TELEGRAM_API_HASH (from https://my.telegram.org)
  3. Your phone number       (+1234567890)
  4. The OTP code Telegram sends you
  5. Optional 2FA password (if you have one set)

The resulting session string is printed to stdout.
Copy it into the TELETHON_SESSION_STRING environment variable.

Security note:
  - The session string is equivalent to being logged in.
  - Treat it like a password — never commit it to version control.
"""

import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession


async def main():
    api_id = int(input("TELEGRAM_API_ID: ").strip())
    api_hash = input("TELEGRAM_API_HASH: ").strip()

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()

    session_string = client.session.save()

    print("\n" + "=" * 60)
    print("SESSION STRING (copy this entire line):")
    print("=" * 60)
    print(session_string)
    print("=" * 60)
    print("\nSet this as your TELETHON_SESSION_STRING environment variable.")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
