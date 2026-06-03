#!/bin/bash
# ──────────────────────────────────────────────
# Hostinger VPS one-time setup & deploy script
# Run as root or with sudo on a fresh VPS
# ──────────────────────────────────────────────
set -e

echo "==> Installing Docker & Docker Compose..."
apt-get update -qq
apt-get install -y -qq ca-certificates curl gnupg

# Docker official GPG key
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

# Docker repo
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  > /etc/apt/sources.list.d/docker.list

apt-get update -qq
apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin

echo "==> Docker installed: $(docker --version)"

# Clone or pull repo
REPO_DIR="/opt/telegram-repost-bot"
REPO_URL="https://github.com/riddler9999/aitradingbot.git"

if [ -d "$REPO_DIR" ]; then
    echo "==> Pulling latest changes..."
    cd "$REPO_DIR"
    git pull origin main
else
    echo "==> Cloning repo..."
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
fi

# Create .env if missing
if [ ! -f .env ]; then
    cp .env.example .env
    echo "==> Created .env from template — fill in your secrets:"
    echo "    nano $REPO_DIR/.env"
    echo ""
    echo "Then run:  cd $REPO_DIR && docker compose up -d"
    exit 0
fi

# Build and run
echo "==> Building and starting container..."
docker compose up -d --build

echo ""
echo "==> Done. Bot is running."
echo "    Logs:    docker compose logs -f"
echo "    Stop:    docker compose down"
echo "    Restart: docker compose restart"
