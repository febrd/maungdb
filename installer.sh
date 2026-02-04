#!/bin/bash

echo "🐯 Installing MaungDB..."

# 1. Build Binary
echo "🔨 Building binary..."
go build -ldflags "-X main.Version=v1.0.0" -o maung ./cmd/maung

# 2. Pindahkan ke /usr/local/bin (Supaya bisa dipanggil dimanapun)
echo "📦 Moving to /usr/local/bin..."
sudo mv maung /usr/local/bin/

# 3. Init Data Directory (Optional, biar folder datanya aman)
mkdir -p ~/maung_data
chmod 777 ~/maung_data

echo "✅ MaungDB Installed! Ketik 'maung' untuk memulai."
