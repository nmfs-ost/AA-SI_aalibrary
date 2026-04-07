#!/bin/bash
# STAGE 1: Initialization & Setup
overall_start_time=$(date +%s)
stage_1_start_time=$(date +%s)

set -e  # Exit immediately if any command fails

echo "🔊🐟 Initializing..."

# --- Ensure the home directory exists ---
echo "🏚️ Preparing workstation setup at $HOME..."
mkdir -p "$HOME"
echo "🛠️ Base station ($HOME) is operational."
stage_1_end_time=$(date +%s)
stage_1_duration=$((stage_1_end_time - stage_1_start_time))

#!/usr/bin/env bash
# STAGE 2: HELM CHECK
stage_2_start_time=$(date +%s)
set -euo pipefail

BAD_REPO="https://baltocdn.com/helm/stable/debian"

echo "🔎 Checking for bad Helm repo: $BAD_REPO"

FOUND_FILE=$(grep -Rl "$BAD_REPO" /etc/apt/sources.list /etc/apt/sources.list.d/ || true)

if [[ -n "$FOUND_FILE" ]]; then
    echo "⚠️  Found bad Helm repo in: $FOUND_FILE"
    echo "🗑️  Removing it..."
    sudo rm -f "$FOUND_FILE"
    echo "✅ Removed $FOUND_FILE"
else
    echo "✅ No bad Helm repo found."
fi
stage_2_end_time=$(date +%s)
stage_2_duration=$((stage_2_end_time - stage_2_start_time))


# STAGE 3: SYSTEM UPDATE
stage_3_start_time=$(date +%s)
sudo apt update
sudo apt upgrade -y
stage_3_end_time=$(date +%s)
stage_3_duration=$((stage_3_end_time - stage_3_start_time))

# STAGE 4: PYTHON 3.12 INSTALLATION
stage_4_start_time=$(date +%s)
# Helper: check if python3.12 exists AND has ensurepip
has_ensurepip() {
  # Does python3.12 exist?
  if ! command -v python3.12 >/dev/null 2>&1; then
    return 1
  fi

  # Does this python3.12 have ensurepip?
  if python3.12 -m ensurepip --version >/dev/null 2>&1; then
    return 0
  fi

  return 1
}

# Check for usable python3.12
if has_ensurepip; then
  echo "✅ Usable Python 3.12 found at: $(command -v python3.12)"
else
  echo "⬇️ Python 3.12 (with ensurepip) not found. Installing Python 3.12.3 to \$HOME/python312..."

  # Step 1: Install build dependencies
  sudo apt update
  sudo apt install -y build-essential libssl-dev zlib1g-dev \
    libncurses5-dev libncursesw5-dev libreadline-dev libsqlite3-dev \
    libgdbm-dev libdb5.3-dev libbz2-dev libexpat1-dev liblzma-dev \
    tk-dev uuid-dev libffi-dev wget

  # Step 2: Download and extract Python 3.12.3 source
  cd ~
  wget https://www.python.org/ftp/python/3.12.3/Python-3.12.3.tgz
  tar -xzf Python-3.12.3.tgz
  cd Python-3.12.3

  # Step 3: Configure with prefix to install in home
  ./configure --prefix="$HOME/python312" --enable-optimizations

  # Step 4: Build and install
  make -j"$(nproc)"
  make install

  # Step 5: Add to PATH
  echo 'export PATH="$HOME/python312/bin:$PATH"' >> ~/.bashrc
  export PATH="$HOME/python312/bin:$PATH"
  echo "✅ Python 3.12.3 installed to \$HOME/python312"
fi
stage_4_end_time=$(date +%s)
stage_4_duration=$((stage_4_end_time - stage_4_start_time))

# STAGE 5: VIRTUAL ENVIRONMENT SETUP
stage_5_start_time=$(date +%s)
# Step 6: Create and activate virtual environment
if [ ! -d "$HOME/venv312" ]; then
  python3.12 -m venv "$HOME/venv312"
  echo "✅ Created virtual environment at ~/venv312"
else
  echo "✅ Virtual environment already exists at ~/venv312"
fi
stage_5_end_time=$(date +%s)
stage_5_duration=$((stage_5_end_time - stage_5_start_time))
# STAGE 6: ACTIVATE VIRTUAL ENVIRONMENT
stage_6_start_time=$(date +%s)
# Step 7: Activate the virtual environment
source ~/venv312/bin/activate
echo "✅ Activated virtual environment. Python version: $(python --version)"



# --- Copy files from /opt to $HOME if /opt is not empty ---
if [ -d /opt ] && [ "$(ls -A /opt)" ]; then
    echo "📦 /opt sonar payload detected. Transferring to base station..."

    shopt -s dotglob  # Include hidden files (like camouflaged cephalopods 🦑)
    # cp -r /opt/aa-scripts "$HOME"/
    # cp -r /opt/google-cloud-login.sh "$HOME"/
    shopt -u dotglob

    echo "🎯 Payload deployed to $HOME — assets ready."
else
    echo "🛑 /opt empty — no acoustic data to transfer."
fi
stage_6_end_time=$(date +%s)
stage_6_duration=$((stage_6_end_time - stage_6_start_time))


# STAGE 7: PYTHON PACKAGE INSTALLATIONS
stage_7_start_time=$(date +%s)
pip install --upgrade pip
pip install pyworms matplotlib toml
echo "🎣 Installing AA-SI_aalibrary (active signal interpretation)..."
pip install --no-cache-dir -vv --force-reinstall git+https://github.com/nmfs-ost/AA-SI_aalibrary

echo "🐡 Installing echoml (echo classification & ML)..."
pip install --no-cache-dir -vv --force-reinstall git+https://github.com/nmfs-ost/AA-SI_KMeans

echo "🦈 Installing echosms (system management for sonar ops)..."
pip install echosms echoregions

pip install ipykernel
python -m ipykernel install --user --name=venv312 --display-name "venv312"
stage_7_end_time=$(date +%s)
stage_7_duration=$((stage_7_end_time - stage_7_start_time))
# STAGE 8: FINALIZATION
stage_8_start_time=$(date +%s)
# --- Final instructions ---
echo "📡 AA-SI environment is live and ready for use."
echo "🔁 Navigate to home directory with: cd"
echo "🧭 Review transferred files and verify AA-SI readiness. Enter 'aa-help' for a command reference with examples."
stage_8_end_time=$(date +%s)
stage_8_duration=$((stage_8_end_time - stage_8_start_time))
overall_end_time=$(date +%s)
overall_duration=$((overall_end_time - overall_start_time))
echo "⏱️ Stage 1 elapsed time: $stage_1_duration seconds"
echo "⏱️ Stage 2 elapsed time: $stage_2_duration seconds"
echo "⏱️ Stage 3 elapsed time: $stage_3_duration seconds"
echo "⏱️ Stage 4 elapsed time: $stage_4_duration seconds"
echo "⏱️ Stage 5 elapsed time: $stage_5_duration seconds"
echo "⏱️ Stage 6 elapsed time: $stage_6_duration seconds"
echo "⏱️ Stage 7 elapsed time: $stage_7_duration seconds"
echo "⏱️ Stage 8 elapsed time: $stage_8_duration seconds"
echo "⏱️ Total elapsed time: $overall_duration seconds"