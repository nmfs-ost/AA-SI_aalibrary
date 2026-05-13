#!/bin/bash

# --- Configuration & Colors ---
GREEN='\033[0;32m'
GRAY='\033[90m' 
NC='\033[0m' 
CHECKMARK="${GREEN}✔${NC}"

# --- Progress Bar Function ---
draw_progress_bar() {
    local current=$1
    local total=$2
    local task=$3
    local width=40
    
    if [ "$total" -eq 0 ]; then total=1; fi
    
    local percentage=$((current * 100 / total))
    local completed=$((width * current / total))
    local remaining=$((width - completed))

    # \r moves to start, \033[K clears the line
    printf "\r\033[K${GREEN}Progress: [%-${width}s] %d%%${NC}  ${GRAY}%s${NC}" \
        "$(printf "%${completed}s" | tr ' ' '=')" \
        "$percentage" \
        "$task"
}

# --- UI Header ---
clear
echo -e "${GREEN}"
cat << "EOF"
  __________________________________________________
 /                                                  \
|      ________________________ _ _____________     |
|     / ___|  ___| ___||  ___| | | / ___|| ____|    |
|    | |  _| |   \___ \| |_  | | | \___ \|  _|      |
|    | |_| | |___|__)  |  _| | |_| |___) | |___      |
|     \____|_____|____/|_|    \___/|____/|_____|    |
|                                                   |
|         GCS FUSE INSTALLER - CLOUD STORAGE        |
 \__________________________________________________/
EOF
echo -e "${NC}"

# --- Main Logic ---
total_steps=6
current_step=0

# Navigate to home directory
cd ~ || exit

# Step 1: System Update
draw_progress_bar $current_step $total_steps "Updating package lists..."
sudo apt-get update -y > /dev/null 2>&1
current_step=$((current_step + 1))
echo -e "\r\033[K${CHECKMARK} System package list updated."

# Step 2: Install Dependencies
draw_progress_bar $current_step $total_steps "Installing curl and gnupg..."
# Added -q (quiet) and redirected output to ensure no tables appear
sudo apt-get install -y -q curl gnupg lsb-release > /dev/null 2>&1
current_step=$((current_step + 1))
echo -e "\r\033[K${CHECKMARK} Dependencies (curl, gnupg, lsb-release) installed."

# Step 3: Add GCSFuse Repo URL
draw_progress_bar $current_step $total_steps "Configuring repository..."
export GCSFUSE_REPO=gcsfuse-$(lsb_release -c -s)
echo "deb https://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list > /dev/null 2>&1
current_step=$((current_step + 1))
echo -e "\r\033[K${CHECKMARK} GCSFuse repository added to sources."

# Step 4: Import Public Key & Refresh
draw_progress_bar $current_step $total_steps "Importing Google Cloud keys..."
# Added -s (silent) to curl to prevent the download progress meter
curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add - > /dev/null 2>&1
sudo apt-get update -y > /dev/null 2>&1
current_step=$((current_step + 1))
echo -e "\r\033[K${CHECKMARK} Google Cloud public key imported."

# Step 5: Install GCSFuse
draw_progress_bar $current_step $total_steps "Downloading and installing gcsfuse..."
# Added -q and redirected output to keep the UI clean
sudo apt-get install -y -q gcsfuse > /dev/null 2>&1
current_step=$((current_step + 1))
echo -e "\r\033[K${CHECKMARK} GCSFuse installation complete."

# Step 6: Mounting prod bucket as read-only
draw_progress_bar $current_step $total_steps "Creating mount dir & mounting prod bucket as read-only..."
# Create mount point if it doesn't exist
MOUNT_POINT="ggn-nmfs-aa-prod-1-data"
mkdir -p "$MOUNT_POINT"
# Mount the bucket with read-only permissions
gcsfuse -o ro ggn-nmfs-aa-prod-1-data "$MOUNT_POINT" > /dev/null 2>&1
current_step=$((current_step + 1))
echo -e "\r\033[K${CHECKMARK} Prod bucket mounted at $MOUNT_POINT with read-only permissions."

# Final Bar State
draw_progress_bar $current_step $total_steps "Done!"

# --- Finalization ---
gcsfuse -v
echo -e "\n\n${GREEN}Installation Successful!${NC}"