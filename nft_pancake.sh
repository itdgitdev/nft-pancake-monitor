#!/bin/bash
# ============ GLOBAL SETTINGS ============
set -e  # dừng script nếu có lệnh lỗi
LOG_DIR="/home/dev/nft_pancake_app/logs"
mkdir -p "$LOG_DIR"

# ============ JOB 1: FETCH DATA ============
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting fetch data..." >> "$LOG_DIR/master.log"
python3.13 /home/dev/nft_pancake_app/flask_app/run_auto_fetch_data.py >> "$LOG_DIR/fetch_data_2.log" 2>&1

# Nghỉ 3 phút
sleep 180

# ============ JOB 2: SOLANA ============
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Updating Sol pool info..." >> "$LOG_DIR/master.log"
sh /home/dev/nft_pancake_app/latest_farms/update_pool_sol_info.sh >> "$LOG_DIR/update_pool_sol_info.log" 2>&1

sleep 60

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Monitoring Sol latest farms..." >> "$LOG_DIR/master.log"
sh /home/dev/nft_pancake_app/latest_farms/monitor_latest_farms.sh >> "$LOG_DIR/active_farm_sol.log" 2>&1

# ============ JOB 3: EVM ============
sleep 60
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Updating EVM pool info..." >> "$LOG_DIR/master.log"
sh /home/dev/nft_pancake_app/latest_farms/update_pool_info.sh >> "$LOG_DIR/update_pool_evm_info_v2.log" 2>&1

# ============ JOB 4: FETCH TX HISTORY ============
sleep 60
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Fetching wallet transaction history..." >> "$LOG_DIR/master.log"
python3.13 /home/dev/nft_pancake_app/flask_app/run_auto_storage_transaction.py >> "$LOG_DIR/fetch_token_tx_history.log" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] All jobs finished." >> "$LOG_DIR/master.log"
