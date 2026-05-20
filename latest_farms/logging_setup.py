import logging 
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logger(name, log_file, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.hasHandlers():
        return logger

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=3, encoding='utf-8')
    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False  # không lan sang root logger
    return logger

# --- Tạo các logger riêng ---
active_farm_sol_logger = setup_logger("ACTIVE_FARM_SOL", "logs/active_farm_sol.log")
pool_sol_info_logger = setup_logger("POOL_INFO", "logs/update_pool_sol_info.log")
pool_evm_info_logger = setup_logger("POOL_EVM_INFO", "logs/update_pool_evm_info_v2.log")
token_cmc_map_logger = setup_logger("TOKEN_CMC_MAP", "logs/fetch_token_cmc_id.log")
aerodrome_evm_pool_info_logger = setup_logger("AERODROME_EVM_POOL_INFO", "logs/aerodrome_dex_fetch_pool_info.log")

# --- Parasite Bot loggers ---
parasite_detect_logger = setup_logger("PARASITE_DETECT", "logs/parasite_detect_zombie.log")
parasite_exec_logger = setup_logger("PARASITE_EXEC", "logs/parasite_execution.log")
parasite_rebalance_logger = setup_logger("PARASITE_REBALANCE", "logs/parasite_rebalance.log")
parasite_tick_history_logger = setup_logger("PARASITE_TICK_HISTORY", "logs/parasite_tick_history.log")