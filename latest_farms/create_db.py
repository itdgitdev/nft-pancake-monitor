import mysql.connector
import gc
import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
env = os.getenv("ENV", "local")

if env == "local":
    DB_CONFIG = {
        "host": os.getenv("LOCAL_DB_HOST"),
        "user": os.getenv("LOCAL_DB_USER"),
        "password": os.getenv("LOCAL_DB_PASS"),
        "database": os.getenv("LOCAL_DB_NAME"),
        "port": int(os.getenv("LOCAL_DB_PORT", 3306)),
        "ssl_disabled": os.getenv("LOCAL_DB_SSL_DISABLED", "true").lower() == "true"
    }
else:
    DB_CONFIG = {
        "host": os.getenv("SERVER_DB_HOST"),
        "user": os.getenv("SERVER_DB_USER"),
        "password": os.getenv("SERVER_DB_PASS"),
        "database": os.getenv("SERVER_DB_NAME"),
        "port": int(os.getenv("SERVER_DB_PORT", 3306)),
        "ssl_disabled": os.getenv("SERVER_DB_SSL_DISABLED", "true").lower() == "true"
    }

# Connect to MySQL database
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

# Create database and table if not exist
def create_database_and_table():
    temp_config = DB_CONFIG.copy()
    temp_config.pop("database")

    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute("SHOW DATABASES")
        databases = [db[0] for db in cursor.fetchall()]

        if DB_CONFIG['database'] in databases:
            print(f"Database {DB_CONFIG['database']} already exists.")
        else:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            print(f"Database {DB_CONFIG['database']} newly created")
        
        # Clear database list after use
        del databases  

        connection.database = DB_CONFIG['database']

        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]

        if "farm_state" in tables:
            print("Table farm_state already exists.")
        else:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS farm_state(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    pid INT NOT NULL,
                    chain VARCHAR(10) NOT NULL,
                    v3Pool VARCHAR(100) NOT NULL,
                    fee FLOAT NOT NULL,
                    alloc_point BIGINT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
            cursor.execute(create_table_query)
            print("Table farm_state newly created")
        
        if "farm_state_sol" in tables:
            print("Table farm_state_sol already exists.")
        else:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS farm_state_sol(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    chain VARCHAR(10) NOT NULL,
                    pool_account VARCHAR(100) NOT NULL,
                    reward_idx INT NOT NULL,
                    token_mint VARCHAR(100) NOT NULL,
                    reward_state INT NOT NULL,
                    open_time BIGINT NOT NULL,
                    end_time BIGINT NOT NULL,
                    reward_claimed BIGINT NOT NULL,
                    reward_total_emissioned BIGINT NOT NULL,
                    token_reward_symbol VARCHAR(50) NOT NULL,
                    token_reward_decimals INT NOT NULL,
                    weekly_rewards DOUBLE NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(create_table_query)
            print("Table farm_state_sol newly created")
        
        if "pool_info" in tables:
            # Migration: add narrow range bot detection columns
            for _col_name, _col_def in [
                ("narrow_range_count",   "INT DEFAULT 0"),
                ("narrow_range_tvl_usd", "DOUBLE DEFAULT 0.0"),
                ("has_narrow_bot_flag",  "BOOLEAN DEFAULT FALSE"),
                ("farm_apr",             "DOUBLE DEFAULT 0.0"),
            ]:
                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'pool_info'
                      AND COLUMN_NAME = %s
                """, (_col_name,))
                _row = cursor.fetchone()
                if (_row[0] if isinstance(_row, tuple) else _row['cnt']) == 0:
                    cursor.execute(f"ALTER TABLE pool_info ADD COLUMN {_col_name} {_col_def}")
                    connection.commit()
                    print(f"Migration: added column pool_info.{_col_name}")
            print("Table pool_info already exists.")
        else:
            create_table_query ="""
                    CREATE TABLE IF NOT EXISTS pool_info (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        chain VARCHAR(10) NOT NULL,
                        pool_address VARCHAR(42) NOT NULL UNIQUE,
                        token0_address VARCHAR(42),
                        token1_address VARCHAR(42),
                        token0_symbol VARCHAR(20),
                        token1_symbol VARCHAR(20),
                        token0_decimals INT,
                        token1_decimals INT,
                        fee INT,
                        alloc_point INT,
                        pid INT,
                        cake_per_day DOUBLE,
                        total_value_lock DOUBLE DEFAULT 0.0,
                        cake_reward_1h FLOAT DEFAULT 0.0,
                        total_current_liquidity DOUBLE DEFAULT 0.0,
                        total_staked_liquidity DOUBLE DEFAULT 0.0,
                        total_inactive_staked_liquidity DOUBLE DEFAULT 0.0, 
                        is_stake_tracked BOOLEAN DEFAULT FALSE,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    );
            """
            cursor.execute(create_table_query)
            print("Table pool_info newly created")
        
        if "pool_sol_info" in tables:
            print("Table pool_info already exists.")
        else:
            create_table_query ="""
                CREATE TABLE IF NOT EXISTS pool_sol_info (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    chain VARCHAR(10) NOT NULL,
                    pool_account VARCHAR(100) NOT NULL UNIQUE,
                    token0_mint VARCHAR(100),
                    token1_mint VARCHAR(100),
                    token0_symbol VARCHAR(50),
                    token1_symbol VARCHAR(50),
                    token0_decimals INT,
                    token1_decimals INT,
                    reward_state INT NOT NULL,
                    open_time BIGINT NOT NULL,
                    end_time BIGINT NOT NULL,
                    fee INT DEFAULT 0,
                    reward_claimed BIGINT NOT NULL,
                    reward_total_emissioned BIGINT NOT NULL,
                    reward_account VARCHAR(60) NOT NULL,
                    reward_symbol VARCHAR(20) NOT NULL,
                    weekly_rewards DOUBLE NOT NULL DEFAULT 0.0,
                    cake_reward_1h FLOAT DEFAULT 0.0,
                    total_valid_liquidity DOUBLE DEFAULT 0.0,
                    total_inactive_staked_liquidity DOUBLE DEFAULT 0.0,
                    total_current_liquidity DOUBLE DEFAULT 0.0,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(create_table_query)
            print("Table pool_sol_info newly created")
        
        if "token_cmc_map" in tables:
            print("Table token_cmc_map already exists.")
        else:
            create_table_query ="""
                CREATE TABLE IF NOT EXISTS token_cmc_map (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    token_address VARCHAR(100) UNIQUE,
                    chain VARCHAR(20),
                    cmc_id VARCHAR(20),
                    symbol VARCHAR(50),
                    name VARCHAR(100),
                    last_updated DATETIME
                )
            """
            cursor.execute(create_table_query)
            print("Table token_cmc_map newly created")

        # ── Parasite Bot Tables ──

        if "detected_pools" not in tables:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS detected_pools (
                    id                          INT AUTO_INCREMENT PRIMARY KEY,
                    chain                       VARCHAR(10)  NOT NULL,
                    pool_address                VARCHAR(42)  NOT NULL,
                    pool_info_id                INT,
                    pid                         INT,
                    token0_address              VARCHAR(42),
                    token1_address              VARCHAR(42),
                    token0_symbol               VARCHAR(20),
                    token1_symbol               VARCHAR(20),
                    token0_decimals             INT,
                    token1_decimals             INT,
                    fee                         INT,
                    tick_spacing                INT,
                    alloc_point                 INT,
                    cake_per_day                DECIMAL(20,8),
                    total_staked_liquidity_usd  DECIMAL(20,4),
                    inactive_ratio              DECIMAL(6,4),
                    estimated_apr               DECIMAL(10,4),
                    tick_current                INT,
                    sqrt_price_x96              VARCHAR(80),
                    delta_tick_24h              INT,
                    sigma_reserve               DECIMAL(14,4),
                    pool_type                   ENUM('ZOMBIE','STABLE','SEMI_STABLE','VOLATILE','UNKNOWN'),
                    zombie_score                DECIMAL(5,4),
                    status                      ENUM('CANDIDATE','APPROVED','WATCH','REJECTED','INVESTED'),
                    selection_source            ENUM('ZOMBIE','COPY_BOT') DEFAULT 'ZOMBIE',
                    reject_reason               VARCHAR(200),
                    last_analyzed_at            DATETIME,
                    created_at                  DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_chain_pool    (chain, pool_address)
                )
            """)
            print("Table detected_pools newly created")
        else:
            # Migration: thêm cột selection_source nếu chưa có
            cursor.execute("""
                SELECT COUNT(*) AS cnt
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = 'detected_pools'
                  AND COLUMN_NAME = 'selection_source'
            """)
            row = cursor.fetchone()
            col_count = row[0] if isinstance(row, tuple) else row['cnt']
            if col_count == 0:
                cursor.execute("""
                    ALTER TABLE detected_pools
                    ADD COLUMN selection_source ENUM('ZOMBIE','COPY_BOT') DEFAULT 'ZOMBIE'
                    AFTER status
                """)
                connection.commit()
                print("Migration: added column detected_pools.selection_source")
            print("Table detected_pools already exists.")

        if "pool_tick_history" not in tables:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pool_tick_history (
                    id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
                    chain                   VARCHAR(10) NOT NULL,
                    pool_address            VARCHAR(42) NOT NULL,
                    pid                     INT,
                    tick                    INT NOT NULL,
                    sqrt_price_x96          VARCHAR(80),
                    block_number            BIGINT,
                    source                  ENUM('SLOT0') DEFAULT 'SLOT0',
                    observed_at             DATETIME NOT NULL,
                    KEY idx_chain_pool_time (chain, pool_address, observed_at),
                    KEY idx_chain_time      (chain, observed_at)
                )
            """)
            print("Table pool_tick_history newly created")
        else:
            print("Table pool_tick_history already exists.")

        if "bot_positions" not in tables:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bot_positions (
                    id                      INT AUTO_INCREMENT PRIMARY KEY,
                    chain                   VARCHAR(10)  NOT NULL,
                    pool_address            VARCHAR(42)  NOT NULL,
                    detected_pool_id        INT,
                    token_id                BIGINT UNIQUE,
                    wallet_address          VARCHAR(42),
                    tick_lower              INT NOT NULL,
                    tick_upper              INT NOT NULL,
                    tick_at_mint            INT,
                    liquidity               VARCHAR(40),
                    amount0_deposited_usd   DECIMAL(20,4),
                    amount1_deposited_usd   DECIMAL(20,4),
                    total_invested_usd      DECIMAL(20,4),
                    total_cake_harvested    DECIMAL(20,8) DEFAULT 0,
                    total_fees_earned_usd   DECIMAL(20,4) DEFAULT 0,
                    total_rebalance_count   INT           DEFAULT 0,
                    status                  ENUM('ACTIVE','OUT_OF_RANGE','HARVESTED','ABANDONED','CLOSED','STOP_LOSS'),
                    is_staked               TINYINT(1)    DEFAULT 1,
                    last_harvest_at         DATETIME,
                    last_rebalance_at       DATETIME,
                    mint_tx_hash            VARCHAR(66),
                    stake_tx_hash           VARCHAR(66),
                    open_at                 DATETIME DEFAULT CURRENT_TIMESTAMP,
                    closed_at               DATETIME,
                    close_reason            VARCHAR(200),
                    consecutive_losses      TINYINT       DEFAULT 0,
                    net_pnl_usd             DECIMAL(20,4) DEFAULT 0,
                    last_pnl_usd            DECIMAL(20,4) DEFAULT NULL,
                    last_pnl_at             DATETIME      DEFAULT NULL,
                    stop_loss_at            DATETIME      DEFAULT NULL,
                    is_blacklisted          TINYINT(1)    DEFAULT 0
                )
            """)
            print("Table bot_positions newly created")
        else:
            print("Table bot_positions already exists.")
            # Migrations for Net PnL & 3-strike Stop Loss
            try:
                cursor.execute("ALTER TABLE bot_positions MODIFY COLUMN status ENUM('ACTIVE','OUT_OF_RANGE','HARVESTED','ABANDONED','CLOSED','STOP_LOSS')")
                cursor.execute("ALTER TABLE bot_positions ADD COLUMN consecutive_losses TINYINT DEFAULT 0")
                cursor.execute("ALTER TABLE bot_positions ADD COLUMN net_pnl_usd DECIMAL(20,4) DEFAULT 0")
                cursor.execute("ALTER TABLE bot_positions ADD COLUMN last_pnl_usd DECIMAL(20,4) DEFAULT NULL")
                cursor.execute("ALTER TABLE bot_positions ADD COLUMN last_pnl_at DATETIME DEFAULT NULL")
                cursor.execute("ALTER TABLE bot_positions ADD COLUMN stop_loss_at DATETIME DEFAULT NULL")
                cursor.execute("ALTER TABLE bot_positions ADD COLUMN is_blacklisted TINYINT(1) DEFAULT 0")
                print("Migration applied for bot_positions (PnL tracking fields)")
            except mysql.connector.Error as err:
                if err.errno != 1060: # Ignore Duplicate column error
                    print(f"Migration bot_positions error: {err}")

        if "bot_transactions" not in tables:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bot_transactions (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    chain           VARCHAR(10)  NOT NULL,
                    bot_position_id INT,
                    tx_hash         VARCHAR(66)  UNIQUE,
                    action          VARCHAR(30),
                    swap_token_in   VARCHAR(42),
                    swap_token_out  VARCHAR(42),
                    swap_amount_in  VARCHAR(40),
                    swap_amount_out VARCHAR(40),
                    swap_provider   VARCHAR(30),
                    status          ENUM('PENDING','SUCCESS','FAILED'),
                    gas_used        BIGINT,
                    gas_price_gwei  DECIMAL(10,4),
                    gas_cost_usd    DECIMAL(10,6),
                    block_number    BIGINT,
                    cake_amount     DECIMAL(20,8) DEFAULT 0,
                    cake_price_usd  DECIMAL(10,4) DEFAULT 0,
                    fees_earned_usd DECIMAL(20,4) DEFAULT 0,
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (bot_position_id) REFERENCES bot_positions(id)
                )
            """)
            print("Table bot_transactions newly created")
        else:
            # Migration: Update action column to VARCHAR(30) + add earnings columns
            try:
                cursor.execute("ALTER TABLE bot_transactions MODIFY COLUMN action VARCHAR(30)")
                cursor.execute("ALTER TABLE bot_transactions ADD COLUMN cake_amount DECIMAL(20,8) DEFAULT 0")
                cursor.execute("ALTER TABLE bot_transactions ADD COLUMN cake_price_usd DECIMAL(10,4) DEFAULT 0")
                cursor.execute("ALTER TABLE bot_transactions ADD COLUMN fees_earned_usd DECIMAL(20,4) DEFAULT 0")
                print("Table bot_transactions existed. Migrations applied.")
            except mysql.connector.Error as err:
                if err.errno != 1060: # Ignore Duplicate column error
                    print(f"Migration bot_transactions error: {err}")

        # --- NEW TABLE: bot_pool_blacklist ---
        if "bot_pool_blacklist" not in tables:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bot_pool_blacklist (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    chain VARCHAR(10) NOT NULL,
                    pool_address VARCHAR(42) NOT NULL,
                    reason VARCHAR(200),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY idx_chain_pool (chain, pool_address)
                )
            """)
            print("Table bot_pool_blacklist newly created")
        else:
            print("Table bot_pool_blacklist already exists.")
            print("Table bot_transactions already exists. Migration for action applied.")

        # Clear table list after use
        del tables 

        connection.commit()

    except mysql.connector.Error as e:
        error_message_sql = f"Error creating database/table: {e}"
        print(error_message_sql)
    
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
    
    gc.collect()

# create_database_and_table()
