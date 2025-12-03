import mysql.connector
# import psycopg2

# DB_CONFIG = {
#   "host": "localhost",
#   "database": "postgres",
#   "user": "postgres",
#   "password": "060501"
# }
DB_CONFIG = {
  "host": "localhost",
  "database": "transaction_storage",
  "user": "root",
  "password": ""
}

def get_connection():
  return mysql.connector.connect(**DB_CONFIG)
  # return psycopg2.connect(**DB_CONFIG)

if __name__ == "__main__":
  conn = get_connection()
  cursor = conn.cursor()
  
  cursor.execute("DROP TABLE IF EXISTS detail_token_transactions")
  cursor.execute("DROP TABLE IF EXISTS nft_token_transactions")
  cursor.execute("DROP TABLE IF EXISTS transactions")
  cursor.execute("DROP TABLE IF EXISTS hash_txs")
  
  #Mysql version
  cursor.execute("""
                  CREATE TABLE hash_txs(
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    hash VARCHAR(100) UNIQUE NOT NULL,
                    block VARCHAR(10) NOT NULL,
                    chain VARCHAR(3) NOT NULL,
                    tx_time TIMESTAMP NOT NULL
                  )
                 """)
  
  cursor.execute("""
                  CREATE TABLE transactions(
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    hash VARCHAR(100) NOT NULL,
                    wallet VARCHAR(44) NOT NULL,
                    FOREIGN KEY (hash) REFERENCES hash_txs(hash) ON DELETE CASCADE ON UPDATE CASCADE,
                    UNIQUE KEY unique_wallet_hash (wallet, hash)
                  )
                """)
  
  cursor.execute("""
                  CREATE TABLE detail_token_transactions(
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    hash VARCHAR(100) NOT NULL,
                    from_address VARCHAR(44) NOT NULL,
                    to_address VARCHAR(44) NOT NULL,
                    contract VARCHAR(44) NOT NULL,
                    amount VARCHAR(50) NOT NULL,                    
                    symbol VARCHAR(50) NOT NULL,
                    wallet VARCHAR(44) NOT NULL,
                    FOREIGN KEY (hash) REFERENCES hash_txs(hash) ON DELETE CASCADE ON UPDATE CASCADE
                  )
                """)
  
  cursor.execute("""
                CREATE TABLE nft_token_transactions(
                  id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  hash VARCHAR(100) NOT NULL UNIQUE,
                  contract VARCHAR(44) NOT NULL,
                  token_id VARCHAR(100) NOT NULL,
                  wallet VARCHAR(44) NOT NULL,
                  FOREIGN KEY (hash) REFERENCES hash_txs(hash) ON DELETE CASCADE ON UPDATE CASCADE
                )
              """)
    
  
  print("Database and table created successfully.")
  conn.commit()
  cursor.close()