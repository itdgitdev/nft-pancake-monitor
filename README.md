# Monitor NFT Pancake
Chương trình Python thực hiện monitor + mint NFT position trên Pancake với các chain(EVM + SOL)

## 1. Cài đặt Python
    Cài đặt Python phiên bản **3.13**

## 2. Cài đặt Mysql host với Laragon:
    https://laragon.org/
  *Lưu ý: Nếu connect trực tiếp đến db của server thì không cần cài

## 3. Cài đặt thư viện Python
Cài đặt các thư viện cần thiết từ file requirements.txt:

  pip install -r requirements.txt

## 4. Cấu trúc chương trình
 - config.py: file cấu hình node rpc, contract, api endpoint.
 - app.py: các route + run Flask app.
 - run_auto_fetch_data.py: cronjob lấy data nft của tất cả các ví mỗi 2h
 - run_auto_storage_transaction.py: cronjob lấy history transaction của các ví
 - monitor_current_tick_evm.py: websocket các ví evm
 - monitor_current_tick.py: websocket các ví sol
 - logging_config: file cấu hình logging
 - services: thực hiện lấy và xử lý data trên các chain evm + sol
 - static: lưu các file css, js, images
 - templates: lưu các file html

## 4. Cấu hình kết nối DB server:
### 4.1 Connect đến DB server qua ssh:
Mở terminal và chạy lệnh này(đảm bảo luôn bật terminal khi chạy chương trình):
    ssh -L 3307:127.0.0.1:3306 -p 24700 user@serverIP

### 4.2 Add file .env vào project:
    # Environment type
    ENV=server

    # Server DB configuration
    SERVER_DB_HOST=127.0.0.1
    SERVER_DB_USER=apebond
    SERVER_DB_PASS=password
    SERVER_DB_NAME=apebond
    SERVER_DB_PORT=3307
    SERVER_DB_SSL_DISABLED=False

    # Local DB configuration
    LOCAL_DB_HOST=localhost
    LOCAL_DB_USER=root
    LOCAL_DB_PASS=
    LOCAL_DB_NAME=apebond
    LOCAL_DB_PORT=3306
    LOCAL_DB_SSL_DISABLED=False

## 5. Hướng dẫn chạy chương trình:
    python app.py