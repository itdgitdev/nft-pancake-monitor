# Binance Futures Monitor: Báo cáo vận hành và bảo mật V2

## 1. Mục đích và kết luận bảo mật

`binance_futures_monitor` là worker chạy trên máy local, đọc position USD-M và
COIN-M từ Binance rồi ghi current-state vào MySQL. Flask chỉ đọc MySQL và không
gọi Binance.

Các nguyên tắc chính:

- Config JSON chỉ chứa account alias, markets và linked wallets.
- Module `.env` lưu full API key và secret key đã bỏ 10 ký tự cuối.
- Người vận hành nhập 10 ký tự cuối bằng `getpass` khi khởi động.
- Full secret chỉ được ghép trong RAM và không được ghi vào file, DB hoặc log.
- Client chỉ có HTTP `GET`; không implement order, transfer hoặc withdrawal.
- Binance positions là dữ liệu tham chiếu của linked account, không phải position
  riêng của wallet và không được cộng vào wallet PnL.

**Cảnh báo quan trọng:** read-only là giới hạn của source code, không phải bảo
đảm của Binance API key. Key bật `Enable Futures` có thể có khả năng gọi Futures
`TRADE` endpoints nếu bị dùng bởi chương trình khác. Không bật Withdrawal vẫn
không ngăn attacker tạo thua lỗ, phí hoặc thanh lý trong Futures account.

## 2. Luồng hoạt động

```text
Local JSON config (không có credentials)
              |
              v
Module .env: full API key + secret prefix
              |
              v
Terminal getpass: 10 ký tự cuối của secret
              |
              v
Ghép full secret trong RuntimeBinanceCredentials
              |
              v
HMAC-SHA256 ký query trên máy local
              |
              v
Binance HTTPS GET: USD-M / COIN-M
              |
              v
Chuẩn hóa position bằng Decimal
              |
              v
MySQL current-state + sync state
              |
              v
Flask DB reader -> Portfolio UI
```

API key được gửi qua header `X-MBX-APIKEY`. Secret key chỉ được dùng tại local
để tạo `HMAC-SHA256` trên query có `timestamp` và `recvWindow=5000`; plaintext
secret không được gửi đến Binance.

Module không log API key, secret, signature, signed URL hoặc request headers.
Credential objects không hiển thị giá trị trong `repr`; worker redact API key và
full secret nếu chúng xuất hiện trong exception message.

## 3. Binance API key và quyền truy cập

### Tạo key

1. Kích hoạt Futures account trước khi tạo API key.
2. Vào Binance `API Management` và tạo `System-generated` HMAC key.
3. Bật `Enable Reading` và `Enable Futures`.
4. Giới hạn key theo public outbound IPv4 cố định của máy monitor.
5. Không bật Withdrawal, Universal Transfer, Spot/Margin hoặc quyền không cần.

Nếu key được tạo trước khi Futures account được kích hoạt, Binance có thể không
cho bật Futures permission. Portfolio Margin cũng có thể ảnh hưởng tùy chọn này.

### Phạm vi rủi ro

- `USER_DATA`: đọc account, position và lịch sử riêng.
- `TRADE`: có thể đặt/hủy lệnh hoặc thay đổi cấu hình Futures.
- Withdrawal và transfer là quyền riêng, nhưng việc tắt chúng không bảo vệ
  Futures collateral khỏi trading loss.
- Cross margin có thể làm toàn bộ số dư khả dụng trong Futures wallet chịu rủi
  ro; Multi-Assets hoặc Portfolio Margin có thể mở rộng phạm vi collateral.

IP whitelist là lớp bảo vệ bắt buộc. Chỉ giữ lượng collateral cần thiết trong
Futures account và revoke key ngay khi nghi ngờ máy, IP hoặc suffix bị lộ.

## 4. Credential local

Tạo file credential riêng của module:

```powershell
Copy-Item `
  latest_farms\binance_futures_monitor\.env.example `
  latest_farms\binance_futures_monitor\.env
```

Với alias `main-hedge`:

```dotenv
BINANCE_MAIN_HEDGE_API_KEY=<full-api-key>
BINANCE_MAIN_HEDGE_SECRET_PREFIX=<secret-without-last-10-characters>
```

Quy tắc alias:

```text
main-hedge -> MAIN_HEDGE
```

Alias được uppercase, ký tự không phải `A-Z/0-9` được thay bằng `_`. Hai alias
tạo cùng tên env sẽ bị từ chối.

File được đọc bằng `dotenv_values(..., interpolate=False)`, không được nạp vào
`os.environ` và không fallback sang root `.env`. File thật đã được gitignore
nhưng vẫn là sensitive material vì chứa full API key và phần lớn secret; cần giới
hạn Windows ACL.

Không lưu 10 ký tự cuối, full secret hoặc full-secret hash/checksum trên máy
monitor. Mỗi account chỉ được prompt suffix một lần dù bật cả USD-M và COIN-M.

## 5. API được monitor sử dụng

| Market | Endpoint                      | Mục đích                         |
| ------ | ----------------------------- | ----------------------------------- |
| USD-M  | `GET /fapi/v1/exchangeInfo` | Metadata symbol và multiplier      |
| USD-M  | `GET /fapi/v3/positionRisk` | Position`USER_DATA`               |
| USD-M  | `GET /fapi/v1/time`         | Sửa clock offset khi gặp`-1021` |
| COIN-M | `GET /dapi/v1/exchangeInfo` | Contract size và metadata          |
| COIN-M | `GET /dapi/v1/positionRisk` | Position`USER_DATA`               |
| COIN-M | `GET /dapi/v1/time`         | Sửa clock offset khi gặp`-1021` |

`exchangeInfo` được cache riêng cho từng market trong 6 giờ. `/time` chỉ được gọi
khi cần sửa timestamp. Client không có Binance `POST`, `PUT` hoặc `DELETE`.

Retry policy:

- Network error và HTTP 5xx: tối đa hai retry với timestamp/signature mới.
- `-1021`: đồng bộ clock của đúng market và retry một lần.
- `-2015`, HTTP `401/403`, `418/429`: không retry liên tục.

## 6. Đồng bộ MySQL

Module quản lý ba bảng:

| Bảng                                 | Nội dung                               |
| ------------------------------------- | --------------------------------------- |
| `binance_account_wallet_links`      | Mapping account alias với wallets      |
| `binance_futures_sync_state`        | Trạng thái và thời gian sync        |
| `binance_futures_positions_current` | Position current-state đã chuẩn hóa |

Không bảng nào chứa API key, secret, prefix, suffix, signature hoặc credential
fingerprint.

Mỗi `(account_alias, market_type)` được xử lý độc lập dưới MySQL advisory lock:

1. Chuyển sync state sang `RUNNING`.
2. Gọi Binance và loại position có `positionAmt=0`.
3. Thành công: xóa snapshot cũ, insert snapshot mới và chuyển `SUCCESS` trong
   một transaction.
4. Response rỗng vẫn là thành công và xóa position cũ của market đó.
5. Thất bại: giữ snapshot cũ, chuyển `FAILED` và để UI đánh dấu stale.

USD-M lỗi không chặn COIN-M và ngược lại.

## 7. Chuẩn bị và chạy job

Root `.env` dùng cho MySQL, độc lập với module credential `.env`:

```dotenv
ENV=local
LOCAL_DB_HOST=127.0.0.1
LOCAL_DB_PORT=3306
LOCAL_DB_USER=<mysql-user>
LOCAL_DB_PASS=<mysql-password>
LOCAL_DB_NAME=<database-name>
```

Tạo config:

```powershell
Copy-Item `
  latest_farms\binance_futures_monitor\sample_config.json `
  my_binance_monitor_config.json
```

Lần đầu hoặc database mới, chạy migration và một cycle:

```powershell
python -m latest_farms.binance_futures_monitor.cli `
  --config my_binance_monitor_config.json `
  --credentials-env latest_farms/binance_futures_monitor/.env `
  --migrate
```

Các lần chạy liên tục sau đó không cần `--migrate`:

```powershell
python -m latest_farms.binance_futures_monitor.cli `
  --config my_binance_monitor_config.json `
  --credentials-env latest_farms/binance_futures_monitor/.env `
  --loop
```

Job mặc định chạy mỗi 60 giây. Dùng `Ctrl+C` để dừng. Sau reboot, logout hoặc
process crash, phải khởi động lại và nhập suffix cho từng account.

## 8. Sự cố thường gặp

| Lỗi                             | Nguyên nhân thường gặp                            | Xử lý                                          |
| -------------------------------- | ------------------------------------------------------ | ------------------------------------------------ |
| Config`FileNotFoundError`      | Sai working directory hoặc path                       | Chạy từ repo root và kiểm tra`--config`    |
| Credentials env không tồn tại | Chưa copy`.env.example`                             | Kiểm tra`--credentials-env`                   |
| Suffix không đủ 10 ký tự    | Nhập thiếu/thừa hoặc có khoảng trắng            | Nhập đúng 10 ký tự cuối                    |
| `AUTH_OR_IP:-2015`             | Sai suffix/key, thiếu Futures permission hoặc sai IP | Kiểm tra credential, permission và whitelist   |
| `-1021` lặp lại              | Đồng hồ máy lệch                                  | Đồng bộ Windows time/NTP                      |
| `429` / `418`                | Quá request weight hoặc IP bị ban                   | Dừng request, chờ và giảm tần suất/process |
| `LOCK_BUSY`                    | Worker khác đang sync cùng account/market           | Chỉ giữ một worker mong muốn                 |
| `FAILED` nhưng còn position  | Snapshot mới lỗi nên snapshot cũ được giữ      | Kiểm tra error code và`last_success_at`      |

Khi debug, chỉ chia sẻ alias, market, error code và timestamp. Không đưa API key,
prefix, suffix, full secret hoặc signed URL vào log, issue hay ảnh chụp.

## 9. Rotate và ứng phó sự cố

1. Dừng monitor.
2. Revoke/delete key cũ trong Binance API Management.
3. Kiểm tra account activity và các Futures positions.
4. Xác minh máy local, log và công cụ debug.
5. Tạo key mới với trusted IPv4 và permission tối thiểu.
6. Cập nhật API key và secret prefix trong module `.env`.
7. Khởi động lại và nhập suffix mới.

Không cần migration hoặc đổi `account_alias` khi rotate key.

## 10. Tài liệu Binance chính thức

- [Tạo và cấu hình Binance API key](https://www.binance.com/en/support/faq/detail/360002502072)
- [USD-M endpoint security](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)
- [USD-M Position Information V3](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade#position-information-v3)
- [COIN-M endpoint security](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/general-info)
- [COIN-M Position Information](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/trade#position-information)

Kiểm tra lại tài liệu Binance khi tạo key mới vì permission, endpoint weight và
giao diện API Management có thể thay đổi.
