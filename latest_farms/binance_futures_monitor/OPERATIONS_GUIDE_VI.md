# Báo cáo vận hành và bảo mật Binance Futures Monitor

Tài liệu này mô tả đúng theo implementation hiện tại của module
`latest_farms/binance_futures_monitor/`. Module chạy trên máy local, đọc vị thế
USD-M và COIN-M từ Binance, sau đó ghi current-state vào MySQL để Flask đọc lại.

## 1. Kết luận bảo mật

- Config JSON không chứa API key, secret key hoặc secret prefix.
- File `.env` riêng của module lưu full API key và secret key đã bỏ 10 ký tự cuối.
- Người vận hành chỉ nhập 10 ký tự cuối của secret bằng `getpass`; nội dung nhập
  không hiển thị trên màn hình.
- Full secret chỉ được ghép và giữ trong bộ nhớ của process Python. Suffix không
  được ghi lại; mỗi lần khởi động phải nhập lại.
- Secret key chỉ được dùng trên máy local để tạo chữ ký HMAC-SHA256. Secret key
  không được gửi như một header hay một tham số request.
- API key được gửi trong header `X-MBX-APIKEY`; query gửi đến Binance chứa
  `timestamp`, `recvWindow=5000` và `signature`.
- Module chỉ implement HTTP `GET` để đọc thời gian, metadata hợp đồng và vị thế.
  Không có endpoint đặt/hủy lệnh, đổi leverage, chuyển tiền hoặc rút tiền.
- Database chỉ lưu account alias, mapping wallet, trạng thái sync và position.
  Database không có cột API key, secret key, signature hay credential fingerprint.
- Flask và browser không nhận credentials, không gọi Binance trực tiếp; chúng chỉ
  đọc dữ liệu do worker ghi vào MySQL.

Điểm cần hiểu chính xác: module là **read-only theo hành vi của source code**,
nhưng quyền `Enable Futures` trên Binance có thể rộng hơn tập lệnh `GET` mà module
này sử dụng. Vì vậy IP whitelist, máy chạy an toàn và việc tắt withdrawal vẫn là
các lớp bảo vệ bắt buộc.

## 2. Kiến trúc và luồng dữ liệu

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

Flask không nằm trên đường đi của credentials. Việc liên kết một Binance account
với một hoặc nhiều on-chain wallet chỉ là mapping bằng `account_alias`; nó không
biến wallet thành chủ sở hữu riêng của các position Binance.

## 3. Vòng đời của API key và secret key

### 3.1 Lúc khởi động

CLI đọc config, sau đó đọc full API key và secret prefix từ file `.env` riêng
bằng `dotenv_values(..., interpolate=False)`. Giá trị không được thêm vào
`os.environ` và không fallback sang `.env` root. Sau khi migration hoàn tất, CLI
prompt một lần cho mỗi account:

```text
Last 10 characters of Binance secret for main-hedge:
```

Terminal không hiện ký tự đang nhập, kể cả dấu `*`. Đây là hành vi bình thường
của `getpass`; nhập giá trị và bấm Enter.

Suffix phải có chính xác 10 ký tự và không bị `strip()`. Nếu một account bật cả
`USD_M` và `COIN_M`, suffix chỉ được nhập một lần và full credential được dùng
lại cho hai market trong toàn bộ vòng `--loop`.

### 3.2 Trong bộ nhớ process

API key và secret prefix được đặt trong `PartialBinanceCredentials`. Sau khi nhận
suffix, module ghép full secret, tạo `BinanceCredentials`, rồi giữ nó trong
`RuntimeBinanceCredentials` theo `account_alias`.

- `BinanceCredentials` tắt dataclass `repr` mặc định.
- `PartialBinanceCredentials` cũng tắt dataclass `repr` mặc định.
- `RuntimeBinanceCredentials.__repr__()` chỉ liệt kê alias, không in key.
- CLI chỉ nhận đường dẫn file `.env`, không nhận giá trị credential qua argument.
- Module không serialize suffix/full secret sang JSON và không ghi credentials
  vào MySQL.

Python không đảm bảo xóa từng byte secret khỏi RAM ngay lập tức. Người có quyền
admin/debugger, memory dump, malware hoặc quyền điều khiển process vẫn có thể đọc
bộ nhớ. Vì vậy chỉ chạy job trên máy được tin cậy và đã cập nhật bảo mật.

### 3.3 Khi ký request

Với private position endpoint, module tạo query:

```text
timestamp=<unix_ms>&recvWindow=5000
```

Sau đó tính:

```text
signature = HMAC_SHA256(secret_key, url_encoded_query)
```

Request thực tế có dạng logic:

```http
GET /fapi/v3/positionRisk?timestamp=...&recvWindow=5000&signature=...
X-MBX-APIKEY: <api-key>
```

Secret key chỉ là khóa đầu vào của phép HMAC tại local. Binance nhận API key và
signature để xác minh request, không nhận plaintext secret key. Thư viện
`requests` dùng HTTPS và mặc định xác minh TLS certificate.

`recvWindow=5000` giới hạn khoảng thời gian request hợp lệ. Nếu Binance trả
`-1021` do lệch đồng hồ, client đồng bộ server time riêng cho market đó và thử
lại một lần với timestamp và chữ ký mới.

### 3.4 Log, exception và output

Module không chủ động log:

- API key hoặc secret key;
- request header;
- signed URL hoặc signature;
- object credentials đầy đủ.

Credential loader không đưa giá trị vào exception. Worker thay API key/full
secret bằng `[REDACTED]` nếu chúng vô tình xuất hiện trong message của exception.
JSON output chỉ có alias, market, status, số position và error code.

Không nên bật HTTP wire-debug của `requests`/`urllib3`, cài proxy giải mã TLS,
hoặc thêm logging signed URL trên máy production. Các công cụ đó nằm ngoài lớp
bảo vệ của module.

### 3.5 Khi dừng job

`Ctrl+C` kết thúc process. Full secret và suffix trong process biến mất khi
process thoát; lần chạy sau phải nhập lại suffix. API key và secret prefix vẫn
nằm trong module `.env`, vì vậy file này phải được bảo vệ như sensitive material.

## 4. Các Binance API được gọi

| Market | Method và endpoint           | Loại               | Mục đích                                  |
| ------ | ----------------------------- | ------------------- | -------------------------------------------- |
| USD-M  | `GET /fapi/v1/exchangeInfo` | Public              | Đọc metadata symbol, asset và multiplier  |
| USD-M  | `GET /fapi/v3/positionRisk` | Signed`USER_DATA` | Đọc position hiện tại                    |
| USD-M  | `GET /fapi/v1/time`         | Public              | Sửa clock offset khi gặp`-1021`          |
| COIN-M | `GET /dapi/v1/exchangeInfo` | Public              | Đọc contract size và metadata hợp đồng |
| COIN-M | `GET /dapi/v1/positionRisk` | Signed`USER_DATA` | Đọc position hiện tại                    |
| COIN-M | `GET /dapi/v1/time`         | Public              | Sửa clock offset khi gặp`-1021`          |

`exchangeInfo` được cache riêng cho từng market trong 6 giờ. `/time` không gọi ở
mỗi cycle; nó chỉ được gọi khi cần sửa lỗi timestamp. Ở trạng thái ổn định, mỗi
account chỉ tạo một signed position request cho mỗi market trong một cycle.

Theo tài liệu Binance tại thời điểm viết báo cáo, USD-M Position Information V3
có IP weight 5 và COIN-M Position Information có IP weight 1. Limit được tính
theo IP và có thể thay đổi; cần đối chiếu `exchangeInfo` khi vận hành nhiều
account. Module dừng retry ngay khi nhận HTTP `429`/`418`.

Không tồn tại lời gọi `POST`, `PUT` hoặc `DELETE` đến Binance trong client hiện
tại.

## 5. Luồng của một cycle đồng bộ

1. Config local được validate. Config có field giống `api_key`, `secret_key` hoặc
   `api_secret` sẽ bị từ chối.
2. Worker thay toàn bộ mapping account-wallet trong
   `binance_account_wallet_links` dưới MySQL advisory lock.
3. Với từng cặp `(account_alias, market_type)`, worker lấy advisory lock riêng.
4. Sync state chuyển sang `RUNNING`.
5. Client đọc/cache exchange metadata, tạo signed request và gọi position API.
6. Position có `positionAmt=0` bị loại; các giá trị tài chính được parse bằng
   `Decimal` và exposure được chuẩn hóa.
7. Nếu thành công, trong một transaction worker xóa current positions cũ của
   đúng account/market, insert snapshot mới và chuyển state sang `SUCCESS`.
8. Response rỗng vẫn là thành công và xóa position cũ của market đó.
9. Nếu thất bại, positions cũ được giữ nguyên, state chuyển `FAILED` và UI có thể
   đánh dấu stale. Lỗi một market không chặn market/account còn lại.

Retry hiện tại:

- Network error và HTTP 5xx: tối đa hai lần retry, mỗi lần có timestamp và chữ ký
  mới.
- `-1021`: refresh clock của đúng market và retry một lần.
- `-2015`, HTTP `401/403`, `418/429`: không retry liên tục.

## 6. Dữ liệu được và không được lưu vào MySQL

Ba bảng của module:

| Bảng                                 | Nội dung                                                |
| ------------------------------------- | -------------------------------------------------------- |
| `binance_account_wallet_links`      | Alias và các wallet được liên kết                 |
| `binance_futures_sync_state`        | Trạng thái, lần attempt/success, error đã rút gọn |
| `binance_futures_positions_current` | Position hiện tại đã chuẩn hóa                     |

Không bảng nào có:

- API key;
- secret key;
- signature;
- signed URL hoặc request headers;
- credential fingerprint.

`account_alias`, ví dụ `main-hedge`, chỉ là tên local ổn định để nối config,
position và wallet mapping. Nó không phải Binance UID và không phải credential.

## 7. Tạo Binance API key cho monitor

Giao diện Binance có thể thay đổi theo khu vực và loại account. Các bước dưới đây
dựa trên web flow được Binance công bố; hãy đối chiếu lại trang API Management
trước khi xác nhận permission.

### 7.1 Điều kiện trước

1. Hoàn thành identity verification, bật 2FA và kích hoạt account theo yêu cầu
   của Binance.
2. Mở và kích hoạt Futures account **trước khi tạo API key**. Nếu dùng cả USD-M
   và COIN-M, bảo đảm sản phẩm Futures tương ứng đã sẵn sàng trên account.
3. Xác định public outbound IPv4 cố định của máy sẽ chạy monitor. Không dùng địa
   chỉ LAN như `192.168.x.x` hoặc `10.x.x.x`.

Binance nêu rõ key tạo trước khi Futures account được enable có thể không bật
được Futures permission. Account đã kích hoạt Portfolio Margin cũng có thể không
bật được permission này theo luồng API key thông thường.

### 7.2 Tạo key HMAC

Module hiện tại chỉ hỗ trợ system-generated **HMAC API key + secret key**; không
hỗ trợ RSA hoặc Ed25519.

1. Đăng nhập đúng website Binance chính thức từ bookmark tin cậy.
2. Vào profile -> `Account` -> `API Management` -> `Create API`.
3. Chọn `System-generated`.
4. Đặt label dễ nhận diện, ví dụ `nft-portfolio-local-monitor`.
5. Hoàn thành 2FA/passkey verification.
6. Tách đúng 10 ký tự cuối của secret. Lưu full API key và phần secret còn lại
   vào module `.env`; giữ suffix ngoài máy monitor hoặc ghi nhớ nó.
7. Không gửi full secret/suffix qua chat, email, screenshot hoặc commit vào Git.

Nếu secret bị mất hoặc không còn truy cập được, không chép key vào config để né
việc nhập lại. Hãy revoke key cũ và tạo key mới.

### 7.3 Cấu hình permission và IP whitelist

Trong `Edit restrictions` của key:

1. Giữ `Enable Reading`.
2. Chọn chế độ chỉ cho phép trusted IP và thêm public outbound IPv4 của máy chạy
   monitor.
3. Bật `Enable Futures`.
4. Không bật `Enable Withdrawals`.
5. Không bật Spot/Margin trading, Universal Transfer hoặc permission khác nếu
   monitor không cần.
6. Lưu thay đổi và hoàn thành security confirmation của Binance.

Binance hạn chế system-generated HMAC key không có IP restriction ở quyền Reading
và yêu cầu IPv4 restriction để bật thêm permission trong cấu hình bảo mật mặc
định. IP whitelist cũng giới hạn nơi key có thể được sử dụng nếu bị lộ.

Lưu ý: checkbox `Enable Futures` là permission cấp Binance account, không phải
cam kết key chỉ đọc position. Cam kết không trade của monitor đến từ việc source
code chỉ có các endpoint `GET` liệt kê ở mục 4. IP whitelist vẫn phải được coi là
bắt buộc.

### 7.4 Checklist trước lần chạy đầu

- [ ] Futures account đã được kích hoạt trước khi tạo key.
- [ ] Key là system-generated HMAC.
- [ ] `Enable Reading` và `Enable Futures` đã bật.
- [ ] Trusted IPv4 trùng với outbound IP của máy local.
- [ ] Withdrawal, transfer và các permission không cần thiết đã tắt.
- [ ] Module `.env` chỉ có full API key và secret prefix, không có suffix.
- [ ] Suffix 10 ký tự được giữ ngoài máy monitor hoặc ghi nhớ.
- [ ] Module `.env` đã được gitignore và giới hạn quyền đọc bằng Windows ACL.
- [ ] Máy chạy monitor không chia sẻ tài khoản OS với người không liên quan.

## 8. Chuẩn bị config và database

Chạy các lệnh từ repo root `D:\python\nft_projects`.

### 8.1 Database environment

Module dùng `latest_farms.create_db.get_connection()` và đọc `.env` ở repo root
bằng `python-dotenv` cho kết nối database.

Local database cần các biến:

```dotenv
ENV=local
LOCAL_DB_HOST=127.0.0.1
LOCAL_DB_PORT=3306
LOCAL_DB_USER=<mysql-user>
LOCAL_DB_PASS=<mysql-password>
LOCAL_DB_NAME=<database-name>
LOCAL_DB_SSL_DISABLED=true
```

Đây là MySQL credential, không phải Binance credential. Root `.env` độc lập với
file credential `.env` nằm trong thư mục monitor.

### 8.2 Tạo config local

```powershell
Copy-Item `
  latest_farms\binance_futures_monitor\sample_config.json `
  my_binance_monitor_config.json
```

Sửa file mới:

```json
{
  "version": 1,
  "interval_seconds": 60,
  "stale_after_seconds": 180,
  "accounts": [
    {
      "alias": "main-hedge",
      "markets": ["USD_M", "COIN_M"],
      "linked_wallets": [
        "0xYourWalletAddress"
      ]
    }
  ]
}
```

Quy tắc:

- `alias` là duy nhất và nên giữ ổn định khi rotate API key.
- Một account có thể liên kết nhiều wallet, và một wallet có thể xuất hiện ở
  nhiều account.
- `markets` chỉ nhận `USD_M`, `COIN_M`.
- Config không được có API key hoặc secret key.
- `my_binance_monitor_config.json` đã được `.gitignore`.

### 8.3 Tạo partial credential `.env`

```powershell
Copy-Item `
  latest_farms\binance_futures_monitor\.env.example `
  latest_farms\binance_futures_monitor\.env
```

Với alias `main-hedge`, sửa file thành:

```dotenv
BINANCE_MAIN_HEDGE_API_KEY=<full-api-key>
BINANCE_MAIN_HEDGE_SECRET_PREFIX=<secret-without-last-10-characters>
```

Alias được uppercase, chuỗi ký tự không phải `A-Z/0-9` được thay bằng `_`, rồi
xóa `_` ở hai đầu. Hai alias tạo cùng một tên env sẽ bị từ chối. Không đưa suffix
hoặc full secret vào file này. Pattern `.env` trong `.gitignore` đã bao phủ file
thật; `.env.example` chỉ chứa placeholder và được phép commit.

## 9. Hướng dẫn chạy job

### 9.1 Migration và one-shot đầu tiên

Dùng lần đầu để tạo bảng và kiểm tra toàn bộ kết nối:

```powershell
python -m latest_farms.binance_futures_monitor.cli `
  --config my_binance_monitor_config.json `
  --credentials-env latest_farms/binance_futures_monitor/.env `
  --migrate
```

Sau một prompt suffix cho mỗi account, job chạy một cycle rồi thoát. Kết quả
thành công có dạng:

```json
[
  {
    "account_alias": "main-hedge",
    "market_type": "USD_M",
    "position_count": 2,
    "status": "SUCCESS"
  }
]
```

`position_count: 0` cũng là kết quả hợp lệ nếu market không có open position.

### 9.2 Chạy liên tục

Sau khi one-shot thành công:

```powershell
python -m latest_farms.binance_futures_monitor.cli `
  --config my_binance_monitor_config.json `
  --credentials-env latest_farms/binance_futures_monitor/.env `
  --loop
```

Job chạy mỗi `interval_seconds`, mặc định 60 giây. Khoảng thời gian được tính từ
lúc bắt đầu cycle; nếu cycle tốn lâu, worker chỉ ngủ phần thời gian còn lại.

Để vừa migrate vừa chạy liên tục:

```powershell
python -m latest_farms.binance_futures_monitor.cli `
  --config my_binance_monitor_config.json `
  --credentials-env latest_farms/binance_futures_monitor/.env `
  --migrate `
  --loop
```

Dùng `Ctrl+C` để dừng an toàn.

### 9.3 Lưu ý khi gọi đây là background job

Job có thể chạy liên tục ở một terminal riêng, nhưng nó vẫn là interactive local
worker. Terminal/session phải còn hoạt động. Sau reboot, logout hoặc process
crash, người vận hành phải khởi động lại và nhập suffix cho từng account.

Không đưa suffix/full secret vào tham số CLI, PowerShell script, root `.env` hoặc
pipe stdin để biến job thành unattended service. Nếu sau này cần Windows
Service/Task Scheduler tự khởi động, cần một credential store riêng, ví dụ
Windows Credential Manager/DPAPI, và một đợt đánh giá bảo mật khác.

## 10. Giám sát và xử lý lỗi

| Dấu hiệu                             | Ý nghĩa thường gặp                                         | Cách xử lý                                                                          |
| -------------------------------------- | --------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `FileNotFoundError` config           | Sai working directory hoặc chưa copy sample                   | Chạy từ repo root và kiểm tra`--config`                                          |
| `credentials env file not found`     | Chưa tạo module`.env` hoặc sai đường dẫn               | Copy`.env.example` và kiểm tra `--credentials-env`                               |
| `must contain exactly 10 characters` | Suffix nhập thiếu hoặc thừa                                 | Nhập đúng 10 ký tự cuối, không thêm khoảng trắng                             |
| `AUTH_OR_IP:-2015`                   | Key/secret sai, thiếu Futures permission hoặc IP không khớp | Kiểm tra key, permission, public IPv4; nếu cần tạo lại key sau khi enable Futures |
| HTTP`401/403`                        | Authentication, permission, IP hoặc WAF từ chối              | Kiểm tra API restrictions và IP, không retry bằng cách chạy nhiều process       |
| `-1021` lặp lại                    | Đồng hồ máy lệch quá lớn                                 | Đồng bộ Windows time/NTP; client đã tự refresh một lần                         |
| `RATE_LIMIT` / `429`               | Quá request weight                                             | Tăng interval, giảm account/process; chờ theo hướng dẫn Binance                  |
| HTTP`418`                            | IP bị Binance tạm ban do tiếp tục sau 429                   | Dừng request và chờ hết ban; không khởi động nhiều worker                     |
| `LOCK_BUSY`                          | Process khác đang sync cùng account/market                   | Xác minh chỉ có một worker mong muốn                                              |
| `FAILED` nhưng vẫn còn position   | Cycle mới lỗi, snapshot cũ được giữ                      | Đọc`error_code`, `last_success_at`; UI sẽ đánh dấu stale                     |

Không dán API key/secret vào issue, log, ảnh chụp màn hình hoặc tin nhắn khi
debug. Chỉ cung cấp `account_alias`, `market_type`, error code và timestamp.

## 11. Rotate hoặc thu hồi key

Khi nghi ngờ key bị lộ:

1. Dừng monitor.
2. Vào Binance API Management và revoke/delete key ngay.
3. Kiểm tra account activity và liên hệ Binance Support nếu có dấu hiệu bất thường.
4. Xác minh lại máy local, shell history, log và các công cụ debug.
5. Tạo key HMAC mới với trusted IP và tối thiểu permission như mục 7.
6. Cập nhật full API key và secret prefix mới trong module `.env`.
7. Khởi động monitor và nhập suffix mới.

Không cần migration hay sửa `account_alias` khi rotate key. Database không có
bản sao credential cũ để phải xóa.

## 12. Phạm vi đảm bảo và rủi ro còn lại

Module không persist **full secret**, nhưng có persist full API key và phần lớn
secret trong module `.env`. Mức bảo vệ dữ liệu trên disk phụ thuộc vào 10 ký tự
cuối không được lưu, IP whitelist và độ an toàn của máy local. Module vẫn giới
hạn code ở các `GET` endpoint, redact exception và tách Flask khỏi Binance. Nó
không thể bảo vệ credential nếu:

- Máy local bị malware hoặc bị chiếm quyền admin.
- Người vận hành lưu suffix/full secret vào file, log, chat hoặc shell script.
- Module `.env` bị đọc cùng với suffix được lưu ở nơi khác trên cùng máy.
- HTTP debug/proxy bên ngoài module ghi header hoặc signed URL.
- Public IP whitelist quá rộng hoặc trùng với hạ tầng không được kiểm soát.
- Source code bị sửa để thêm endpoint trade trước khi chạy.
- Key được bật withdrawal/transfer hoặc các permission không cần thiết.

Quy tắc vận hành phù hợp là: dedicated key, trusted IPv4 cố định, permission tối
thiểu, máy local tin cậy, review diff trước khi chạy, và revoke ngay khi nghi ngờ.

## 13. Tài liệu Binance chính thức

- [How to Create API Keys on Binance](https://www.binance.com/en/support/faq/detail/360002502072)
- [USD-M General Info: endpoint security và HMAC](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)
- [USD-M Position Information V3](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade#position-information-v3)
- [COIN-M General Info: endpoint security và HMAC](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/general-info)
- [COIN-M Position Information](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/trade#position-information)

Tài liệu Binance có thể thay đổi permission, endpoint weight và giao diện API
Management. Kiểm tra lại các link trên khi tạo key mới hoặc thay đổi deployment.
