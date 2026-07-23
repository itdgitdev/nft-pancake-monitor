# Hướng Dẫn Chạy Auto-Rebalancer + Auto-Compound Trên Windows

Rebalancer và auto-compound dùng chung một chương trình; rebalancer luôn chạy trước.

> **Cảnh báo:** `03_RUN_ONE_CYCLE.bat` và `04_RUN_LOOP.bat` chạy live và có thể gửi giao dịch thật. Lần đầu chỉ cấu hình một pool, giữ `max_jobs_per_cycle=1`, `execute_burn=false` và không chạy tool khác bằng cùng wallet.

## Bước 1 — Tải và giải nén project

1. Mở [github.com/itdgitdev/nft-pancake-monitor](https://github.com/itdgitdev/nft-pancake-monitor).
2. Nhấn **Code** → **Download ZIP**.
3. Mở thư mục `Downloads`, nhấn chuột phải vào file ZIP và chọn **Extract All...**.
4. Chọn nơi dễ nhớ, ví dụ `D:\configured-rebalancer`, rồi nhấn **Extract**.

![Tải source code từ GitHub](image/LOCAL_RUN_GUIDE/1784632747237.png)

*Kết quả đúng cần nhìn thấy: menu **Code** có lựa chọn **Download ZIP**.*

GitHub thường tạo thư mục có hậu tố `-main`. Hãy mở thư mục đó cho đến khi thấy đồng thời:

```text
requirements.txt
01_SETUP.bat
latest_farms\
```

Đây là **thư mục gốc project**. Không chạy tool bên trong file ZIP và không di chuyển riêng thư mục `configured_pool_rebalancer`.

## Bước 2 — Chuẩn bị thông tin

Trước khi cấu hình, chuẩn bị:

- Địa chỉ wallet chạy bot.
- Địa chỉ pool.
- DEX và chain: Pancake/BNB hoặc Aerodrome/Base.
- Pancake farm PID nếu position tham gia farm.
- Khoảng rebalance mong muốn: phần trăm dưới và trên current price.
- File `.env` do người quản trị cung cấp, chứa DB/RPC/API credentials.
- Private key: 54 ký tự hex đầu nằm trong `.env`; giữ riêng 10 ký tự cuối.

Không cần nhập token ID. Với Aerodrome, cũng không cần nhập NPM, gauge, token address, fee hoặc tick spacing; tool tự xác minh các giá trị này on-chain.

## Bước 3 — Cài Python và chạy setup

### 3.1 Cài Python 3.11

Tải Python 3.11.9 tại [trang phát hành chính thức](https://www.python.org/downloads/release/python-3119/).

![1784732895539](image/LOCAL_RUN_GUIDE/1784732895539.png)

*Kết quả đúng cần nhìn thấy: chọn bản Python 3.13.8 có Windows installer.*

Khi cài đặt, giữ Python Launcher và chọn **Add python.exe to PATH** nếu màn hình cài đặt có lựa chọn này.

![Cài Python](image/LOCAL_RUN_GUIDE/1784631606924.png)

*Kết quả đúng cần nhìn thấy: chọn **Windows installer (64-bit)** trên máy Windows 64-bit thông thường.*

### 3.2 Chạy setup

Trong thư mục gốc project, double-click:

```text
01_SETUP.bat
```

Script sẽ tự:

- Kiểm tra Python 3.11.
- Tạo môi trường `.venv` riêng cho tool.
- Cài các thư viện cần thiết.
- Kiểm tra CLI.

Chỉ tiếp tục khi cửa sổ hiển thị:

```text
SETUP COMPLETED
```

Setup không đọc private key, không sửa config và không gửi transaction.

## Bước 4 — Đặt file `.env`

1. Copy file `.env` do người quản trị cung cấp vào thư mục gốc project.
2. Trong File Explorer, bật **View → Show → File name extensions**.
3. Xác nhận tên file đúng là `.env`, không phải `.env.txt`.

File cần có private-key prefix tương ứng với wallet alias `main`:

```dotenv
CONFIGURED_REBALANCER_MAIN_PRIVATE_KEY_PREFIX=<54_KY_TU_HEX_DAU>
```

Private key đầy đủ có 64 ký tự hex sau khi bỏ `0x`:

- 54 ký tự đầu được lưu trong `.env`.
- 10 ký tự cuối chỉ nhập khi chạy live.
- Không lưu 10 ký tự cuối hoặc full private key trong file, ảnh chụp hay log.
- Không dùng website hoặc công cụ online để chia private key.

Khuyến nghị nhờ người quản trị chuẩn bị sẵn prefix trong `.env` để tránh đếm sai.

## Bước 5 — Chọn và chỉnh mẫu config

Mở thư mục:

```text
latest_farms\configured_pool_rebalancer\templates
```

Chọn đúng một file:

| Trường hợp                    | File mẫu                 |
| -------------------------------- | ------------------------- |
| Pancake position có farm        | `pancake_farm.json`     |
| Pancake position không có farm | `pancake_unstaked.json` |
| Aerodrome                        | `aerodrome.json`        |

Copy file đã chọn ra thư mục gốc project và đổi tên thành:

```text
my_rebalance_config.json
```

Mở file bằng Notepad hoặc VS Code và chỉ thay các giá trị sau:

### Wallet

```json
"bot_wallet": "0xYOUR_WALLET_ADDRESS"
```

### Tên pool

Giữ dấu ngoặc kép và đặt tên dễ nhận biết, ví dụ `USDC-PROS-0.05`:

```json
"name": "REPLACE_WITH_POOL_NAME"
```

### Pool address

```json
"pool_address": "0xYOUR_POOL_ADDRESS"
```

### Pancake farm PID

Chỉ có trong mẫu `pancake_farm.json`. Thay cả placeholder và dấu ngoặc kép bằng số PID:

```json
"pid": 123
```

Không tự đoán PID. Nếu pool không có farm, dùng mẫu `pancake_unstaked.json`.

### Khoảng rebalance

```json
"lower_percent": -10.0,
"upper_percent": 20.0
```

`lower_percent` phải âm, `upper_percent` phải dương. Không sửa các field khác trong lần chạy đầu.

Mẫu đã dùng các mặc định an toàn ban đầu:

| Config                        | Giá trị | Ý nghĩa                                                         |
| ----------------------------- | --------: | ----------------------------------------------------------------- |
| `interval_seconds`          |  `1800` | Một cycle mỗi 30 phút.                                         |
| `slippage_bps`              |    `10` | Slippage tối đa 0,1%.                                           |
| `max_swap_price_impact_pct` |   `0.5` | Bỏ route có price impact trên 0,5%.                            |
| `max_jobs_per_cycle`        |     `1` | Tối đa một rebalance job cho pool trong cycle.                 |
| `execute_burn`              | `false` | Không burn NFT cũ.                                              |
| `auto_compound.enabled`     |  `true` | Bật auto-compound sau rebalancer.                                |
| `min_compound_usd`          |   `3.0` | Fee phải đạt ít nhất 3 USD để được xét compound.       |
| `gas_cost_multiplier`       |   `2.0` | Giá trị tái đầu tư phải đủ lớn so với gas ước tính. |
| `min_interval_seconds`      | `21600` | Cooldown 6 giờ sau một lần compound thành công.              |

> File giữ `dry_run=true` làm mặc định an toàn. Khi chạy BAT live, flag `--execute` của CLI sẽ chuyển runtime sang live và có thể gửi transaction thật.

## Bước 6 — Kiểm tra trước khi chạy live

Double-click:

```text
02_CHECK_CONFIG.bat
```

Script này chỉ đọc cấu hình, `.env`, DB schema và RPC chain ID. Nó không hỏi suffix, không tạo full private key, không migrate DB, không discovery position và không gửi transaction.

Chỉ tiếp tục khi dòng cuối là:

```text
READY FOR LIVE RUN
```

Với lần chạy đầu, output phải cho biết chỉ có một configured pool. Nếu có bất kỳ dòng `[FAIL]`, sửa lỗi đó rồi chạy lại script.

Trước live-run, xác nhận thêm:

- Wallet có đủ BNB trên BNB Chain hoặc ETH trên Base để trả gas.
- Pool address, chain và Pancake PID đúng.
- Cả hai `max_jobs_per_cycle` đều bằng `1`.
- `execute_burn=false`.
- Không có worker/tool khác dùng cùng wallet.
- Người quản trị xác nhận wallet không có job pending hoặc recovery chưa xử lý.

Nếu journal table bị thiếu, liên hệ người quản trị. Không tự chạy migration và không tự sửa DB.

## Bước 7 — Chạy live một cycle an toàn

Đây là live-run, không phải simulation. Double-click:

```text
03_RUN_ONE_CYCLE.bat
```

Script sẽ:

1. Chạy lại preflight.
2. Hiển thị wallet, chain và pool đã cấu hình.
3. Yêu cầu nhập chính xác `LIVE`.
4. Yêu cầu 10 ký tự cuối private key; ký tự không hiển thị là bình thường.
5. Chạy đúng một cycle và ghi log.

Không đóng cửa sổ khi transaction đang pending. Sau khi cycle kết thúc, log nằm tại:

```text
latest_farms\logs\configured_rebalancer_loop.log
```

Các kết quả bình thường có thể gặp:

| Kết quả              | Ý nghĩa                                                             |
| ---------------------- | --------------------------------------------------------------------- |
| `IN_RANGE`           | Position đang trong range, không rebalance.                         |
| `BELOW_MIN_COMPOUND` | Fee chưa đạt ngưỡng compound.                                    |
| `STAKE_POLICY`       | Aerodrome đang staked nên không compound trading fee.              |
| `COMPLETED`          | Compound hoàn thành.                                                |
| `REMINTED`           | Rebalance hoàn thành và NFT mới đã được stake lại.          |
| `REMINTED_UNSTAKED`  | Rebalance hoàn thành, NFT mới giữ unstaked như trạng thái cũ. |

Dừng và liên hệ người quản trị nếu thấy:

```text
ERROR
FAILED
RECOVERY_REQUIRED
MANUAL_RECOVERY
SWAP_PENDING kéo dài
MINTED_UNSTAKED kéo dài
```

Không chỉ dựa vào ảnh mẫu. Hãy kiểm tra từng transaction hash trên BscScan/BaseScan và xác nhận owner, stake mode, range, liquidity của NFT.

### Khi nào bắt buộc chạy one-cycle?

- Lần đầu chạy tool.
- Sau khi thêm/sửa pool hoặc wallet.
- Sau khi đổi RPC, DB hoặc gas policy.
- Sau khi nâng cấp code.
- Sau khi người quản trị xử lý recovery.

Có thể bỏ qua nếu chỉ restart loop bình thường với cùng code/config và không có pending/recovery job.

## Bước 8 — Chạy liên tục

Chỉ chạy sau khi one-cycle đầu đã được đối soát. Double-click:

```text
04_RUN_LOOP.bat
```

1. Nhập `START` để xác nhận live loop.
2. Nhập 10 ký tự cuối private key khi CLI hỏi.
3. Giữ máy tính bật, không để sleep và không đóng cửa sổ đang chạy.

Với `interval_seconds=1800`, cycle mới bắt đầu mỗi 30 phút. Chỉ chạy một loop cho cùng wallet.

## Bước 9 — Xem log, dừng và khởi động lại

### Xem log realtime

Double-click:

```text
05_VIEW_LOG.bat
```

Nếu log chưa tồn tại, hãy chạy one-cycle hoặc loop trước.

### Dừng an toàn

1. Mở cửa sổ đang chạy `04_RUN_LOOP.bat`.
2. Nếu đang có transaction, chờ đến khi receipt rõ ràng.
3. Nhấn `Ctrl+C` một lần.
4. Chờ terminal báo loop đã dừng và trả quyền điều khiển.

Không đóng cửa sổ, End Task hoặc tắt máy giữa lúc transaction đang xử lý.

### Khởi động lại

- Nếu chỉ dừng bình thường và không đổi gì: chạy lại `04_RUN_LOOP.bat`.
- Nếu đã sửa config/code hoặc vừa xử lý recovery: chạy `02_CHECK_CONFIG.bat` và `03_RUN_ONE_CYCLE.bat` trước.

## Bước 10 — Thêm pool mới

1. Dừng loop an toàn.
2. Backup `my_rebalance_config.json`.
3. Thêm đúng một pool mới vào mảng `pools`; giữa hai pool phải có dấu phẩy:

```json
"pools": [
  {
    "name": "pool-thu-nhat"
  },
  {
    "name": "pool-thu-hai"
  }
]
```

Mỗi object thực tế phải có đầy đủ field như object trong file template tương ứng. Tương tự như hình bên dưới.

![1784733094171](image/LOCAL_RUN_GUIDE/1784733094171.png)

4. Giữ `max_jobs_per_cycle=1` cho pool mới.
5. Chạy `02_CHECK_CONFIG.bat`.
6. Chạy `03_RUN_ONE_CYCLE.bat`.
7. Đối chiếu kết quả rồi mới chạy `04_RUN_LOOP.bat`.

## Lỗi thường gặp

### Setup

| Lỗi                      | Cách xử lý                                                 |
| ------------------------- | ------------------------------------------------------------- |
| Python 3.11 was not found | Cài Python 3.11 rồi chạy lại`01_SETUP.bat`.             |
| `No module named ...`   | Chạy lại`01_SETUP.bat`; không dùng Python global.       |
| Không thấy file BAT     | Mở đúng thư mục gốc project, không mở bên trong ZIP. |

### Environment và DB

| Lỗi                                     | Cách xử lý                                                          |
| ---------------------------------------- | ---------------------------------------------------------------------- |
| `.env not found`                       | Bật hiển thị extension và kiểm tra file không phải`.env.txt`. |
| `Access denied` / `Unknown database` | Xin lại đúng`.env` từ người quản trị.                        |
| Journal table missing                    | Liên hệ người quản trị; không tự migrate.                      |

### Config và private key

| Lỗi                                  | Cách xử lý                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------------ |
| Config file not found                 | File phải nằm ở project root và có tên`my_rebalance_config.json`.      |
| JSON syntax                           | Kiểm tra dấu phẩy, ngoặc và đuôi`.json`, hoặc copy lại template.    |
| Replace all config placeholders       | Thay toàn bộ`YOUR_...` và `REPLACE_...`.                                |
| Prefix phải có 54 ký tự           | Xin lại prefix hoặc chia lại private key; không dán full key vào log.    |
| Key không khớp wallet               | Dừng; kiểm tra prefix, suffix và`bot_wallet`.                             |
| Không thấy ký tự khi nhập suffix | Đây là hành vi bảo mật bình thường. Nhập đủ 10 ký tự rồi Enter. |

### RPC, discovery và transaction

| Lỗi                   | Cách xử lý                                                           |
| ---------------------- | ----------------------------------------------------------------------- |
| `No working RPC`     | Kiểm tra mạng và liên hệ người quản trị về RPC.               |
| Không thấy position  | Kiểm tra wallet, pool và Pancake PID.                                 |
| Không đủ gas        | Nạp BNB hoặc ETH cho wallet trước khi chạy lại.                   |
| `STAKE_POLICY`       | Không phải lỗi; Aerodrome staked không được auto-compound.       |
| `BELOW_MIN_COMPOUND` | Không phải lỗi; fee chưa đạt ngưỡng.                            |
| `RECOVERY_REQUIRED`  | Dừng loop, không sửa DB thủ công và liên hệ người quản trị. |

## Phụ lục A — Các lệnh thủ công

Chỉ dùng khi file BAT không chạy và người quản trị yêu cầu.

Mở PowerShell tại project root.

### Setup

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

### Kiểm tra read-only

```powershell
.\.venv\Scripts\python.exe -m latest_farms.configured_pool_rebalancer.preflight `
  --project-root . `
  --config .\my_rebalance_config.json
```

### Live một cycle

```powershell
.\.venv\Scripts\python.exe -m latest_farms.configured_pool_rebalancer.cli `
  --config .\my_rebalance_config.json `
  --execute
```

### Live loop

```powershell
.\.venv\Scripts\python.exe -m latest_farms.configured_pool_rebalancer.cli `
  --config .\my_rebalance_config.json `
  --execute `
  --loop
```

Chi tiết kiến trúc và state machine: [REPORT_V2.md](REPORT_V2.md).
