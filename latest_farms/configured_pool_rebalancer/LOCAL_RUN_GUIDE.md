# Hướng Dẫn Cài Đặt Và Chạy Auto-Rebalancer + Auto-Compound Trên Windows

Auto-rebalancer và auto-compound dùng chung một chương trình. Mỗi lượt chạy, chương trình luôn kiểm tra và xử lý rebalance trước, sau đó mới kiểm tra compound.

> **Cảnh báo:** `03_RUN_ONE_CYCLE.bat` và `04_RUN_LOOP.bat` có thể gửi giao dịch thật. Lần đầu chỉ nên cấu hình một pool, giữ `max_jobs_per_cycle=1`, `execute_burn=false` và không chạy chương trình khác bằng cùng wallet.

## Bước 1 — Tải và giải nén chương trình

1. Mở [github.com/itdgitdev/nft-pancake-monitor](https://github.com/itdgitdev/nft-pancake-monitor).
2. Nhấn **Code** → **Download ZIP**.
3. Mở thư mục `Downloads`, nhấn chuột phải vào file ZIP và chọn **Extract All...**.
4. Chọn nơi dễ nhớ, ví dụ `D:\configured-rebalancer`, rồi nhấn **Extract**.

![Tải chương trình từ GitHub](image/LOCAL_RUN_GUIDE/1784632747237.png)

*Kết quả đúng cần nhìn thấy: menu **Code** có lựa chọn **Download ZIP**.*

GitHub thường tạo một thư mục có hậu tố `-main`. Hãy mở thư mục đó cho đến khi thấy đồng thời:

```text
requirements.txt
01_SETUP.bat
latest_farms\
```

Đây là **thư mục chính của chương trình**. Không chạy chương trình trực tiếp trong file ZIP và không di chuyển riêng thư mục `configured_pool_rebalancer`.

## Bước 2 — Chuẩn bị thông tin

Trước khi cấu hình, chuẩn bị:

- Địa chỉ wallet chạy bot.
- Địa chỉ pool.
- DEX và chain: Pancake/BNB hoặc Aerodrome/Base.
- Pancake farm PID nếu position tham gia farm.
- Khoảng rebalance mong muốn: phần trăm dưới và trên giá hiện tại.
- File `.env` do người quản trị cung cấp.
- Private key: 54 ký tự hex đầu được lưu trong `.env`; giữ riêng 10 ký tự cuối.

Không cần nhập token ID. Với Aerodrome, cũng không cần nhập NPM, gauge, token address, fee hoặc tick spacing; chương trình tự xác minh các giá trị này on-chain.

## Bước 3 — Cài Python và chuẩn bị chương trình

### 3.1 Cài Python 3.13

1. Mở [trang tải Python dành cho Windows](https://www.python.org/downloads/windows/).
2. Tìm phiên bản Python `3.13.x`.
3. Chọn **Windows installer (64-bit)** trên máy Windows 64-bit thông thường.

![Chọn Python 3.13](image/LOCAL_RUN_GUIDE/1784732895539.png)

*Kết quả đúng cần nhìn thấy: chọn một phiên bản Python 3.13 có bộ cài Windows 64-bit.*

![Chọn bộ cài Windows 64-bit](image/LOCAL_RUN_GUIDE/1784631606924.png)

Mở file vừa tải, chọn **Add python.exe to PATH** nếu màn hình cài đặt có lựa chọn này, sau đó hoàn tất cài đặt.

![1784776605055](image/LOCAL_RUN_GUIDE/1784776605055.png)

Sau khi cài xong:

1. Mở PowerShell.
2. Nhập:

```powershell
py --version
```

![Kiểm tra phiên bản Python](image/LOCAL_RUN_GUIDE/1784775288083.png)

Nếu kết quả bắt đầu bằng `Python 3.13` thì Python đã được cài thành công.

### 3.2 Chạy file cài đặt ban đầu

Trong thư mục chính của chương trình, nhấp đúp:

```text
01_SETUP.bat
```

File này sẽ tự:

- Kiểm tra Python 3.13.
- Chuẩn bị môi trường Python riêng cho chương trình.
- Cài các thành phần cần thiết.
- Kiểm tra chương trình có thể khởi động.

Chỉ tiếp tục khi cửa sổ hiển thị:

```text
SETUP COMPLETED
```

Bước này không đọc private key, không sửa file cấu hình và không gửi giao dịch.

## Bước 4 — Đặt file `.env`

1. Sao chép file `.env` do người quản trị cung cấp vào thư mục chính của chương trình.
2. Trong File Explorer, bật **View → Show → File name extensions**.
3. Xác nhận tên file đúng là `.env`, không phải `.env.txt`.

Trong `.env` cần có dòng chứa 54 ký tự đầu của private key cho wallet `main`:

```dotenv
CONFIGURED_REBALANCER_MAIN_PRIVATE_KEY_PREFIX=<54_KY_TU_HEX_DAU>
```

Private key đầy đủ có 64 ký tự hex sau khi bỏ `0x`:

- 54 ký tự đầu được lưu trong `.env`.
- 10 ký tự cuối chỉ nhập khi chạy thật.
- Không lưu 10 ký tự cuối hoặc toàn bộ private key trong file, ảnh chụp hay nhật ký.
- Không dùng website hoặc công cụ online để chia private key.

## Bước 5 — Chọn và chỉnh file cấu hình mẫu

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

Sao chép file đã chọn ra thư mục chính của chương trình và đổi tên thành:

```text
my_rebalance_config.json
```

Mở file bằng Notepad hoặc VS Code và chỉ thay những giá trị sau.

### Wallet

```json
"bot_wallet": "0xYOUR_WALLET_ADDRESS"
```

### Tên pool

Giữ dấu ngoặc kép và đặt tên dễ nhận biết, ví dụ:

```json
"name": "USDC-PROS-0.05"
```

### Địa chỉ pool

```json
"pool_address": "0xYOUR_POOL_ADDRESS"
```

### Pancake farm PID

Chỉ áp dụng cho file `pancake_farm.json`. Thay giá trị mẫu bằng PID dạng số:

```json
"pid": 123
```

Không tự đoán PID. Nếu pool không có farm, sử dụng `pancake_unstaked.json`.

### Khoảng rebalance

```json
"lower_percent": -10.0,
"upper_percent": 20.0
```

`lower_percent` phải âm và `upper_percent` phải dương. Không sửa các dòng khác trong lần chạy đầu.

File mẫu đã dùng các giá trị an toàn ban đầu:

| Dòng cấu hình              | Giá trị | Ý nghĩa                                                                   |
| ----------------------------- | --------: | --------------------------------------------------------------------------- |
| `interval_seconds`          |  `1800` | Chạy một lượt mỗi 30 phút.                                            |
| `slippage_bps`              |    `10` | Slippage tối đa 0,1%.                                                     |
| `max_swap_price_impact_pct` |   `0.5` | Bỏ đường swap có price impact trên 0,5%.                              |
| `max_jobs_per_cycle`        |     `1` | Mỗi pool chỉ thực hiện tối đa một rebalance trong một lượt.       |
| `execute_burn`              | `false` | Không burn NFT cũ.                                                        |
| `auto_compound.enabled`     |  `true` | Kiểm tra compound sau khi rebalancer chạy xong.                           |
| `min_compound_usd`          |   `3.0` | Fee phải đạt ít nhất 3 USD để được xét compound.                 |
| `gas_cost_multiplier`       |   `2.0` | Chỉ compound khi giá trị tái đầu tư đủ lớn so với gas dự kiến. |
| `min_interval_seconds`      | `21600` | Chờ 6 giờ sau một lần compound thành công.                            |

> File mẫu có `dry_run=true`. Tuy nhiên, khi chạy `03_RUN_ONE_CYCLE.bat` hoặc `04_RUN_LOOP.bat`, chương trình vẫn chuyển sang chế độ chạy thật và có thể gửi giao dịch.

## Bước 6 — Kiểm tra trước khi chạy thật

Nhấp đúp:

```text
02_CHECK_CONFIG.bat
```

File này kiểm tra:

- Python và file `.env`.
- Nội dung file cấu hình.
- Kết nối cơ sở dữ liệu.
- Kết nối BNB Chain hoặc Base.
- Độ dài 54 ký tự đầu của private key.

Nó không hỏi 10 ký tự cuối của private key, không tạo private key đầy đủ, không sửa cơ sở dữ liệu và không gửi giao dịch.

Chỉ tiếp tục khi dòng cuối là:

```text
READY FOR LIVE RUN
```

Trong lần chạy đầu, kết quả phải cho biết chỉ có một pool được cấu hình. Nếu xuất hiện dòng `[FAIL]`, sửa lỗi đó rồi chạy lại.

Trước khi chạy thật, xác nhận:

- Wallet có đủ BNB trên BNB Chain hoặc ETH trên Base để trả gas.
- Địa chỉ pool, chain và Pancake PID đã đúng.
- Cả hai dòng `max_jobs_per_cycle` đều bằng `1`.
- `execute_burn=false`.
- Không có chương trình khác dùng cùng wallet.

Nếu chương trình báo thiếu bảng dữ liệu, hãy liên hệ người quản trị. Không tự tạo bảng hoặc sửa cơ sở dữ liệu.

## Bước 7 — Chạy thật một lần để kiểm tra

Đây là lần chạy thật, không phải mô phỏng. Nhấp đúp:

```text
03_RUN_ONE_CYCLE.bat
```

Chương trình sẽ:

1. Kiểm tra lại toàn bộ cấu hình.
2. Hiển thị wallet, chain và số pool.
3. Yêu cầu nhập chính xác `LIVE`.
4. Yêu cầu 10 ký tự cuối của private key. Ký tự sẽ không hiển thị khi nhập.
5. Chạy đúng một lượt và ghi nhật ký hoạt động.

Không đóng cửa sổ khi giao dịch đang chờ blockchain xác nhận. Sau khi hoàn tất, nhật ký nằm tại:

```text
latest_farms\logs\configured_rebalancer_loop.log
```

Các kết quả bình thường:

| Kết quả              | Ý nghĩa                                                         |
| ---------------------- | ----------------------------------------------------------------- |
| `IN_RANGE`           | Position đang trong range nên không cần rebalance.            |
| `BELOW_MIN_COMPOUND` | Fee chưa đạt ngưỡng compound.                                |
| `STAKE_POLICY`       | Aerodrome position đang staked nên không compound trading fee. |
| `COMPLETED`          | Compound hoàn thành.                                            |
| `REMINTED`           | Rebalance hoàn thành và NFT mới đã được stake lại.      |
| `REMINTED_UNSTAKED`  | Rebalance hoàn thành và NFT mới vẫn unstaked như trước.   |

Dừng chương trình và liên hệ người quản trị nếu thấy:

```text
ERROR
FAILED
RECOVERY_REQUIRED
MANUAL_RECOVERY
SWAP_PENDING kéo dài
MINTED_UNSTAKED kéo dài
```

Kiểm tra từng transaction hash trên BscScan hoặc BaseScan, đồng thời xác nhận owner, stake mode, range và liquidity của NFT.

### Khi nào cần chạy bước này?

Bắt buộc chạy:

- Lần đầu sử dụng chương trình.
- Sau khi thêm hoặc sửa pool/wallet.
- Sau khi đổi RPC, cơ sở dữ liệu hoặc gas policy.
- Sau khi cập nhật chương trình.

Có thể bỏ qua khi chỉ chạy lại chế độ liên tục với cùng chương trình, cùng cấu hình và không có công việc cũ đang chờ xử lý.

## Bước 8 — Chạy tự động liên tục

Chỉ thực hiện sau khi đã kiểm tra kết quả của Bước 7. Nhấp đúp:

```text
04_RUN_LOOP.bat
```

1. Nhập `START` để xác nhận.
2. Nhập 10 ký tự cuối của private key khi được yêu cầu.
3. Giữ máy tính bật, không để máy sleep và không đóng cửa sổ đang chạy.

Với `interval_seconds=1800`, chương trình bắt đầu một lượt mới sau mỗi 30 phút. Chỉ chạy một cửa sổ `04_RUN_LOOP.bat` cho cùng wallet.

## Bước 9 — Xem nhật ký, dừng và chạy lại

### Theo dõi nhật ký hoạt động

Nhấp đúp:

```text
05_VIEW_LOG.bat
```

Cửa sổ sẽ tự cập nhật khi có nội dung mới. Nếu chưa có file nhật ký, hãy chạy Bước 7 hoặc Bước 8 trước.

### Dừng an toàn

1. Mở cửa sổ đang chạy `04_RUN_LOOP.bat`.
2. Nếu đang có giao dịch, chờ blockchain xác nhận kết quả.
3. Nhấn `Ctrl+C` một lần.
4. Chờ cửa sổ thông báo chương trình đã dừng.

Không đóng cửa sổ, dùng End Task hoặc tắt máy khi giao dịch đang được xử lý.

### Chạy lại

- Nếu chỉ dừng bình thường và không thay đổi gì: chạy lại `04_RUN_LOOP.bat`.
- Nếu đã sửa file cấu hình, cập nhật chương trình hoặc vừa xử lý lỗi: chạy `02_CHECK_CONFIG.bat` và `03_RUN_ONE_CYCLE.bat` trước.

## Bước 10 — Thêm pool mới

1. Dừng chương trình theo hướng dẫn ở Bước 9.
2. Tạo một bản sao dự phòng của `my_rebalance_config.json`.
3. Thêm một khối thông tin pool mới vào danh sách `pools`. Phải có dấu phẩy giữa hai khối:

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

Khối thông tin thực tế phải có đầy đủ các dòng giống file mẫu tương ứng.

![Ví dụ danh sách nhiều pool](image/LOCAL_RUN_GUIDE/1784733094171.png)

4. Mỗi lần chỉ thêm một pool.
5. Giữ `max_jobs_per_cycle=1` cho pool mới.
6. Chạy `02_CHECK_CONFIG.bat`.
7. Chạy `03_RUN_ONE_CYCLE.bat`.
8. Kiểm tra kết quả trước khi chạy `04_RUN_LOOP.bat`.

## Lỗi thường gặp

### Cài đặt chương trình

| Lỗi                              | Cách xử lý                                                                                           |
| --------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Python 3.13 was not found         | Cài Python 3.13 rồi chạy lại`01_SETUP.bat`.                                                       |
| Python installation is incomplete | Cài lại Python 3.13 bằng Windows installer, xóa`.venv` tạo dở rồi chạy lại `01_SETUP.bat`. |
| `No module named ...`           | Chạy lại`01_SETUP.bat` trong đúng thư mục chương trình.                                      |
| Không thấy file BAT             | Mở đúng thư mục chính, không mở trực tiếp bên trong ZIP.                                     |

### File `.env` và cơ sở dữ liệu

| Lỗi                                     | Cách xử lý                                                                          |
| ---------------------------------------- | -------------------------------------------------------------------------------------- |
| `.env not found`                       | Bật hiển thị phần mở rộng tên file và kiểm tra file không phải`.env.txt`. |
| `Access denied` / `Unknown database` | Xin lại file`.env` đúng từ người quản trị.                                   |
| Journal table missing                    | Liên hệ người quản trị; không tự tạo hoặc sửa bảng dữ liệu.              |

### File cấu hình và private key

| Lỗi                            | Cách xử lý                                                                              |
| ------------------------------- | ------------------------------------------------------------------------------------------ |
| Config file not found           | File phải nằm trong thư mục chính và có tên`my_rebalance_config.json`.           |
| JSON syntax                     | Kiểm tra dấu phẩy, dấu ngoặc và đuôi`.json`, hoặc sao chép lại file mẫu.     |
| Replace all config placeholders | Thay toàn bộ giá trị bắt đầu bằng`YOUR_...` và `REPLACE_...`.                 |
| Prefix phải có 54 ký tự     | Kiểm tra lại 54 ký tự đầu của private key.                                          |
| Key không khớp wallet         | Dừng chương trình và kiểm tra lại hai phần của private key cùng`bot_wallet`.   |
| Không thấy ký tự khi nhập  | Đây là hành vi bảo mật bình thường. Nhập đủ 10 ký tự cuối rồi nhấn Enter. |

### Kết nối blockchain, position và giao dịch

| Lỗi                   | Cách xử lý                                                                          |
| ---------------------- | -------------------------------------------------------------------------------------- |
| `No working RPC`     | Kiểm tra mạng và liên hệ người quản trị về RPC.                              |
| Không thấy position  | Kiểm tra wallet, pool và Pancake PID.                                                |
| Không đủ gas        | Nạp BNB hoặc ETH cho wallet trước khi chạy lại.                                  |
| `STAKE_POLICY`       | Không phải lỗi; Aerodrome position đang staked nên không auto-compound.          |
| `BELOW_MIN_COMPOUND` | Không phải lỗi; fee chưa đạt ngưỡng.                                           |
| `RECOVERY_REQUIRED`  | Dừng chương trình, không sửa cơ sở dữ liệu và liên hệ người quản trị. |

## Phụ lục — Lệnh chạy thủ công

Chỉ sử dụng khi file BAT không chạy và có hướng dẫn của người quản trị.

Mở PowerShell tại thư mục chính của chương trình.

### Chuẩn bị chương trình

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

### Kiểm tra mà không gửi giao dịch

```powershell
.\.venv\Scripts\python.exe -m latest_farms.configured_pool_rebalancer.preflight `
  --project-root . `
  --config .\my_rebalance_config.json
```

### Chạy thật một lần

```powershell
.\.venv\Scripts\python.exe -m latest_farms.configured_pool_rebalancer.cli `
  --config .\my_rebalance_config.json `
  --execute
```

### Chạy tự động liên tục

```powershell
.\.venv\Scripts\python.exe -m latest_farms.configured_pool_rebalancer.cli `
  --config .\my_rebalance_config.json `
  --execute `
  --loop
```

Tài liệu kỹ thuật chi tiết: [REPORT_V2.md](REPORT_V2.md).
