# Hướng Dẫn Chạy Rebalancer + Auto-Compound Trên Máy Local

Tài liệu này hướng dẫn chạy live trên Windows. Rebalancer và auto-compound dùng chung một lệnh; rebalancer luôn chạy trước.

> `--execute` cho phép gửi transaction thật. Lần đầu chỉ cấu hình một pool, giữ `max_jobs_per_cycle=1`, `execute_burn=false` và không chạy worker khác bằng cùng wallet.

## Bước 1 — Chuẩn bị

Cần có:

- Python 3.11.
- Source code project.
- Khối cấu hình DB `.env` do người quản trị cung cấp.
- RPC cho `BNB` hoặc `BAS` trong `latest_farms/config.py`.
- Ít nhất một swap provider hoạt động: KyberSwap, 0x hoặc OKX.
- Địa chỉ wallet, pool và một NFT token ID đã biết.
- Pancake farm PID nếu pool có farm.
- Private key của wallet chạy bot.

## Bước 2 — Cài môi trường Python

Mở PowerShell tại thư mục gốc project:

```powershell
Set-Location "D:\duong-dan\toi\project"
Test-Path .\latest_farms\configured_pool_rebalancer\cli.py
```

Kết quả phải là `True`. Sau đó chạy:

```powershell
py -3.11 -m venv .venv
Set-ExecutionPolicy -Scope Process Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m latest_farms.configured_pool_rebalancer.cli --help
```

Mỗi lần mở PowerShell mới, chạy lại:

```powershell
.\.venv\Scripts\Activate.ps1
```

## Bước 3 — Tạo file `.env`

Tạo `.env` tại thư mục gốc project.

1. Dán nguyên khối `ENV` và DB variables do người quản trị gửi.
2. Không đổi host thành `127.0.0.1`, không tự tạo database và không đổi tên biến.
3. Thêm private-key prefix ở cuối file:

```dotenv
# Khối ENV và DB variables được cung cấp
ENV=...
...

CONFIGURED_REBALANCER_MAIN_PRIVATE_KEY_PREFIX=<54_KY_TU_HEX_DAU>
```

Private key phải có 64 ký tự hex sau khi bỏ `0x`:

- Lưu 54 ký tự đầu trong `.env`.
- Giữ riêng 10 ký tự cuối để nhập khi CLI hỏi.
- Không lưu full private key hoặc 10 ký tự cuối trong file/log.

Kiểm tra DB:

```powershell
python -c "from latest_farms.create_db import get_connection; c=get_connection(); print('MySQL OK'); c.close()"
```

Chỉ tiếp tục khi thấy `MySQL OK`. Đảm bảo `.env` không được commit.

## Bước 4 — Kiểm tra RPC và swap provider

Mở `latest_farms/config.py` và xác nhận:

- `RPC_URLS_2` có RPC đúng chain.
- `RPC_BACKUP_LIST` có ít nhất một RPC dự phòng.
- Có API credential cho ít nhất một provider trong KyberSwap, 0x hoặc OKX.

Nếu project đã được cấu hình sẵn, không cần sửa. Không commit RPC/API secret.

## Bước 5 — Tạo `my_rebalance_config.json`

Tạo file `my_rebalance_config.json` tại thư mục gốc với nội dung sau:

```json
{
  "version": 2,
  "interval_seconds": 1800,
  "use_legacy_position_cache": false,
  "use_db_position_cache": true,
  "wallets": {
    "main": {
      "bot_wallet": "0xYOUR_WALLET_ADDRESS",
      "private_key_prefix_env": "CONFIGURED_REBALANCER_MAIN_PRIVATE_KEY_PREFIX"
    }
  },
  "pool_defaults": {
    "wallet": "main",
    "slippage_bps": 50,
    "max_swap_price_impact_pct": 1.0,
    "min_swap_input_usd": 0.25,
    "min_swap_output_usd": 0.10,
    "max_jobs_per_cycle": 1,
    "execute_burn": false,
    "auto_bootstrap_start_block": true,
    "auto_compound": {
      "enabled": true,
      "min_interval_seconds": 21600,
      "min_compound_usd": 5.0,
      "gas_cost_multiplier": 3.0,
      "min_range_buffer_ratio": 0.1,
      "max_jobs_per_cycle": 1
    }
  },
  "pools": [
    REPLACE_WITH_ONE_POOL_OBJECT
  ],
  "discord": {
    "enabled": false,
    "webhook_url_env": "CONFIGURED_REBALANCER_DISCORD_WEBHOOK"
  }
}
```

Thay `0xYOUR_WALLET_ADDRESS` và thay `REPLACE_WITH_ONE_POOL_OBJECT` bằng đúng một mẫu dưới đây.

### Pancake V3 có farm

```json
{
  "name": "my-pancake-farm",
  "chain": "BNB",
  "dex_type": "pancake_v3_masterchef",
  "pool_address": "0xYOUR_POOL_ADDRESS",
  "pid": 123,
  "rebalance_range": {
    "mode": "price_percent",
    "lower_percent": -10.0,
    "upper_percent": 20.0
  }
}
```

`pid` hiện chưa được tự động detect. Phải nhập đúng PID nếu pool có farm.

### Pancake V3 không có farm

```json
{
  "name": "my-pancake-pool",
  "chain": "BNB",
  "dex_type": "pancake_v3",
  "pool_address": "0xYOUR_POOL_ADDRESS",
  "pid": null,
  "rebalance_range": {
    "mode": "price_percent",
    "lower_percent": -10.0,
    "upper_percent": 20.0
  }
}
```

### Aerodrome V3

```json
{
  "name": "my-aerodrome-pool",
  "chain": "BAS",
  "dex_type": "aerodrome_gauge",
  "pool_address": "0xYOUR_POOL_ADDRESS",
  "rebalance_range": {
    "mode": "price_percent",
    "lower_percent": -10.0,
    "upper_percent": 20.0
  }
}
```

Aerodrome tự đọc NPM, gauge, token, fee và tick spacing từ pool. 

Lưu ý:

- `seed_token_ids` chỉ hỗ trợ discovery, không giới hạn tool vào đúng token ID đó.
- Trạng thái staked/unstaked luôn được xác minh on-chain.
- `auto_compound.enabled=true` chạy cả hai module; đặt `false` nếu chỉ chạy rebalancer.
- `lower_percent` phải âm và `upper_percent` phải dương.

Các config chính:

| Config                            | Ý nghĩa                                                   |
| --------------------------------- | ----------------------------------------------------------- |
| `slippage_bps=50`               | Slippage tối đa 0,5%.                                     |
| `max_swap_price_impact_pct=1.0` | Bỏ route có price impact trên 1%.                        |
| `max_jobs_per_cycle=1`          | Tối đa một rebalance job cho pool trong cycle.           |
| `execute_burn=false`            | Không burn NFT cũ.                                        |
| `min_compound_usd=5.0`          | Fee phải đạt ít nhất 5 USD để được xét compound. |
| `gas_cost_multiplier=3.0`       | Fee phải đủ lớn so với tổng gas ước tính.          |
| `min_interval_seconds=21600`    | Chờ 6 giờ sau lần compound thành công.                 |

## Bước 6 — Kiểm tra config

Chạy:

```powershell
python -c "from latest_farms.configured_pool_rebalancer.settings import load_worker_config; c=load_worker_config(r'.\my_rebalance_config.json'); print(f'Config OK: {len(c.pools)} pool(s)')"
```

Kết quả phải là:

```text
Config OK: 1 pool(s)
```

Trước khi chạy live, xác nhận:

- Wallet đúng và có đủ BNB/ETH trả gas.
- Pool address, chain, token ID và Pancake PID đúng.
- Chỉ có một pool trong config đầu tiên.
- Cả hai `max_jobs_per_cycle` đều bằng `1`.
- `execute_burn=false`.
- Không có worker/tool khác gửi transaction bằng cùng wallet.
- DB không có job pending, `RECOVERY_REQUIRED` hoặc `MANUAL_RECOVERY` cho wallet này.

## Bước 7 — Xác nhận journal DB

Hỏi người quản trị xem `configured_rebalance_jobs` và `configured_compound_jobs` đã được migrate chưa.

- Nếu đã migrate: dùng lệnh live thông thường ở bước 8.
- Nếu được yêu cầu migrate: thêm `--migrate` vào lần live đầu tiên. Không dùng lại flag này ở các lần chạy sau.

## Bước 8 — Chạy live một cycle

Đóng mọi worker khác dùng cùng wallet, sau đó chạy:

```powershell
python -m latest_farms.configured_pool_rebalancer.cli `
  --config .\my_rebalance_config.json `
  --execute `
  --log-level INFO
```

Nếu cần migrate journal trong lần chạy đầu:

```powershell
python -m latest_farms.configured_pool_rebalancer.cli `
  --config .\my_rebalance_config.json `
  --migrate `
  --execute `
  --log-level INFO
```

Khi CLI hỏi, nhập 10 ký tự cuối của private key. Ký tự không hiển thị trên màn hình.

Không đóng terminal khi transaction đang pending. Chờ CLI in JSON kết quả và trả lại terminal prompt.

Kết quả theo position:

- In-range: không rebalance; auto-compound kiểm tra fee và profitability.
- Out-of-range: rebalance trước; không compound wallet đó trong cùng cycle.
- Aerodrome staked: không compound và trả `STAKE_POLICY`.
- Không đủ điều kiện: không gửi transaction.

Sau cycle đầu:

1. Lấy từng transaction hash trong output/log.
2. Kiểm tra receipt `Success` trên BscScan/BaseScan.
3. Kiểm tra owner, stake mode, range và liquidity của NFT.
4. Với compound, kiểm tra collect, swap nếu có và increase liquidity.
5. Không chạy loop nếu xuất hiện `RECOVERY_REQUIRED` hoặc `MANUAL_RECOVERY`.

## Bước 9 — Chạy liên tục

Chỉ thực hiện sau khi cycle đầu đã được đối soát:

```powershell
python -m latest_farms.configured_pool_rebalancer.cli `
  --config .\my_rebalance_config.json `
  --execute `
  --loop `
  --log-level INFO
```

`interval_seconds=1800` chạy một cycle mỗi 30 phút. Chỉ chạy một worker cho cùng wallet.

Theo dõi log bằng PowerShell thứ hai:

```powershell
Get-Content .\latest_farms\logs\configured_rebalancer_loop.log -Wait -Tail 100
```

Trước khi nhấn `Ctrl+C`, nên chờ transaction hiện tại có receipt rõ ràng.

## Bước 10 — Thêm pool mới

1. Dừng loop.
2. Thêm một pool vào mảng `pools`.
3. Chạy lại bước kiểm tra config.
4. Chạy đúng một cycle live với giới hạn một job.
5. Đối soát transaction rồi mới chạy loop lại.

## Lỗi thường gặp

| Lỗi                                     | Cách xử lý                                                                    |
| ---------------------------------------- | -------------------------------------------------------------------------------- |
| `No module named ...`                  | Activate`.venv` và chạy lại `pip install -r requirements.txt`.            |
| `config file not found`                | Chạy từ project root và kiểm tra đường dẫn config.                       |
| `invalid address`                      | Thay toàn bộ placeholder bằng địa chỉ EVM hợp lệ.                        |
| `No working RPC`                       | Kiểm tra RPC đúng chain và kết nối mạng.                                  |
| Không thấy position                    | Kiểm tra wallet, pool, PID và`seed_token_ids`.                               |
| `Access denied` / `Unknown database` | Dán lại đúng khối DB env; không tự tạo DB, liên hệ người quản trị. |
| Table journal không tồn tại           | Xác nhận migration hoặc dùng`--migrate` theo hướng dẫn ở bước 7.     |
| Key không khớp wallet                  | Dừng; kiểm tra lại prefix/suffix và`bot_wallet`.                           |
| `STRATEGY_MISMATCH`                    | Chỉ là cảnh báo; tool vẫn tôn trọng trạng thái on-chain.                |
| `STAKE_POLICY`                         | Aerodrome đang staked nên không compound.                                     |
| `BELOW_MIN_COMPOUND`                   | Fee chưa đạt ngưỡng; chờ cycle sau.                                        |
| `RECOVERY_REQUIRED`                    | Dừng worker, không sửa DB thủ công và đối chiếu transaction on-chain.   |

Chi tiết kiến trúc và state machine: [REPORT_V2.md](REPORT_V2.md).
