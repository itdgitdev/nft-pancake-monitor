from __future__ import annotations

from collections.abc import Mapping

from eth_account import Account
from web3 import Web3


class RuntimeSigner:
    """In-memory transaction signer for live runs."""

    def __init__(self, private_keys_by_wallet: Mapping[str, str]):
        self._private_keys_by_wallet: dict[str, str] = {}
        for wallet, private_key in private_keys_by_wallet.items():
            wallet_cs = Web3.to_checksum_address(wallet)
            key = str(private_key or "").strip()
            if not key:
                raise ValueError(f"empty private key for wallet {wallet_cs}")
            try:
                account = Account.from_key(key)
            except Exception as exc:
                raise ValueError(f"invalid private key for wallet {wallet_cs}") from exc
            if Web3.to_checksum_address(account.address) != wallet_cs:
                raise ValueError(f"private key does not match bot_wallet {wallet_cs}")
            self._private_keys_by_wallet[wallet_cs] = key

    def __repr__(self) -> str:
        wallets = ", ".join(sorted(self._private_keys_by_wallet))
        return f"RuntimeSigner(wallets=[{wallets}])"

    def sign_transaction(self, wallet: str, tx: dict):
        wallet_cs = Web3.to_checksum_address(wallet)
        private_key = self._private_keys_by_wallet.get(wallet_cs)
        if not private_key:
            raise RuntimeError(f"missing runtime private key for wallet {wallet_cs}")
        return Account.sign_transaction(tx, private_key)

