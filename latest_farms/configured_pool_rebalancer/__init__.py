"""
Configured pool rebalancer.

This package is intentionally isolated from the existing parasite_bot code.
It can read shared project config/helpers, but it does not require changes to
the legacy execution or rebalance engines.
"""

__all__ = [
    "settings",
    "models",
    "worker",
]
