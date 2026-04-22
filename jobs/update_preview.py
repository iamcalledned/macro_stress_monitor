"""
Entry point for the intraday market-proxy preview update mode.
"""
try:
    from .update import run_preview_update
except ImportError:  # pragma: no cover - direct script compatibility
    from update import run_preview_update


def main():
    run_preview_update()


if __name__ == "__main__":
    main()
