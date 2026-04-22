"""
Entry point for the structural full recompute update mode.
"""
try:
    from .update import run_full_update
except ImportError:  # pragma: no cover - direct script compatibility
    from update import run_full_update


def main():
    run_full_update()


if __name__ == "__main__":
    main()
