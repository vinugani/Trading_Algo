from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from delta_exchange_bot.cli.professional_bot import main
except Exception:
    from delta_exchange_bot.cli.trading_bot import main


if __name__ == "__main__":
    main()
