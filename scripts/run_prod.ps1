cd $PSScriptRoot\..\
poetry run python scripts/live_preflight.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
poetry run python scripts/run_bot.py --mode live --strategy portfolio
