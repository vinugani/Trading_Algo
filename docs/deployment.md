# Deployment

- Use `poetry build` and containerize with `Dockerfile` for production.
- Provide environment variables in secrets manager.
- Run with `poetry run python -m delta_exchange_bot.cli.main --mode live`.
- Add monitoring for trade execution latency and error rates.
