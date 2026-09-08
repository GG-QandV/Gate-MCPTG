import asyncio
from typing import Annotated, Optional

import typer
from typer import Context, Option, Typer

app = Typer()


@app.callback(invoke_without_command=True)
def _run(ctx: Context) -> None:
    if ctx.invoked_subcommand is None:
        # This will run if no subcommand is specified
        run()


@app.command()
def sign_in(
    api_id: Annotated[str, Option(help="Telegram API id")],
    api_hash: Annotated[str, Option(help="Telegram API hash")],
    phone_number: Annotated[Optional[str], Option(help="Phone number with country code")] = None,
    qr: Annotated[bool, Option("--qr", help="Use QR code authentication")] = False,
) -> None:
    """Connect to Telegram API using phone number or QR code."""
    # Валидация параметров
    if not qr and phone_number is None:
        raise typer.BadParameter("Phone number is required when not using QR authentication")
    
    from .telegram import connect_to_telegram

    asyncio.run(connect_to_telegram(api_id, api_hash, phone_number, use_qr=qr))


@app.command()
def run() -> None:
    """Run the mcp-telegram server (single-process v1)."""
    from .server import run_mcp_server

    asyncio.run(run_mcp_server())


@app.command()
def daemon() -> None:
    """Run tgmcpd daemon (persistent + IPC)."""
    from .daemon import main as daemon_main

    asyncio.run(daemon_main())


@app.command()
def proxy() -> None:
    """Run tg-mcp-proxy (stdio ↔ IPC bridge)."""
    from .proxy import run as proxy_run

    asyncio.run(proxy_run())


@app.command()
def logout() -> None:
    """Logout from Telegram API."""
    from .telegram import logout_from_telegram

    asyncio.run(logout_from_telegram())
