"""CLI for Zepp Life MCP."""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from zepp_life_mcp.adapters.cloud_session import CloudSessionAdapter
from zepp_life_mcp.adapters.export_file import ExportFileAdapter
from zepp_life_mcp.auth import load_token, save_token
from zepp_life_mcp.auth import setup_interactive as setup_cloud_auth
from zepp_life_mcp.config import Config, get_config_path, load_config, save_config
from zepp_life_mcp.server import main as server_main
from zepp_life_mcp.services.sync_service import SyncService
from zepp_life_mcp.storage import Database

PROGRAM_NAME = "zepp-life-mcp"


async def _check_adapter_health(adapter) -> tuple[bool, list[str]]:
    connected = await adapter.connect()
    data_types = adapter.get_available_data_types() if connected else []
    if hasattr(adapter, "close"):
        await adapter.close()
    return connected, data_types


def cmd_setup(args):
    if args.mode == "export_file" and args.export_path:
        path = Path(args.export_path).expanduser().resolve()
        if not path.exists():
            print(f"❌ Папка не существует: {path}")
            sys.exit(1)
        adapter = ExportFileAdapter(path)
        if adapter.connect():
            print(f"✅ Найдены данные: {', '.join(adapter.get_available_data_types())}")
        else:
            print("⚠️  В папке не найдены данные Zepp Life")
        config = Config(mode="export_file", export_path=path)
        save_config(config)
        print("✅ Конфигурация сохранена (export_file)")
        print(f"   Путь: {path}")
        return

    if args.mode == "cloud_session" and args.token:
        save_token(args.token, args.user_id)
        config = Config(mode="cloud_session", region=args.region or "eu")
        save_config(config)
        print("✅ Конфигурация сохранена (cloud_session)")
        print(f"   Токен: {args.token[:4]}***")
        if args.user_id:
            suffix = args.user_id[-4:]
            print(f"   User ID: {'*' * max(0, len(args.user_id) - 4)}{suffix}")
        return

    print(f"{PROGRAM_NAME} - Setup Wizard")
    print("=" * 50)
    print()
    print("Выберите режим:")
    print("1. Export File - чтение из CSV/JSON файлов")
    print("2. Zepp Cloud - прямой доступ через app_token")
    print()
    choice = input("Ваш выбор [1-2]: ").strip()

    if choice == "1":
        path = Path(input("Путь к папке с экспортом: ").strip()).expanduser().resolve()
        if not path.exists():
            print(f"❌ Папка не существует: {path}")
            sys.exit(1)
        config = Config(mode="export_file", export_path=path)
        save_config(config)
        print("✅ Конфигурация сохранена!")
        return

    if choice == "2":
        token, _ = setup_cloud_auth()
        if token:
            print("✅ Токен сохранен!")
            return
        print("❌ Не удалось получить токен")
        sys.exit(1)

    print("❌ Неверный выбор")
    sys.exit(1)


def cmd_doctor(args):
    print(f"{PROGRAM_NAME} - Doctor")
    print("=" * 50)
    print()
    config_path = get_config_path()
    print(f"Конфигурация: {config_path}")

    if not config_path.exists():
        print("❌ Конфигурация не найдена")
        print(f"   Запустите: {PROGRAM_NAME} setup")
        sys.exit(1)

    try:
        config = load_config()
        print("✅ Конфигурация загружена")
        print(f"   Режим: {config.mode}")

        if config.mode == "export_file":
            if config.export_path:
                print(f"   Путь к экспорту: {config.export_path}")
                if config.export_path.exists():
                    adapter = ExportFileAdapter(config.export_path)
                    if adapter.connect():
                        print(f"✅ Данные найдены: {', '.join(adapter.get_available_data_types())}")
                    else:
                        print("⚠️  Данные не найдены в указанной папке")
                else:
                    print("❌ Папка с экспортом не существует")
            else:
                print("❌ Путь к экспорту не настроен")

        elif config.mode == "cloud_session":
            token, user_id = load_token()
            if token:
                print(f"✅ Токен найден: {token[:4]}***")
                adapter = CloudSessionAdapter(token, user_id, region=config.region)
                connected, data_types = asyncio.run(_check_adapter_health(adapter))
                print(f"   Подключение: {'✅' if connected else '❌'}")
                if connected:
                    print(f"   Типы данных: {', '.join(data_types)}")
            else:
                print("❌ Токен не найден")
                print(f"   Запустите: {PROGRAM_NAME} setup")

        print()
        print(f"База данных: {config.database_path}")
        if config.database_path.exists():
            Database(config.database_path)
            print("✅ База данных доступна")
        else:
            print("ℹ️  База данных будет создана при первом запуске")
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)


async def cmd_sync_async(args):
    print(f"{PROGRAM_NAME} - Sync")
    print("=" * 50)
    print()

    try:
        config = load_config()
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)

    if config.mode == "not_configured":
        print("❌ Сервер не настроен")
        print(f"   Запустите: {PROGRAM_NAME} setup")
        sys.exit(1)

    db = Database(config.database_path)

    if config.mode == "export_file":
        if not config.export_path:
            print("❌ Путь к экспорту не настроен")
            sys.exit(1)
        adapter = ExportFileAdapter(config.export_path)
        if not adapter.connect():
            print("❌ Не удалось подключиться к данным")
            sys.exit(1)
    elif config.mode == "cloud_session":
        token, user_id = load_token()
        if not token:
            print("❌ Токен не найден")
            print(f"   Запустите: {PROGRAM_NAME} setup")
            sys.exit(1)
        adapter = CloudSessionAdapter(
            token, user_id, config.region, config.timezone, api_host=config.api_host
        )
        if not await adapter.connect():
            print("❌ Не удалось подключиться к API")
            sys.exit(1)
    else:
        print(f"❌ Неизвестный режим: {config.mode}")
        sys.exit(1)

    sync_service = SyncService(adapter, db, archive_raw=config.store_raw_payloads)
    data_types = [args.type] if args.type else adapter.get_available_data_types()
    print(f"Синхронизация {len(data_types)} типов данных...")
    print()

    for data_type in data_types:
        try:
            result = await sync_service.sync_data_type(
                data_type=data_type,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            print(f"✅ {data_type}: {result['added']} добавлено, {result['updated']} обновлено")
        except Exception as e:
            print(f"❌ {data_type}: {e}")

    print()
    print("Синхронизация завершена!")


def cmd_sync(args):
    asyncio.run(cmd_sync_async(args))


def main():
    parser = argparse.ArgumentParser(prog=PROGRAM_NAME, description="MCP server for Zepp Life data")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    serve_parser = subparsers.add_parser("serve", help="Run MCP server")
    serve_parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default=os.environ.get("ZEPP_MCP_TRANSPORT", "stdio"),
        help="stdio for a local MCP client, http to serve over the network",
    )
    serve_parser.add_argument("--host", default=os.environ.get("ZEPP_MCP_HOST", "0.0.0.0"))
    serve_parser.add_argument(
        "--port", type=int, default=int(os.environ.get("ZEPP_MCP_PORT", "8080"))
    )
    serve_parser.add_argument(
        "--read-only",
        action="store_true",
        default=os.environ.get("ZEPP_MCP_READ_ONLY", "").lower() in ("1", "true", "yes"),
        help="Disable sync_data so a separate writer owns the database",
    )

    setup_parser = subparsers.add_parser("setup", help="Configure the server")
    setup_parser.add_argument("--mode", choices=["export_file", "cloud_session"], help="Setup mode")
    setup_parser.add_argument("--export-path", help="Path to export files (for export_file mode)")
    setup_parser.add_argument("--token", help="App token (for cloud_session mode)")
    setup_parser.add_argument("--user-id", help="User ID (optional, for cloud_session mode)")
    setup_parser.add_argument("--region", help="Cloud region")

    subparsers.add_parser("doctor", help="Check configuration and diagnose issues")

    sync_parser = subparsers.add_parser("sync", help="Sync data from source")
    sync_parser.add_argument(
        "--type",
        choices=[
            "daily_activity",
            "sleep",
            "heart_rate",
            "workouts",
            "workout_details",
            "body_measurements",
        ],
        help="Type of data to sync",
    )
    sync_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    sync_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")

    args = parser.parse_args()
    if args.command == "serve" or args.command is None:
        # The bearer token is env-only on purpose: an argv secret is visible in
        # `ps` and in the pod spec.
        asyncio.run(
            server_main(
                transport=getattr(args, "transport", "stdio"),
                host=getattr(args, "host", "0.0.0.0"),
                port=getattr(args, "port", 8080),
                auth_token=os.environ.get("ZEPP_MCP_AUTH_TOKEN") or None,
                read_only=getattr(args, "read_only", False),
            )
        )
    elif args.command == "setup":
        cmd_setup(args)
    elif args.command == "doctor":
        cmd_doctor(args)
    elif args.command == "sync":
        cmd_sync(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
