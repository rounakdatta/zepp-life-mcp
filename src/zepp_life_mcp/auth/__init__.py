"""Authentication helpers for Zepp Life cloud API."""

import logging
import webbrowser
from contextlib import suppress

import keyring
from keyring.errors import PasswordDeleteError

logger = logging.getLogger(__name__)

SERVICE_NAME = "zepp-life-mcp"
ACCOUNT_NAME = "zepp_auth"


def save_token(token: str, user_id: str | None = None) -> None:
    keyring.set_password(SERVICE_NAME, f"{ACCOUNT_NAME}_token", token)
    if user_id:
        save_user_id(user_id)


def save_user_id(user_id: str) -> None:
    keyring.set_password(SERVICE_NAME, f"{ACCOUNT_NAME}_user_id", user_id)


def load_token() -> tuple[str | None, str | None]:
    try:
        token = keyring.get_password(SERVICE_NAME, f"{ACCOUNT_NAME}_token")
        user_id = keyring.get_password(SERVICE_NAME, f"{ACCOUNT_NAME}_user_id")
        return token, user_id
    except Exception:
        return None, None


def delete_token() -> None:
    with suppress(PasswordDeleteError):
        keyring.delete_password(SERVICE_NAME, f"{ACCOUNT_NAME}_token")
    with suppress(PasswordDeleteError):
        keyring.delete_password(SERVICE_NAME, f"{ACCOUNT_NAME}_user_id")


def get_auth_instructions() -> str:
    return """
Получение Zepp Life app_token:

1. Откройте https://user.huami.com/privacy2/index.html
2. Авторизуйтесь
3. Откройте DevTools -> Application -> Cookies
4. Скопируйте cookie `apptoken`

После этого выполните:
  zepp-life-mcp setup --mode cloud_session --token <ваш_токен>
"""


def open_auth_page() -> None:
    url = "https://user.huami.com/privacy2/index.html"
    logger.info(f"Opening {url}")
    webbrowser.open(url)
    print(get_auth_instructions())


def setup_interactive() -> tuple[str | None, str | None]:
    existing_token, existing_user_id = load_token()
    if existing_token:
        response = input("Использовать существующий токен? [Y/n]: ").strip().lower()
        if response in ("", "y", "yes"):
            return existing_token, existing_user_id

    print("1. Открыть инструкцию в браузере")
    print("2. Ввести токен вручную")
    choice = input("Ваш выбор [1-2]: ").strip()

    if choice == "1":
        open_auth_page()
        token = input("Введите полученный токен: ").strip()
        if token:
            save_token(token)
            return token, None
    elif choice == "2":
        token = input("Введите app_token: ").strip()
        user_id = input("Введите user_id (опционально): ").strip() or None
        if token:
            save_token(token, user_id)
            return token, user_id

    return None, None
