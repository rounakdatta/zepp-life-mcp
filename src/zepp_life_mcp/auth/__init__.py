"""Authentication helpers for Zepp Life cloud API."""

import logging
import os
import webbrowser
from contextlib import suppress
from pathlib import Path

import keyring
from keyring.errors import PasswordDeleteError

logger = logging.getLogger(__name__)

SERVICE_NAME = "zepp-life-mcp"
ACCOUNT_NAME = "zepp_auth"

# A container has no Secret Service, so keyring raises there and the old
# load_token() swallowed it and reported "not configured". These let the
# credential arrive from a k8s Secret instead, as an env var or a mounted file.
ENV_TOKEN = "ZEPP_APP_TOKEN"
ENV_TOKEN_FILE = "ZEPP_APP_TOKEN_FILE"
ENV_USER_ID = "ZEPP_USER_ID"


def _credentials_from_environment() -> tuple[str | None, str | None]:
    token = os.environ.get(ENV_TOKEN) or None

    token_file = os.environ.get(ENV_TOKEN_FILE)
    if not token and token_file:
        try:
            token = Path(token_file).read_text(encoding="utf-8").strip() or None
        except OSError as exc:
            # Loud: a mounted Secret that cannot be read is a deploy fault, and
            # falling through to "not configured" would hide it.
            logger.error("Could not read %s=%s: %s", ENV_TOKEN_FILE, token_file, exc)

    return token, os.environ.get(ENV_USER_ID) or None


def save_token(token: str, user_id: str | None = None) -> None:
    _set_password(f"{ACCOUNT_NAME}_token", token)
    if user_id:
        save_user_id(user_id)


def save_user_id(user_id: str) -> None:
    _set_password(f"{ACCOUNT_NAME}_user_id", user_id)


def _set_password(key: str, value: str) -> None:
    try:
        keyring.set_password(SERVICE_NAME, key, value)
    except Exception as exc:
        # Discovering the UID must not crash a containerised run that was
        # configured by environment in the first place.
        logger.warning("Keyring unavailable, not persisting %s: %s", key, exc)


def load_token() -> tuple[str | None, str | None]:
    """Resolve credentials from the environment first, then the system keyring.

    Environment wins so a deployed instance is configured by exactly one thing
    and never picks up a stale keyring entry from the image or the host.
    """
    token, user_id = _credentials_from_environment()
    if token:
        return token, user_id

    try:
        keyring_token = keyring.get_password(SERVICE_NAME, f"{ACCOUNT_NAME}_token")
        keyring_user_id = keyring.get_password(SERVICE_NAME, f"{ACCOUNT_NAME}_user_id")
        return keyring_token, user_id or keyring_user_id
    except Exception as exc:
        logger.debug("Keyring unavailable: %s", exc)
        return None, user_id


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
