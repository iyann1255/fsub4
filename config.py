import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

def _getenv(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def _parse_ids(raw: str) -> set[int]:
    out: set[int] = set()
    for part in (raw or "").replace(",", " ").split():
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
        elif part.startswith("-") and part[1:].isdigit():
            out.add(int(part))
    return out

def _collect_force_sub() -> list[str]:
    targets: list[str] = []
    i = 1
    while True:
        v = _getenv(f"FORCE_SUB{i}")
        if not v:
            break
        targets.append(v)
        i += 1
    return targets

@dataclass(frozen=True)
class Config:
    bot_token: str
    owner_id: int
    channel_id: int
    admins: set[int]
    force_sub_targets: list[str]

    buttons_per_row: int
    join_text: str

    start_message: str
    force_sub_message: str

    secret_key: str

    storage_backend: str
    mongo_uri: str
    mongo_db: str

def load_config() -> Config:
    bot_token = _getenv("BOT_TOKEN")
    if not bot_token:
        raise SystemExit("ENV BOT_TOKEN belum diisi")

    owner_id = int(_getenv("OWNER_ID", "0") or "0")
    channel_id = int(_getenv("CHANNEL_ID", "0") or "0")
    if not owner_id or not channel_id:
        raise SystemExit("ENV OWNER_ID / CHANNEL_ID wajib diisi")

    admins = _parse_ids(_getenv("ADMINS"))
    admins.add(owner_id)

    targets = _collect_force_sub()

    buttons_per_row = int(_getenv("BUTTONS_PER_ROW", "3") or "3")
    join_text = _getenv("BUTTONS_JOIN_TEXT", "ᴊᴏɪɴ") or "ᴊᴏɪɴ"

    start_message = _getenv("START_MESSAGE", "<b>Hai {mention}</b>")
    force_sub_message = _getenv("FORCE_SUB_MESSAGE", "<b>Wajib join dulu</b>")

    secret_key = _getenv("SECRET_KEY")
    if not secret_key or len(secret_key) < 16:
        raise SystemExit("ENV SECRET_KEY wajib diisi (min 16 char)")

    storage_backend = (_getenv("STORAGE_BACKEND", "sqlite") or "sqlite").lower()
    mongo_uri = _getenv("MONGO_URI")
    mongo_db = _getenv("MONGO_DB", "fsub")

    return Config(
        bot_token=bot_token,
        owner_id=owner_id,
        channel_id=channel_id,
        admins=admins,
        force_sub_targets=targets,
        buttons_per_row=max(1, min(buttons_per_row, 8)),
        join_text=join_text,
        start_message=start_message,
        force_sub_message=force_sub_message,
        secret_key=secret_key,
        storage_backend=storage_backend,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
    )
