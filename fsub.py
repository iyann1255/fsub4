from __future__ import annotations
from typing import Iterable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

async def is_user_joined_all(context: ContextTypes.DEFAULT_TYPE, user_id: int, targets: list[str]) -> bool:
    if not targets:
        return True

    for chat in targets:
        try:
            member = await context.bot.get_chat_member(chat_id=chat, user_id=user_id)
            status = getattr(member, "status", None)
            # status: "member", "administrator", "creator", "restricted", "left", "kicked"
            if status in ("left", "kicked"):
                return False
        except Exception:
            # kalau bot nggak bisa cek member (misal target private + bot belum jadi admin),
            # anggap belum join biar aman
            return False
    return True

def build_join_keyboard(
    targets: list[str],
    buttons_per_row: int,
    join_text: str,
    done_callback_data: str,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []

    # tombol join (unlimited)
    buf: list[InlineKeyboardButton] = []
    for idx, t in enumerate(targets, start=1):
        # URL join:
        # - kalau @username => https://t.me/username
        # - kalau -100id => butuh invite link; kalau private wajib kamu isi yg @username / link publik
        if str(t).startswith("@"):
            url = f"https://t.me/{t.lstrip('@')}"
        else:
            # fallback: tidak ideal untuk private ID-only
            url = "https://t.me/"
        buf.append(InlineKeyboardButton(f"{join_text} {idx}", url=url))

        if len(buf) >= buttons_per_row:
            rows.append(buf)
            buf = []
    if buf:
        rows.append(buf)

    # tombol sudah join
    rows.append([InlineKeyboardButton("✅ Sudah Join", callback_data=done_callback_data)])
    return InlineKeyboardMarkup(rows)
