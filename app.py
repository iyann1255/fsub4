from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from telegram import Update
from telegram.error import NetworkError, TimedOut
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import load_config
from fsub import build_join_keyboard, is_user_joined_all
from shortlink import gen_code
from storage import FileRecord, build_storage

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("fsub-modern")

CFG = load_config()
STORE = build_storage(CFG.storage_backend, CFG.mongo_uri, CFG.mongo_db)

CB_DONE = "fsub_done"


def _mention_html(user) -> str:
    name = (user.first_name or "bro").replace("<", "").replace(">", "")
    return f"<a href='tg://user?id={user.id}'>{name}</a>"


def _admin_only(user_id: int) -> bool:
    return user_id in CFG.admins


async def _retryable_copy_message(context, chat_id, from_chat_id, message_id, tries: int = 3):
    """
    Retry copy_message on transient network errors/timeouts.
    """
    last_err = None
    for i in range(tries):
        try:
            return await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=from_chat_id,
                message_id=message_id,
            )
        except (TimedOut, NetworkError) as e:
            last_err = e
            await asyncio.sleep(1.5 * (i + 1))
    raise last_err


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    if not u or not update.message:
        return
    text = CFG.start_message.format(mention=_mention_html(u))
    await update.message.reply_html(text, disable_web_page_preview=True)


async def gate_or_send(update: Update, context: ContextTypes.DEFAULT_TYPE, file_id: str) -> None:
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return

    ok = await is_user_joined_all(context, u.id, CFG.force_sub_targets)
    if not ok:
        kb = build_join_keyboard(
            CFG.force_sub_targets,
            CFG.buttons_per_row,
            CFG.join_text,
            done_callback_data=f"{CB_DONE}:{file_id}",
        )
        await msg.reply_html(CFG.force_sub_message, reply_markup=kb, disable_web_page_preview=True)
        return

    rec = STORE.get(file_id)
    if not rec:
        await msg.reply_text("File tidak ditemukan / sudah dihapus dari database channel.")
        return

    try:
        await _retryable_copy_message(
            context,
            msg.chat_id,
            rec.db_chat_id,
            rec.db_message_id,
        )
    except Exception as e:
        log.exception("copy_message failed: %s", e)
        await msg.reply_text("Gagal ambil file dari channel database. Pastikan bot admin + izin post & read.")


async def deep_link_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # /start <payload>
    if not update.message or not update.effective_user:
        return

    args = context.args
    if not args:
        await start_cmd(update, context)
        return

    code = args[0].strip()

    # Anti-share: claim-on-first-use
    status, file_id = STORE.claim_link(code, update.effective_user.id)

    if status == "INVALID" or not file_id:
        await update.message.reply_text("Link invalid / sudah tidak berlaku.")
        return

    if status == "NOT_OWNER":
        await update.message.reply_text("⛔ Link ini sudah terikat ke akun lain. Minta link baru ya.")
        return

    await gate_or_send(update, context, file_id)


async def done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not q.from_user:
        return

    data = (q.data or "")
    if not data.startswith(f"{CB_DONE}:"):
        await q.answer()
        return

    file_id = data.split(":", 1)[1].strip()

    ok = await is_user_joined_all(context, q.from_user.id, CFG.force_sub_targets)
    if not ok:
        await q.answer("Masih belum join semua ya.", show_alert=True)
        return

    await q.answer()

    # delete gate message (optional)
    try:
        await q.message.delete()
    except Exception:
        pass

    fake_update = Update(update.update_id, message=q.message)
    await gate_or_send(fake_update, context, file_id)


async def save_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    u = update.effective_user
    if not msg or not u:
        return

    if not _admin_only(u.id):
        return

    kind = None
    if msg.document:
        kind = "document"
    elif msg.video:
        kind = "video"
    elif msg.photo:
        kind = "photo"
    elif msg.audio:
        kind = "audio"
    elif msg.voice:
        kind = "voice"
    else:
        return

    try:
        copied = await _retryable_copy_message(
            context,
            CFG.channel_id,
            msg.chat_id,
            msg.message_id,
        )
    except Exception as e:
        log.exception("copy to db channel failed: %s", e)
        await msg.reply_text("Gagal simpan ke channel database. Pastikan bot admin di channel itu + izin post.")
        return

    file_id = str(uuid4())
    STORE.upsert(
        FileRecord(
            file_id=file_id,
            db_chat_id=CFG.channel_id,
            db_message_id=copied.message_id,
            kind=kind,
            caption=msg.caption_html if msg.caption_html else None,
        )
    )

    me = await context.bot.get_me()
    if not me.username:
        await msg.reply_text("Bot belum punya username. Set dulu di @BotFather biar link /start bisa dipakai.")
        return

    code = None
    for _ in range(20):
        c = gen_code(10)
        if not STORE.get_file_id_by_code(c):
            code = c
            break
    if not code:
        await msg.reply_text("Gagal generate code unik. Coba ulang.")
        return

    STORE.save_link(code, file_id)

    link = f"https://t.me/{me.username}?start={code}"
    await msg.reply_html(
        f"<b>Saved.</b>\n\nLink:\n<code>{link}</code>\n\n<i>Note:</i> Link akan terkunci ke akun pertama yang membukanya.",
        disable_web_page_preview=True,
    )


def main() -> None:
    request = HTTPXRequest(
        connect_timeout=20,
        read_timeout=60,
        write_timeout=60,
        pool_timeout=20,
    )

    app: Application = (
        ApplicationBuilder()
        .token(CFG.bot_token)
        .request(request)
        .build()
    )

    app.add_handler(CommandHandler("start", deep_link_start))
    app.add_handler(CallbackQueryHandler(done_cb, pattern=r"^fsub_done:"))
    app.add_handler(
        MessageHandler(
            filters.ALL
            & (filters.Document.ALL | filters.VIDEO | filters.PHOTO | filters.AUDIO | filters.VOICE),
            save_file,
        )
    )

    log.info("Bot running...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
