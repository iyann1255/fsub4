from __future__ import annotations
import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Protocol

from pymongo import MongoClient


@dataclass
class FileRecord:
    file_id: str
    db_chat_id: int
    db_message_id: int
    kind: str  # "document" | "video" | "photo" | "audio" | etc
    caption: str | None = None


class Storage(Protocol):
    # files
    def upsert(self, rec: FileRecord) -> None: ...
    def get(self, file_id: str) -> Optional[FileRecord]: ...

    # short links
    def save_link(self, code: str, file_id: str) -> None: ...
    def get_file_id_by_code(self, code: str) -> Optional[str]: ...

    # anti-share (claim-on-first-use)
    def claim_link(self, code: str, user_id: int) -> tuple[str, Optional[str]]: ...


class SQLiteStorage:
    def __init__(self, path: str = "data.db") -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            db_chat_id INTEGER NOT NULL,
            db_message_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            caption TEXT
        )
        """)

        # mapping kode pendek -> file_id + pemilik (anti share)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS links (
            code TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            owner_user_id INTEGER
        )
        """)
        self.conn.commit()

    def upsert(self, rec: FileRecord) -> None:
        self.conn.execute("""
        INSERT INTO files(file_id, db_chat_id, db_message_id, kind, caption)
        VALUES(?,?,?,?,?)
        ON CONFLICT(file_id) DO UPDATE SET
          db_chat_id=excluded.db_chat_id,
          db_message_id=excluded.db_message_id,
          kind=excluded.kind,
          caption=excluded.caption
        """, (rec.file_id, rec.db_chat_id, rec.db_message_id, rec.kind, rec.caption))
        self.conn.commit()

    def get(self, file_id: str) -> Optional[FileRecord]:
        cur = self.conn.execute(
            "SELECT file_id, db_chat_id, db_message_id, kind, caption FROM files WHERE file_id=?",
            (file_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return FileRecord(*row)

    # ===== short link methods =====
    def save_link(self, code: str, file_id: str) -> None:
        # Kalau code sudah ada, jangan overwrite owner_user_id
        self.conn.execute("""
        INSERT INTO links(code, file_id, owner_user_id)
        VALUES(?, ?, NULL)
        ON CONFLICT(code) DO UPDATE SET
          file_id=excluded.file_id
        """, (code, file_id))
        self.conn.commit()

    def get_file_id_by_code(self, code: str) -> Optional[str]:
        cur = self.conn.execute("SELECT file_id FROM links WHERE code=?", (code,))
        row = cur.fetchone()
        return row[0] if row else None

    def claim_link(self, code: str, user_id: int) -> tuple[str, Optional[str]]:
        """
        Return (status, file_id)
        status:
          - "OK"        -> boleh ambil, file_id valid
          - "NOT_OWNER" -> sudah di-claim user lain
          - "INVALID"   -> code tidak ada
        """
        cur = self.conn.execute("SELECT file_id, owner_user_id FROM links WHERE code=?", (code,))
        row = cur.fetchone()
        if not row:
            return "INVALID", None

        file_id, owner = row[0], row[1]

        if owner is None:
            # Claim pertama kali (coba atomic)
            self.conn.execute(
                "UPDATE links SET owner_user_id=? WHERE code=? AND owner_user_id IS NULL",
                (user_id, code),
            )
            self.conn.commit()

            # Re-check siapa yang berhasil claim
            cur2 = self.conn.execute("SELECT owner_user_id FROM links WHERE code=?", (code,))
            owner2 = cur2.fetchone()[0]
            if int(owner2) == int(user_id):
                return "OK", file_id
            return "NOT_OWNER", None

        if int(owner) != int(user_id):
            return "NOT_OWNER", None

        return "OK", file_id


class MongoStorage:
    def __init__(self, uri: str, db_name: str) -> None:
        if not uri:
            raise ValueError("MONGO_URI kosong")
        self.client = MongoClient(uri)
        db = self.client[db_name]

        self.files = db["files"]
        self.files.create_index("file_id", unique=True)

        self.links = db["links"]
        self.links.create_index("code", unique=True)

    def upsert(self, rec: FileRecord) -> None:
        self.files.update_one(
            {"file_id": rec.file_id},
            {"$set": rec.__dict__},
            upsert=True
        )

    def get(self, file_id: str) -> Optional[FileRecord]:
        doc = self.files.find_one({"file_id": file_id}, {"_id": 0})
        if not doc:
            return None
        return FileRecord(**doc)

    # ===== short link methods =====
    def save_link(self, code: str, file_id: str) -> None:
        # Jangan overwrite owner_user_id kalau sudah ada
        self.links.update_one(
            {"code": code},
            {"$set": {"file_id": file_id}, "$setOnInsert": {"code": code, "owner_user_id": None}},
            upsert=True
        )

    def get_file_id_by_code(self, code: str) -> Optional[str]:
        doc = self.links.find_one({"code": code}, {"_id": 0, "file_id": 1})
        return doc["file_id"] if doc else None

    def claim_link(self, code: str, user_id: int) -> tuple[str, Optional[str]]:
        doc = self.links.find_one({"code": code}, {"_id": 0, "file_id": 1, "owner_user_id": 1})
        if not doc:
            return "INVALID", None

        owner = doc.get("owner_user_id", None)
        file_id = doc.get("file_id")

        if owner is None:
            res = self.links.update_one(
                {"code": code, "owner_user_id": None},
                {"$set": {"owner_user_id": int(user_id)}}
            )
            if res.modified_count == 1:
                return "OK", file_id
            return "NOT_OWNER", None

        if int(owner) != int(user_id):
            return "NOT_OWNER", None

        return "OK", file_id


def build_storage(backend: str, mongo_uri: str, mongo_db: str) -> Storage:
    backend = (backend or "sqlite").lower()
    if backend == "mongo":
        return MongoStorage(mongo_uri, mongo_db)
    return SQLiteStorage(os.getenv("SQLITE_PATH", "data.db"))
