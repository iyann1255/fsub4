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
    def upsert(self, rec: FileRecord) -> None: ...
    def get(self, file_id: str) -> Optional[FileRecord]: ...

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
        cur = self.conn.execute("SELECT file_id, db_chat_id, db_message_id, kind, caption FROM files WHERE file_id=?",
                                (file_id,))
        row = cur.fetchone()
        if not row:
            return None
        return FileRecord(*row)

class MongoStorage:
    def __init__(self, uri: str, db_name: str) -> None:
        if not uri:
            raise ValueError("MONGO_URI kosong")
        self.client = MongoClient(uri)
        self.col = self.client[db_name]["files"]
        self.col.create_index("file_id", unique=True)

    def upsert(self, rec: FileRecord) -> None:
        self.col.update_one(
            {"file_id": rec.file_id},
            {"$set": rec.__dict__},
            upsert=True
        )

    def get(self, file_id: str) -> Optional[FileRecord]:
        doc = self.col.find_one({"file_id": file_id}, {"_id": 0})
        if not doc:
            return None
        return FileRecord(**doc)

def build_storage(backend: str, mongo_uri: str, mongo_db: str) -> Storage:
    backend = (backend or "sqlite").lower()
    if backend == "mongo":
        return MongoStorage(mongo_uri, mongo_db)
    return SQLiteStorage(os.getenv("SQLITE_PATH", "data.db"))
