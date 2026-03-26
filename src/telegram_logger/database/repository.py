from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from telegram_logger.database.methods import (
    delete_expired_messages_from_db,
    get_message_ids_by_event,
    message_exists,
    save_message,
)
from telegram_logger.database.models import register_models


@dataclass(slots=True)
class MessageEventRow:
    id: int
    from_id: int
    chat_id: int
    type: int
    msg_text: str | None
    media: bytes | None
    noforwards: bool
    self_destructing: bool


class MessageRepository:
    def __init__(self, sqlite_url: str):
        self.sqlite_url = sqlite_url

    async def init(self) -> None:
        await register_models()

    async def message_exists(self, msg_id: int, chat_id: int) -> bool:
        return await message_exists(msg_id, chat_id)

    async def save_message(self, **kwargs) -> None:
        await save_message(
            msg_id=kwargs["id"],
            from_id=kwargs["from_id"],
            chat_id=kwargs["chat_id"],
            type=kwargs["type"],
            msg_text=kwargs["msg_text"],
            media=kwargs["media"],
            noforwards=kwargs["noforwards"],
            self_destructing=kwargs["self_destructing"],
            created_at=kwargs["created_at"],
            edited_at=kwargs["edited_at"],
        )

    async def get_messages_by_event(
        self,
        chat_id: int | None,
        ids: Sequence[int],
        include_dm_where_chat_id_missing: bool = True,
    ):
        class _Event:
            pass

        event = _Event()
        event.chat_id = chat_id
        rows = await get_message_ids_by_event(event, list(ids))

        result: list[MessageEventRow] = []
        for row in rows:
            mapping = getattr(row, "_mapping", {})

            result.append(
                MessageEventRow(
                    id=mapping.get("id", row[0]),
                    from_id=mapping.get("from_id", row[1]),
                    chat_id=mapping.get("chat_id", row[2]),
                    type=mapping.get("type", row[3]),
                    msg_text=mapping.get("msg_text", row[4]),
                    media=mapping.get("media", row[5]),
                    noforwards=mapping.get("noforwards", row[6]),
                    self_destructing=mapping.get("self_destructing", row[7]),
                )
            )

        return result

    async def delete_expired_messages(self, current_time):
        await delete_expired_messages_from_db(current_time)
