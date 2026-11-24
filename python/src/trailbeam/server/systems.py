"""Message keys: a message is (messaging system, message ID), as one text (contract/span-contract-amqp.md A6).

A store keys a message by one text, its ``mid`` (store.py); every query, index, summary and page works with it.
From schema version 4 a message is the pair (``messaging.system.name``, ``messaging.message.id``), so that an
SQS message and a RabbitMQ message whose application set the same ID text are two messages. The pair is written
as one text by ``message_key``, and read back by ``split``:

- **SQS** (and every span before 4.0): the message ID as SQS gave it, unchanged. A store's existing rows are
  keyed this way already, so no row, summary or URL of an SQS message changes, and no migration rewrites them.
- **Any other system**: ``~<system>~<message ID>``, such as ``~rabbitmq~order-17``.

The two can't meet: an SQS key never starts with ``~``. SQS's message IDs are UUIDs and never do; if a span
names one that does anyway (an emulator, a hand-made span), its key is ``~aws.sqs~<ID>``, which ``split`` reads
back as SQS. ``~`` is unreserved in URLs, so a key goes in a path segment or query string as it is.

The key is what the API's message ID is (``/api/messages/<key>``). The ID the span carried stays in the
span's attributes (``messaging.message.id``).

A store before this module may hold an SQS message whose ID started with ``~``, under the raw ID: none from SQS
itself. It stays readable under that key; a span of it stored from now on goes to ``~aws.sqs~<ID>``.
"""
from __future__ import annotations

from typing import Optional

SQS = "aws.sqs"
SNS = "aws.sns"
RABBITMQ = "rabbitmq"

MARK = "~"
# The systems whose keys are written with their name. Only the name of a system Trailbeam reads goes in a key, so
# ``split`` knows every one.
NAMED = (RABBITMQ, SQS)


def message_key(system: str, message_id: Optional[str]) -> Optional[str]:
    """The key of the message ``message_id`` of ``system`` (``SQS`` for every SQS span); None without an ID."""
    if message_id is None or message_id == "":
        return None
    if system == SQS and not message_id.startswith(MARK):
        return message_id
    if system not in NAMED:
        raise ValueError(f"no message key for messaging system {system!r}")
    return f"{MARK}{system}{MARK}{message_id}"


def split(key: str) -> tuple[str, str]:
    """(system, message ID) of a key ``message_key`` wrote. A key it could not have written is an SQS ID."""
    if key.startswith(MARK):
        system, sep, message_id = key[1:].partition(MARK)
        if sep and system in NAMED and message_id:
            return system, message_id
    return SQS, key


def system_of(key: Optional[str]) -> Optional[str]:
    return None if key is None else split(key)[0]
