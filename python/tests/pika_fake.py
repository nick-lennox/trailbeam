"""A real ``pika.channel.Channel`` on a fake connection, so a test can play the broker frame by frame.

The connection records the methods the channel sends and hands frames to the channel the way
``pika.connection.Connection`` does: a method frame through the callback manager, content through
``_handle_content_frame``'s targets.
"""
from __future__ import annotations

import pika
import pika.callback
import pika.channel
from pika import frame, spec


class FakeConnection:
    publisher_confirms = True
    basic_nack = True
    accept_consumer_cancel_ok = True

    def __init__(self, host="localhost", port=5672, vhost="/"):
        self.params = pika.ConnectionParameters(host=host, port=port, virtual_host=vhost)
        self.callbacks = pika.callback.CallbackManager()
        self.sent = []  # methods and published messages, in order
        self.fail_writes = None  # an exception class: the next write raises it

    def _send_method(self, channel_number, method, content=None):
        self._write()
        self.sent.append(method if content is None else (method, content))

    def _marshal_message(self, channel_number, method, content):
        return [(method, content)]

    def _output_marshaled_frames(self, frames):
        self._write()
        self.sent.extend(frames)

    def _write(self):
        if self.fail_writes is not None:
            err, self.fail_writes = self.fail_writes, None
            raise err("write failed")


def reply(ch: pika.channel.Channel, method) -> None:
    """A method frame from the broker on ``ch``'s channel."""
    ch.connection.callbacks.process(ch.channel_number, method, ch.connection, frame.Method(ch.channel_number, method))


def open_channel(conn: FakeConnection, number: int = 1) -> pika.channel.Channel:
    ch = pika.channel.Channel(conn, number, lambda _ch: None)
    ch.open()
    reply(ch, spec.Channel.OpenOk())
    assert ch.is_open
    return ch


def consume(ch, queue: str, consumer_tag: str, auto_ack: bool = False, on_message=None) -> str:
    tag = ch.basic_consume(queue, on_message or (lambda *a: None), auto_ack=auto_ack, consumer_tag=consumer_tag)
    reply(ch, spec.Basic.ConsumeOk(consumer_tag=tag))
    return tag


def props(properties: dict, headers: dict | None = None) -> spec.BasicProperties:
    return spec.BasicProperties(headers=headers if headers else None, **properties)


def deliver(ch, consumer_tag, delivery_tag, redelivered=False, exchange="", routing_key="", properties=None,
            body=b"{}") -> None:
    m = spec.Basic.Deliver(consumer_tag=consumer_tag, delivery_tag=delivery_tag, redelivered=redelivered,
                           exchange=exchange, routing_key=routing_key)
    ch._on_deliver(frame.Method(ch.channel_number, m), frame.Header(ch.channel_number, len(body),
                                                                    properties or spec.BasicProperties()), body)


def get_ok(ch, delivery_tag, redelivered=False, exchange="", routing_key="", properties=None, body=b"{}") -> None:
    m = spec.Basic.GetOk(delivery_tag=delivery_tag, redelivered=redelivered, exchange=exchange,
                         routing_key=routing_key, message_count=0)
    ch._on_getok(frame.Method(ch.channel_number, m), frame.Header(ch.channel_number, len(body),
                                                                  properties or spec.BasicProperties()), body)


def get_empty(ch) -> None:
    reply(ch, spec.Basic.GetEmpty())


def returned(ch, reply_code, reply_text, exchange, routing_key, properties=None, body=b"{}") -> None:
    m = spec.Basic.Return(reply_code=reply_code, reply_text=reply_text, exchange=exchange, routing_key=routing_key)
    ch._on_return(frame.Method(ch.channel_number, m), frame.Header(ch.channel_number, len(body),
                                                                   properties or spec.BasicProperties()), body)


def confirm(ch, ack: bool, delivery_tag: int, multiple: bool = False) -> None:
    reply(ch, (spec.Basic.Ack if ack else spec.Basic.Nack)(delivery_tag=delivery_tag, multiple=multiple))


def broker_closes_channel(ch, reply_code, reply_text) -> None:
    reply(ch, spec.Channel.Close(reply_code=reply_code, reply_text=reply_text, class_id=0, method_id=0))


def broker_cancels(ch, consumer_tag) -> None:
    reply(ch, spec.Basic.Cancel(consumer_tag=consumer_tag))


def connection_lost(ch, error=None) -> None:
    ch._on_close_meta(error or pika.exceptions.StreamLostError("connection reset"))


def client_closes_channel(ch) -> None:
    ch.close()
    reply(ch, spec.Channel.CloseOk())
