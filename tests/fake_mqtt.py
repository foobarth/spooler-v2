"""Minimal in-process MQTT 3.1.1 broker for tests — never a real broker.

Supports only what the bridge uses: CONNECT (incl. LWT will fields),
CONNACK, PUBLISH (QoS 0, retain flag recorded), PINGREQ/PINGRESP and
DISCONNECT. Anything else raises, so protocol drift shows up as a loud
test failure instead of a silent pass. The bridge never subscribes, so
there is no SUBSCRIBE support.

Tests assert against the recorded log (publishes / connects), never
against mqtt_bridge internals.
"""

import asyncio
from dataclasses import dataclass


@dataclass
class RecordedPublish:
    topic: str
    payload: bytes
    retain: bool = False


@dataclass
class RecordedConnect:
    client_id: str
    will_topic: str | None = None
    will_payload: bytes | None = None


class _ProtocolError(Exception):
    pass


async def _read_packet(reader: asyncio.StreamReader):
    """Returns (packet_type, flags, body). Raises on closed stream."""
    header = await reader.readexactly(1)
    ptype, flags = header[0] >> 4, header[0] & 0x0F
    length, shift = 0, 0
    while True:
        byte = (await reader.readexactly(1))[0]
        length |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
    body = await reader.readexactly(length) if length else b""
    return ptype, flags, body


def _parse_string(body: bytes, offset: int):
    n = int.from_bytes(body[offset:offset + 2], "big")
    return body[offset + 2:offset + 2 + n].decode("utf-8"), offset + 2 + n


def _parse_binary(body: bytes, offset: int):
    n = int.from_bytes(body[offset:offset + 2], "big")
    return body[offset + 2:offset + 2 + n], offset + 2 + n


class FakeBroker:
    def __init__(self):
        self.publishes: list[RecordedPublish] = []
        self.connects: list[RecordedConnect] = []
        self.port: int | None = None
        self._server: asyncio.AbstractServer | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._clean_disconnect = True
        self._will: RecordedConnect | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._writer:
            self._writer.close()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def drop_client(self) -> None:
        """Close the TCP connection without a DISCONNECT packet — the broker
        then synthesizes the client's LWT will publish, which is how tests
        assert the offline LWT fires."""
        self._clean_disconnect = False
        if self._writer:
            self._writer.close()
            self._writer = None

    async def _handle(self, reader, writer) -> None:
        self._writer = writer
        self._clean_disconnect = True
        try:
            while True:
                ptype, flags, body = await _read_packet(reader)
                if ptype == 1:      # CONNECT
                    will = self._parse_connect(flags, body)
                    self._will = will
                    self.connects.append(will)
                    writer.write(b"\x20\x02\x00\x00")   # CONNACK
                    await writer.drain()
                elif ptype == 3:    # PUBLISH
                    topic, offset = _parse_string(body, 0)
                    if flags & 0x06:                    # QoS 1/2 — not supported
                        raise _ProtocolError(f"QoS > 0 publish on {topic!r}")
                    payload, _ = _parse_binary(body, offset)
                    self.publishes.append(
                        RecordedPublish(topic, payload, retain=bool(flags & 0x01)))
                elif ptype == 12:   # PINGREQ
                    writer.write(b"\xd0\x00")           # PINGRESP
                    await writer.drain()
                elif ptype == 14:   # DISCONNECT
                    return
                else:
                    raise _ProtocolError(f"unsupported packet type {ptype}")
        except (asyncio.IncompleteReadError, ConnectionError, _ProtocolError) as e:
            # Abrupt close on a connection that declared a will → fire the LWT.
            if not self._clean_disconnect and self._will and self._will.will_topic:
                self.publishes.append(RecordedPublish(
                    self._will.will_topic, self._will.will_payload, retain=True))
        finally:
            if writer is self._writer:
                self._writer = None

    @staticmethod
    def _parse_connect(flags: int, body: bytes) -> RecordedConnect:
        _, offset = _parse_string(body, 0)          # protocol name ("MQTT")
        offset += 4                                 # level + connect flags + keepalive
        will_topic = will_payload = None
        if flags & 0x04:                            # will flag
            will_topic, offset = _parse_string(body, offset)
            will_payload, offset = _parse_binary(body, offset)
        client_id, _ = _parse_string(body, offset)
        return RecordedConnect(client_id, will_topic, will_payload)

    # ── assertion helpers ────────────────────────────────────────────────────

    def topics(self, prefix: str) -> list[RecordedPublish]:
        return [p for p in self.publishes if p.topic == prefix or p.topic.startswith(prefix + "/")]

    def payloads(self, topic: str) -> list[bytes]:
        return [p.payload for p in self.publishes if p.topic == topic]

    async def wait_for_publish(self, topic: str, timeout: float = 2.0) -> RecordedPublish:
        async def _poll():
            while True:
                for p in self.publishes:
                    if p.topic == topic:
                        return p
                await asyncio.sleep(0.01)
        return await asyncio.wait_for(_poll(), timeout)

    async def wait_for_connect(self, timeout: float = 2.0) -> RecordedConnect:
        async def _poll():
            while not self.connects:
                await asyncio.sleep(0.01)
            return self.connects[0]
        return await asyncio.wait_for(_poll(), timeout)
