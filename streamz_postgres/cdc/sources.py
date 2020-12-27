from enum import Enum, auto
from threading import Thread
from typing import Dict, List

import psycopg2
from psycopg2.extras import (
    LogicalReplicationConnection,
    StopReplication,
    REPLICATION_LOGICAL,
    ReplicationCursor,
)
from streamz import Source

from streamz_postgres.pg.replication_protocol import Buffer


class Format(Enum):
    RAW = 0
    PROTO = 1
    DICT = 2

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value

    def __gte__(self, other):
        return self.value >= other.value

    def __lte__(self, other):
        return self.value <= other.value


class Consumer:
    def __call__(self, msg):
        print("CONSUME", msg.data_size)


class from_postgres_cdc(Source):
    def __init__(
        self,
        client_params: Dict,
        slot_name: str,
        publications: List[str],
        format=Format.DICT,
        default=str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._client_params = client_params
        self._slot_name = slot_name
        self._publications = ",".join(publications)
        self._format = format
        self._default = default
        self._data_types = {}

    def start(self):
        thread = Thread(target=self._consume)
        self.stopped = False
        thread.start()

    def _consume(self):
        """This is run in a separate thread"""

        def cb(msg):
            print("CONSUME", msg.data_size)
            if self.stopped:
                raise StopReplication
            if self._format > Format.RAW:
                msg = self._parse(msg.payload)
            if self._format > Format.PROTO:
                msg = self._convert(msg)
            self.emit(msg)

        connection = psycopg2.connect(
            **self._client_params,
            connection_factory=LogicalReplicationConnection,
        )
        cursor = connection.cursor()
        cursor.create_replication_slot(
            slot_name=self._slot_name, output_plugin="pgoutput"
        )
        cursor.start_replication(
            slot_name=self._slot_name,
            options=dict(publication_names=self._publications, proto_version=1),
        )
        cursor.consume_stream(cb)

    @staticmethod
    def _parse(b: bytes):
        """Parse raw bytes to replication protocol message"""
        return Buffer(b).parse()

    @staticmethod
    def _convert(proto_message):
        """Convert replication protocol message into a dict"""
        raise NotImplementedError
