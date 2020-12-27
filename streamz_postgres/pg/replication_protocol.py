import struct
from io import BytesIO
from typing import NamedTuple, List


class TupleData(NamedTuple):
    num_columns: int
    data: List[str]


class Begin(NamedTuple):
    final_lsn: int
    commit_timestamp: int
    xid: int


class Commit(NamedTuple):
    flags: int
    commit_lsn: int
    end_lsn: int
    commit_timestamp: int


class Origin(NamedTuple):
    commit_lsn: int
    origin_name: str


class Column(NamedTuple):
    flags: int
    is_pk: bool
    name: str
    data_type: int
    data_type_modifier: int


class Relation(NamedTuple):
    id: int
    namespace: str
    name: str
    replica_identity: int
    num_columns: int
    columns: List[Column]


class Type(NamedTuple):
    id: int
    namespace: str
    name: str


class Insert(NamedTuple):
    relation_id: int
    new_tuple: TupleData


class Update(NamedTuple):
    relation_id: int
    key_tuple: TupleData
    new_tuple: TupleData
    old_tuple: TupleData


class Delete(NamedTuple):
    relation_id: int
    key_tuple: TupleData
    old_tuple: TupleData


class Truncate(NamedTuple):
    num_relations: int
    flags: int
    cascade: bool
    restart_identity: bool
    relation_ids: List[int]


class Buffer:
    """Common buffer data types.

    Reference:
        https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

    Special thanks: https://github.com/kyleconroy/pgoutput/blob/master/parse.go
    """

    def __init__(self, b: bytes):
        self.b = BytesIO(b)
        self.type = self.Byte1()

    def Byte1(self) -> str:
        return self.b.read(1).decode()

    def Int8(self) -> int:
        return struct.unpack(">B", self.b.read(1))[0]

    def Int16(self) -> int:
        return struct.unpack(">H", self.b.read(2))[0]

    def Int32(self) -> int:
        return struct.unpack(">I", self.b.read(4))[0]

    def Int64(self) -> int:
        return struct.unpack(">Q", self.b.read(8))[0]

    def String(self, size=None) -> str:
        if size is not None:
            return struct.unpack(f">{size}s", self.b.read(size))[0].decode()
        buf = b""
        char = self.b.read(1)
        while ord(char) != 0:
            buf += char
            char = self.b.read(1)
        return struct.unpack(f"{len(buf)}s", buf)[0].decode()

    def TupleData(self) -> list:
        num_items = self.Int16()
        data = self._TupleData_parse_data(num_items)
        return TupleData(num_items, data)

    def _TupleData_parse_data(self, num_items) -> list:
        return [self._TupleData_parse_next() for _ in range(num_items)]

    def _TupleData_parse_next(self):
        kind = self.Byte1()
        if kind in ("n", "u"):  # TODO: handle "u" (TOASTed field) appropriately (how?)
            return None
        if kind == "t":
            size = self.Int32()
            return self.String(size)
        raise ValueError(f"Unknown TupleData item type: {kind}")

    def _Column_parse_next(self):
        flags = self.Int8()
        return Column(
            flags=flags,
            is_pk=flags == 1,
            name=self.String(),
            data_type=self.Int32(),
            data_type_modifier=self.Int32(),
        )

    def _Column_parse_columns(self, num_columns):
        return [self._Column_parse_next() for _ in range(num_columns)]

    def _parse_Begin(self):
        return Begin(
            final_lsn=self.Int64(),
            commit_timestamp=self.Int64(),
            xid=self.Int32(),
        )

    def _parse_Commit(self):
        return Commit(
            flags=self.Int8(),
            commit_lsn=self.Int64(),
            end_lsn=self.Int64(),
            commit_timestamp=self.Int64(),
        )

    def _parse_Origin(self):
        return Origin(
            commit_lsn=self.Int64(),
            origin_name=self.String(),
        )

    def _parse_Relation(self):
        _id = self.Int32()
        namespace = self.String()
        name = self.String()
        replica_identity = self.Int8()
        num_columns = self.Int16()
        columns = self._Column_parse_columns(num_columns)
        return Relation(
            id=_id,
            namespace=namespace,
            name=name,
            replica_identity=replica_identity,
            num_columns=num_columns,
            columns=columns,
        )

    def _parse_Type(self):
        return Type(id=self.Int32(), namespace=self.String(), name=self.String())

    def _parse_Insert(self):
        relation_id = self.Int32()
        _ = self.Byte1()
        return Insert(relation_id=relation_id, new_tuple=self.TupleData())

    def _parse_Update(self):
        relation_id = self.Int32()
        td = self.Byte1()
        ko_data = None, None
        if td in ("K", "O"):
            ko_data = self.TupleData()
            _ = self.Byte1()  # always "N" for new tuple
        return Update(
            relation_id=relation_id,
            key_tuple=ko_data if td == "K" else None,
            old_tuple=ko_data if td == "O" else None,
            new_tuple=self.TupleData(),
        )

    def _parse_Delete(self):
        relation_id = self.Int32()
        ko = self.Byte1()
        ko_data = self.TupleData()
        return Delete(
            relation_id=relation_id,
            key_tuple=ko_data if ko == "K" else None,
            old_tuple=ko_data if ko == "O" else None,
        )

    def _parse_Truncate(self):
        num_relations = self.Int32()
        flags = self.Int8()
        cascade = flags & 1 == 1
        restart_identity = flags & 2 == 2
        return Truncate(
            num_relations=num_relations,
            flags=flags,
            cascade=cascade,
            restart_identity=restart_identity,
            relation_ids=[self.Int32() for _ in range(num_relations)],
        )

    _types = {
        "B": _parse_Begin,
        "C": _parse_Commit,
        "O": _parse_Origin,
        "R": _parse_Relation,
        "Y": _parse_Type,
        "I": _parse_Insert,
        "U": _parse_Update,
        "D": _parse_Delete,
        "T": _parse_Truncate,
    }

    def parse(self):
        return self._types[self.type](self)
