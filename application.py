#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2021-2022 asrelo
# All rights reserved.
#
# This file is part of "hasty-ruby-coypu".

"""hasty-ruby-coypu

Application
"""

import argparse
import importlib
import math
import pathlib
import random
import sqlite3
import sys
import types
import uuid
from collections.abc import Callable
from typing import Final, Optional, Union

import commons

def sqlite3_blob_encode_str(s: str) -> bytes:  #pylint: disable=invalid-name
    return s.encode('utf-8')

def sqlite3_blob_decode_str(b: bytes) -> str:  #pylint: disable=invalid-name
    return b.decode('utf-8')

def sqlite3_blob_encode_int(i: int) -> bytes:  #pylint: disable=invalid-name
    result: bytes
    try:
        result = i.to_bytes(4, 'little', signed=False)
    except OverflowError as err:
        raise AssertionError() from err
    return result

def sqlite3_blob_decode_int(b: bytes) -> int:  #pylint: disable=invalid-name
    assert len(b) == 4
    return int.from_bytes(b, 'little', signed=False)

def sqlite3_blob_encode_uuid(u: uuid.UUID) -> bytes:  #pylint: disable=invalid-name
    return u.bytes

def sqlite3_blob_decode_uuid(b: bytes) -> uuid.UUID:  #pylint: disable=invalid-name
    return uuid.UUID(bytes=b)

SQLITE_BLOB_CODERS: Final[dict[str, tuple[Callable, Callable]]] = {
    'str': (sqlite3_blob_encode_str, sqlite3_blob_decode_str),
    'int': (sqlite3_blob_encode_int, sqlite3_blob_decode_int),
    'uuid': (sqlite3_blob_encode_uuid, sqlite3_blob_decode_uuid),
}

OUTPUT_DB_EXTRA_SPEC: Final[dict[str, tuple[Optional[str], str]]] = {
    'db_id': (None, 'str'),
    'db_v': (None, 'int'),
    'latest_output_timestamp': (None, 'int'),
}

def output_db_extra_get(api_obj: Union[sqlite3.Connection, sqlite3.Cursor], internal_id: str
    ) -> Optional[bytes]:
    # may raise 'sqlite3' exceptions
    stored_name = OUTPUT_DB_EXTRA_SPEC[internal_id][0]
    if stored_name is None:
        stored_name = internal_id
    row: sqlite3.Row
    # no changes, no commit
    with commons.get_cursor_from_api_obj_custom(api_obj) as db_cur:
        db_cur.execute('SELECT "value" FROM "extra" WHERE "name"=?', (stored_name,))
        # 'rowcount' just does not work
        # do not load the whole table (which can be large) in case of failure
        rows = db_cur.fetchmany(size=2)
        assert len(rows) <= 1
        if len(rows) == 0:
            return None
        row = rows[0]
    assert len(row) == 1
    return row[0]

def output_db_extra_put(api_obj: Union[sqlite3.Connection, sqlite3.Cursor], internal_id: str,
                        val: Optional[bytes]) -> None:
    # may raise 'sqlite3' exceptions
    stored_name = OUTPUT_DB_EXTRA_SPEC[internal_id][0]
    if stored_name is None:
        stored_name = internal_id
    query_prep: tuple[str, tuple]
    if val is not None:
        query_prep = (
            'INSERT OR REPLACE INTO "extra" ("name", "value") VALUES (?, ?)', (internal_id, val))
    else:
        query_prep = ('DELETE FROM "extra" WHERE "name"=?', (internal_id,))
    with commons.get_cursor_from_api_obj_custom(api_obj) as db_cur:
        db_cur.execute(query_prep[0], query_prep[1])
    # do not do anything after commit; if an error occures then changes are not stored
    return

OUTPUT_DB_ID_STR: Final[str] = 'hasty-ruby-coypu'
OUTPUT_DB_V_INT: Final[int] = 1
UUID_CATEGORY_BASE: Final = uuid.uuid3(commons.UUID_BASE_NAMESPACE, 'category')
UUID_ENTRY_BASE: Final = uuid.uuid3(commons.UUID_BASE_NAMESPACE, 'entry')
DEFAULT_CATEGORY_UUID: Final = uuid.uuid3(UUID_CATEGORY_BASE, 'default')
DEFAULT_CATEGORY_TEXT_ID: Final[str] = 'default'
DEFAULT_CATEGORY_DISPLAYNAME: Final[str] = 'Default'

# see <https://www.sqlite.org/faq.html> for quirks on index columns
# use of 'rowid' is intended; 'collection_id' and 'entry_id' ARE NOT row IDs!
DB_CREATION_SEQUENCE_SQL: Final[list[Union[str, tuple[str, tuple], tuple[str, dict]]]] = [
    'PRAGMA foreign_keys = 1',
    'PRAGMA secure_delete = 1',
    ('CREATE TABLE "extra" ('
     '"name" TEXT NOT NULL UNIQUE,'
     '"value" BLOB'
     ')'),
    ('CREATE TABLE "collections" ('
     '"collection_uuid" BLOB UNIQUE,'
     '"collection_text_id" TEXT,'
     '"collection_displayname" TEXT'
     ')'),
    ('CREATE TABLE "entries" ('
     '"entry_uuid" BLOB UNIQUE,'
     '"collection_uuid" BLOB,'
     '"full_url" TEXT NOT NULL,'
     '"title" TEXT,'
     'FOREIGN KEY ("collection_uuid")'
     'REFERENCES "collections" ("collection_uuid")'
     ')'),
]

def verify_output_database(api_obj: Union[sqlite3.Connection, sqlite3.Cursor]) -> None:
    db_id: Optional[str] = SQLITE_BLOB_CODERS['str'][1](output_db_extra_get(api_obj, 'db_id'))
    assert db_id == OUTPUT_DB_ID_STR
    db_v: int = SQLITE_BLOB_CODERS['int'][1](output_db_extra_get(api_obj, 'db_v'))
    assert db_v == OUTPUT_DB_V_INT
    return

def build_output_database(api_obj: Union[sqlite3.Connection, sqlite3.Cursor]) -> None:
    with commons.get_cursor_from_api_obj_custom(api_obj) as db_cur:
        for query_data in DB_CREATION_SEQUENCE_SQL:
            if isinstance(query_data, tuple):
                db_cur.execute(query_data[0], query_data[1])
            else:
                db_cur.execute(query_data)
            db_cur.fetchall()
    output_db_extra_put(api_obj, 'db_id', SQLITE_BLOB_CODERS['str'][0](OUTPUT_DB_ID_STR))
    output_db_extra_put(api_obj, 'db_v', SQLITE_BLOB_CODERS['int'][0](OUTPUT_DB_V_INT))
    return

def store_output_data(output_data: commons.LoadedData, output_db_filepath: str) -> None:
    resolved_filepath: pathlib.Path = commons.path_resolve_custom(output_db_filepath)
    db_created: bool = not resolved_filepath.is_file()
    _LOCAL_TO_EXTERNAL_STAGES: Final[dict[str, str]] = {  #pylint: disable=invalid-name
        'db_connecting': 'output_db_connecting',
        'db_verifying': 'output_db_verifying',
        'db_building': 'output_db_building',
        'db_filling': 'output_db_filling',
        'db_disconnecting': 'output_db_disconnecting',
    }
    try:
        with commons.DetainSigint():
            with commons.ExceptionOccuredWhile('db_connecting'):
                if db_created:
                    # we cannot create a db file uniformly with URL
                    with sqlite3.connect(str(resolved_filepath), timeout=5.):
                        pass
                db_con = sqlite3.connect('file:{}?mode={}'.format(str(resolved_filepath), 'rw'),
                                         timeout=5., uri=True)
        if not db_created:
            with commons.DetainSigint(), commons.ExceptionOccuredWhile(
                'db_verifying'), commons.Sqlite3TransactionCustom(db_con, 'db_verifying'):
                verify_output_database(db_con)
        else:
            with commons.DetainSigint(), commons.ExceptionOccuredWhile(
                'db_building'), commons.Sqlite3TransactionCustom(db_con, 'db_building'):
                build_output_database(db_con)
        with commons.ExceptionOccuredWhile('db_filling'), commons.Sqlite3TransactionCustom(
            db_con, 'writing_new_data'):
            with commons.DetainSigint():
                output_db_extra_put(db_con, 'latest_output_timestamp',
                    SQLITE_BLOB_CODERS['int'][0](
                        math.floor(output_data.output_timestamp.timestamp())))
            with db_con.cursor(factory=commons.Sqlite3CursorCustom) as db_cur:
                with commons.DetainSigint():
                    db_cur.execute('INSERT OR IGNORE INTO "collections" VALUES (?, ?, ?)',
                        (SQLITE_BLOB_CODERS['uuid'][0](DEFAULT_CATEGORY_UUID),
                         DEFAULT_CATEGORY_TEXT_ID, DEFAULT_CATEGORY_DISPLAYNAME))
                with commons.ProcessSigintAsInterruption(
                    "Data are being written to the output DB."):
                    for entry in output_data.entries:
                        with commons.DetainSigint():
                            db_cur.execute(
                                ('SELECT EXISTS(SELECT 1 FROM "entries"'
                                 'WHERE ("full_url"=? AND "title"=?))'), (entry[0], entry[1]))
                            if db_cur.fetchone()[0] == 0:
                                # not a secret token
                                entry_uuid = uuid.uuid3(
                                    UUID_ENTRY_BASE, random.randbytes(16).hex())
                                db_cur.execute('INSERT INTO "entries" VALUES (?, ?, ?, ?)',
                                    (SQLITE_BLOB_CODERS['uuid'][0](entry_uuid),
                                     SQLITE_BLOB_CODERS['uuid'][0](DEFAULT_CATEGORY_UUID),
                                     entry[0], entry[1]))
        with commons.DetainSigint(), commons.ExceptionOccuredWhile('db_disconnecting'):
            db_con.commit()
            db_con.close()
    except sqlite3.OperationalError as err:
        temp_exc = commons.DatabaseAccessError(
            ("Could not store data to output DB: operational error occured; failed stage: '{}'"
            ).format(err.occured_while))
        temp_exc.failed_stage = _LOCAL_TO_EXTERNAL_STAGES[err.occured_while]  #pylint: disable=attribute-defined-outside-init
        raise temp_exc from err
    except sqlite3.DatabaseError as err:
        temp_exc = commons.DatabaseAccessError(
            ("Could not store data to output DB: database error occured; failed stage: '{}'"
            ).format(err.occured_while))
        temp_exc.failed_stage = _LOCAL_TO_EXTERNAL_STAGES[err.occured_while]  #pylint: disable=attribute-defined-outside-init
        raise temp_exc from err
    finally:
        db_con.close()  # repeated closing is tolerated in fact
    return

def import_needed_loader(conv_type: str) -> types.ModuleType:
    _CONV_TYPE_TO_LOADERS_MODULE_NAME: Final[dict[str, str]] = {  #pylint: disable=invalid-name
        'tosqldb': 'tabs_outliner_db',
        'gcbf': 'chrome_bookmarks'
    }
    module_path: str = 'loaders.{}'.format(_CONV_TYPE_TO_LOADERS_MODULE_NAME[conv_type])
    loader = importlib.import_module(module_path)
    return loader

def main(start_args: argparse.Namespace, prog_name: Optional[str] = None) -> commons.ExitCode:
    #pylint: disable=too-many-return-statements
    def _handle_unexpected_input_error(err: commons.UnexpectedInputError) -> None:
        print("Unexpected input supplied; job interrupted.\n{}".format(str(err)),
              file=sys.stderr)
        if start_args.verbose_errors:
            commons.print_exception_traceback(err)
    assert isinstance(start_args, argparse.Namespace)
    assert (prog_name is None) or isinstance(prog_name, str)
    try:
        # data loading, validating and converting
        loader: type.ModuleType = import_needed_loader(start_args.conv_type)
        try:
            data = loader.load_input_data(start_args.input_db)
        except commons.FileAccessError as err:
            print("Could not access the input file; job interrupted.\n{}".format(str(err)),
                  file=sys.stderr)
            return commons.ExitCode.EFILEACC
        except commons.DatabaseAccessError as err:
            print(("Unspecified DB error occured on data loading; job interrupted.\n{}"
                ).format(str(err)), file=sys.stderr)
            if start_args.verbose_errors:
                commons.print_exception_traceback(err)
            return commons.ExitCode.EDBUNSPEC
        except commons.UnexpectedInputError as err:
            _handle_unexpected_input_error(err)
            return commons.ExitCode.EBADINPUT
        # data storing
        try:
            store_output_data(data, start_args.output_db)
        except commons.FileAccessError as err:
            print("Could not access the output file; job interrupted.\n{}".format(str(err)),
                  file=sys.stderr)
            return commons.ExitCode.EFILEACC
        except commons.DatabaseAccessError as err:
            print(("Unspecified DB error occured on data storing, failed stage: '{}';"
                   " job interrupted.\n{}").format(err.failed_stage, str(err)), file=sys.stderr)
            if start_args.verbose_errors:
                commons.print_exception_traceback(err)
            if err.failed_stage == 'output_db_building':
                print(("ATTENTION! An error occured during building a new DB; it IS CURRENTLY"
                       " CORRUPTED, but does not contain any application data. YOU SHOULD delete"
                       " the database file and all its journals BEFORE trying again."),
                      file=sys.stderr)
            else:
                print(
                    "Attention! The DB should not be corrupted by now, but check it if you can.",
                    file=sys.stderr)
            return commons.ExitCode.EDBUNSPEC
        # end
    except KeyboardInterrupt as err:
        print("The program was terminated with 'SIGINT'.", file=sys.stderr)
        return commons.ExitCode.EUSRINTER
    return commons.ExitCode.SUCCESS
