#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2021-2022 asrelo
# All rights reserved.
#
# This file is part of "hasty-ruby-coypu".

"""hasty-ruby-coypu

Data loader for Tabs Outliner DB
"""

import datetime
import functools
import itertools
import json
import math
import pathlib
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from typing import Any, NoReturn, Optional

import commons

def _check_input_fields_types(
    assorted_row: Mapping[str, Any], fields_types: Mapping[str, type]) -> None:
    # raises on check failure
    t = []
    for field_name in fields_types.keys():  #pylint: disable=consider-iterating-dictionary,consider-using-dict-items
        if type(assorted_row[field_name]) is not fields_types[field_name]:  # pylint: disable=unidiomatic-typecheck
            t.append(
                ("Input DB's field \"current_session_snapshot.{}\" was recognized as type '{}',"
                 " expected '{}'"
                ).format(field_name, type(assorted_row[field_name]).__name__,
                         fields_types[field_name].__name__))
    if len(t) > 0:
        raise commons.UnexpectedInputError("; ".join(t))
    return

def _parse_input_json(serialized_json):
    def _json_parse_constant_custom(val) -> NoReturn:
        raise commons.UnexpectedInputError(
            "Encountered weird constant in input JSON: \"{}\"".format(str(val)))
    try:
        parsed_json = json.loads(
            serialized_json, object_hook=dict, parse_constant=_json_parse_constant_custom)
    except json.JSONDecodeError as err:
        raise commons.UnexpectedInputError(
            "Input JSON is corrupted (failed at index {})".format(err.pos)) from err
    return parsed_json

def _load_from_file(input_db_filepath: str):
    try:
        resolved_filepath: pathlib.Path = commons.path_resolve_custom(input_db_filepath)
    except commons.FileAccessError as err:
        raise commons.FileAccessError(
            "Cannot locate input DB path: {}".format(str(err))) from err
    if not resolved_filepath.is_file():
        raise commons.FileAccessError(
            "Input DB file not found: \"{}\"".format(str(resolved_filepath)))
    row: sqlite3.Row
    try:
        # reading only, no changes or commits
        with commons.DetainSigint(), sqlite3.connect(
            'file:{}?mode={}'.format(str(resolved_filepath), 'ro'), timeout=5., uri=True
            ) as db_con:
            db_con.row_factory = sqlite3.Row
            with db_con.cursor(factory=commons.Sqlite3CursorCustom) as db_cur:
                db_cur.execute('SELECT * FROM "current_session_snapshot"')
                # 'rowcount' just does not work
                # do not load the whole table (which can be large) in case of failure
                fetched_rows = db_cur.fetchmany(size=2)
                if len(fetched_rows) != 1:
                    raise commons.UnexpectedInputError(
                        ("Input DB's table \"current_session_snapshot\" contains {} rows, known"
                         " to contain exactly 1 row").format(db_cur.rowcount))
                row = fetched_rows[0]
    except sqlite3.OperationalError as err:
        raise commons.DatabaseAccessError(
            "Could not load data from input DB: operational error occured") from err
    except sqlite3.DatabaseError as err:
        raise commons.DatabaseAccessError(
            "Could not load data from input DB: database error occured") from err
    if len(row) != 4:
        raise commons.UnexpectedInputError(
            ("Input DB's table \"current_session_snapshot\" contains {} fields, expected 4"
             " fields").format(len(row)))
    FIELD_TYPES: dict[str, type] = {  #pylint: disable=invalid-name
        'id': float,
        'timestamp': float,
        'op_array_len': float,
        'data': str,
    }
    assorted_row = dict(zip(FIELD_TYPES.keys(), row))
    _check_input_fields_types(assorted_row, FIELD_TYPES)  # raises on check failure
    parsed_data: dict = {}
    try:
        parsed_data['id'] = float(assorted_row['id'])
    except ValueError as err:
        raise commons.UnexpectedInputError(
            ("Failed to convert input DB's field \"current_session_snapshot.id\" to float, its"
             " value is \"{}\"").format(str(assorted_row['id']))) from err
    try:
        # getting aware object
        parsed_data['timestamp']: datetime.datetime = datetime.datetime.fromtimestamp(
            float(assorted_row['timestamp'])/1000., datetime.timezone.utc)
    except ValueError as err:
        raise commons.UnexpectedInputError(
            ("Failed to convert input DB's field \"current_session_snapshot.timestamp\" to"
             " timestamp, its value is \"{}\"").format(str(assorted_row['timestamp']))) from err
    try:
        parsed_data['op_array_len']: float = float(assorted_row['op_array_len'])
    except ValueError as err:
        raise commons.UnexpectedInputError(
            ("Failed to convert input DB's field \"current_session_snapshot.op_array_len\" to"
             " float, its value is \"{}\"").format(str(assorted_row['op_array_len']))) from err
    try:
        parsed_data['data'] = _parse_input_json(assorted_row['data'])
        # returned data should be valid but not format-checked yet
    except commons.UnexpectedInputError as err:
        raise commons.UnexpectedInputError(
            ("Failed to convert input DB's field \"current_session_snapshot.data\" to a"
            " structured object")) from err
    return parsed_data

def _precheck_parsed_input_data(data) -> None:
    # raises 'commons.UnexpectedInputError' on check failure
    try:
        # not using 'assert's because their messages would be evaled every time
        # yes, check exact equivalence (this float is represented exactly)
        if not data['id'] == 1.0:
            raise commons.UnexpectedInputError(
                ("Only \"current_session_snapshot.id\" == 1.0 is known, {} encountered"
                ).format(str(data['id'])))
        if not data['timestamp'] >= datetime.datetime.fromtimestamp(0, datetime.timezone.utc):
            raise commons.UnexpectedInputError(
                ("Weird \"current_session_snapshot.timestamp\" encountered: {}"
                ).format(str(data['timestamp'])))
        if not (math.isclose(data['op_array_len'], round(data['op_array_len']))
                and round(data['op_array_len']) >= 0):
            raise commons.UnexpectedInputError(
                ("\"current_session_snapshot.op_array_len\" is expected to be non-negative"
                 " integer, encountered: {}").format(str(data['op_array_len'])))
        if not isinstance(data['data'], Sequence):
            raise commons.UnexpectedInputError(
                ("JSON root element is expected to be an array ('Sequence'), \"{}\" encountered"
                ).format(type(data['data']).__name__))
        if not data['op_array_len'] == len(data['data']):
            raise commons.UnexpectedInputError(
                ("JSON root array is expected to contain exactly"
                 " \"current_session_snapshot.op_array_len\"=={} elements total, {} found"
                ).format(round(data['op_array_len']), len(data['data'])))
        dada = data['data']
        try:
            assert isinstance(dada[0], Mapping)
            assert len(set(dada[0].keys()) ^ set(('type', 'node'))) == 0
            assert dada[0]['type'] == 2000
            assert isinstance(dada[0]['node'], Mapping)
            assert len(set(dada[0]['node'].keys()) ^ set(('type', 'data'))) == 0
            assert dada[0]['node']['type'] == 'session'
            assert isinstance(dada[0]['node']['data'], Mapping)
            assert len(set(dada[0]['node']['data'].keys())
                       ^ set(('treeId', 'nextDId', 'nonDumpedDId'))) == 0
            assert commons.silence_exc(
                functools.partial(float, dada[0]['node']['data']['treeId']),
                (TypeError, ValueError))[1] is None
            assert float(dada[0]['node']['data']['treeId']) >= 0.
            assert dada[0]['node']['data']['nextDId'] == 1
            assert dada[0]['node']['data']['nonDumpedDId'] == 1
        except AssertionError as err:
            raise commons.UnexpectedInputError(
                "JSON special first element has unexpected format") from err
        try:
            assert isinstance(dada[-1], Mapping)
            assert len(set(dada[-1].keys()) ^ set(('type', 'time'))) == 0
            assert dada[-1]['type'] == 11111
        except AssertionError as err:
            raise commons.UnexpectedInputError(
                "JSON special last element has unexpected format") from err
        if not (((data['data'][-1]['time']/1000.) < data['timestamp'].timestamp())
                and (abs(data['timestamp'].timestamp() - (data['data'][-1]['time']/1000.))
                    < 10.)):
            raise commons.UnexpectedInputError(
                ("Timestamps on JSON and DB have weird relation; JSON's is {} and DB's is {}"
                ).format(str(data['data'][-1]['time']/1000.),
                         str(data['timestamp'].timestamp())))
        # the rest of elements have too complex structure and are to be checked during conversion
    except (AssertionError, commons.UnexpectedInputError) as err:
        raise commons.UnexpectedInputError(
            "Input data has unexpected format: {}".format(str(err))) from err
    return

def _check_input_entry_contents(entry_contents: Mapping) -> None:
    # raises 'commons.UnexpectedInputError' on check failure
    # sorry
    #pylint: disable=too-many-branches,too-many-statements
    def _check_marks_subobj(subobj: Any) -> None:
        # raises 'commons.UnexpectedInputError' on check failure
        if not isinstance(subobj, Mapping):
            raise commons.UnexpectedInputError(
                "JSON entry subobject \"marks\" has unexpected type: \"{}\"".format(
                    type(subobj).__name__))
        try:
            assert isinstance(entry_contents['marks'], Mapping)
            assert len(set(entry_contents['marks'].keys())
                       - set(('relicons', 'customFavicon', 'customTitle'))) == 0
            assert 'relicons' in entry_contents['marks']
            assert isinstance(entry_contents['marks']['relicons'], Sequence)
            assert len(entry_contents['marks']['relicons']) == 0
            if 'customFavicon' in entry_contents['marks']:
                assert isinstance(entry_contents['marks']['customFavicon'], str)
            if 'customTitle' in entry_contents['marks']:
                assert isinstance(entry_contents['marks']['customTitle'], str)
        except (AssertionError, commons.UnexpectedInputError) as err:
            raise commons.UnexpectedInputError(
                "JSON entry subobject \"marks\" has unknown format") from err
        return
    def _check_colapsed_subobj(subobj: Any) -> None:
        # raises 'commons.UnexpectedInputError' on check failure
        if not isinstance(subobj, bool):
            raise commons.UnexpectedInputError(
                "JSON entry subobject \"colapsed\" has unexpected type: \"{}\"".format(
                    type(subobj).__name__))
        return
    def _check_data_subobj(subobj: Any, typ: Optional[str]) -> None:
        # raises 'commons.UnexpectedInputError' on check failure
        #pylint: disable=consider-iterating-dictionary
        if not isinstance(subobj, Mapping):
            raise commons.UnexpectedInputError(
                "JSON entry subobject \"data\" has unexpected type: \"{}\"".format(
                    type(subobj).__name__))
        try:
            if (typ is None) or (typ=='tab'):
                # basic checks (presense and type)
                _required_keys: dict[str, Callable[[Any], bool]] = {
                    'audible':          lambda x: isinstance(x, bool),
                    'autoDiscardable':  lambda x: isinstance(x, bool) and x,
                    'discarded':        lambda x: isinstance(x, bool),
                    'mutedInfo':        lambda x: isinstance(x, Mapping),
                    'title':            lambda x: isinstance(x, str),
                    'url':              lambda x: isinstance(x, str),
                }
                _optional_keys: dict[str, Callable[[Any], bool]] = {
                    'active':       lambda x: isinstance(x, bool) and x,
                    'favIconUrl':   lambda x: isinstance(x, str),
                    'groupId':      lambda x: isinstance(x, int) and (x==-1),
                    'highlighted':  lambda x: isinstance(x, bool),
                    'openerTabId':  lambda x: isinstance(x, int) and (x>0),
                    'pendingUrl':   lambda x: isinstance(x, str),
                }
                if typ is None:
                    _optional_keys.update({
                        'id':                       lambda x: isinstance(x, int) and (x>0),
                        'wasSavedOnLastWinSave':    lambda x: isinstance(x, bool) and x,
                        'windowId': lambda x: isinstance(x, int) and (x>0),
                    })
                if typ == 'tab':
                    _required_keys.update({
                        'id':       lambda x: isinstance(x, int) and (x>0),
                    })
                    _optional_keys.update({
                        'status':   lambda x: isinstance(x, str),
                        'windowId': lambda x: isinstance(x, int) and (x>0),
                    })
                assert all(map(lambda k: k in subobj.keys(), _required_keys.keys()))
                assert all(map(
                    lambda k: k in dict(_required_keys, **_optional_keys).keys(), subobj.keys()))
                assert all(map(
                    lambda i: dict(_required_keys, **_optional_keys)[i[0]](i[1]),
                    subobj.items()))
                # advanced checks
                if 'status' in subobj:
                    assert subobj['status'] in ('loading', 'unloaded')
                def _assert_muted_info(obj) -> None:
                    assert ('muted' in obj) and isinstance(obj['muted'], bool)
                    assert all(map(lambda x: x in ['muted', 'reason'], obj.keys()))
                    if obj['muted']:
                        assert ('reason' in obj) and isinstance(obj['reason'], str)
                        assert obj['reason'] == 'user'
                    return
                _assert_muted_info(subobj['mutedInfo'])
            elif typ in ('savedwin', 'win'):
                # basic checks (presense and type)
                _required_keys: dict[str, Callable[[Any], bool]] = {
                    'type': lambda x: isinstance(x, str),
                    'rect': lambda x: isinstance(x, str),
                }
                _optional_keys: dict[str, Callable[[Any], bool]] = {
                    'id':   lambda x: isinstance(x, int) and (x>0),
                    'focused':  lambda x: isinstance(x, bool) and x,
                }
                if typ == 'savedwin':
                    _optional_keys.update({
                        'crashDetectedDate':    lambda x: isinstance(x, int) and (x>=0),
                    })
                if typ == 'win':
                    _optional_keys.update({
                        'state':    lambda x: isinstance(x, str),
                    })
                assert all(map(lambda k: k in subobj.keys(), _required_keys.keys()))
                assert all(map(
                    lambda k: k in dict(_required_keys, **_optional_keys).keys(), subobj.keys()))
                assert all(map(
                    lambda i: dict(_required_keys, **_optional_keys)[i[0]](i[1]),
                    subobj.items()))
                # advanced checks
                assert (subobj['type'] in ('normal', 'popup'))
                if 'state' in subobj:
                    assert subobj['state'] in ('maximized',)
                def _parse_rect_record(x: str) -> list[int]:  #pylint: disable=invalid-name
                    t = x.split('_', maxsplit=(4+1))
                    assert len(t) == 4
                    try:
                        t = [int(ta) for ta in t]
                    except ValueError as err:
                        raise AssertionError() from err
                    return t
                rect_record: list[int] = _parse_rect_record(subobj['rect'])
                assert (rect_record[0] <= rect_record[2]) and (rect_record[1] <= rect_record[3])
            elif typ == 'group':
                assert ('rect' in subobj)
                assert isinstance(subobj['rect'], str)
                rect_record: list[str] = subobj['rect'].split('_', maxsplit=5)
                assert len(rect_record) == 4
                assert all(map(lambda x: x == 'undefined', rect_record))
            else:
                assert False
        except (AssertionError, commons.UnexpectedInputError) as err:
            print(subobj)
            print(typ)
            raise commons.UnexpectedInputError(
                "JSON entry subobject \"data\" has unknown format") from err
        return
    if 'type' not in entry_contents:
        # common saved tabs?
        pass
    elif entry_contents['type'] == 'savedwin':
        # ever opened groups and windows?
        if 'marks' in entry_contents:
            _check_marks_subobj(entry_contents['marks'])
    elif entry_contents['type'] == 'group':
        # groups that have never had open tabs?
        if 'marks' in entry_contents:
            _check_marks_subobj(entry_contents['marks'])
        if 'colapsed' in entry_contents:
            _check_colapsed_subobj(entry_contents['colapsed'])
    elif entry_contents['type'] == 'tab':
        # tabs in windows?
        if 'marks' in entry_contents:
            _check_marks_subobj(entry_contents['marks'])
    elif entry_contents['type'] == 'win':
        # currently open window?
        if 'marks' in entry_contents:
            _check_marks_subobj(entry_contents['marks'])
        if 'colapsed' in entry_contents:
            _check_colapsed_subobj(entry_contents['colapsed'])
    else:
        t = entry_contents['type']
        raise commons.UnexpectedInputError(
            "JSON entry contents has unknown type: \"{}\"".format(
                t if isinstance(t, str) else repr(t)))
    if 'data' not in entry_contents:
        raise commons.UnexpectedInputError(
            "JSON entry contents is expected to have \"data\" subobject, nothing found")
    _check_data_subobj(
        entry_contents['data'], entry_contents['type'] if 'type' in entry_contents else None)
    return

def _convert_internal_data(input_data: dict) -> commons.LoadedData:
    output_data: commons.LoadedData = commons.LoadedData(
        datetime.datetime.now(tz=datetime.timezone.utc), set())
    # The first and the last special objects should already be checked, and it does not contain
    # useful info.
    for input_entry_i, input_entry in zip(itertools.count(1), input_data['data'][1:-1]):
        try:
            assert len(input_entry) == 3
            assert input_entry[0] == 2001
            assert isinstance(input_entry[1], Mapping)
            assert isinstance(input_entry[2], Sequence)
            assert all(map(lambda x: isinstance(x, int) and x>=0, input_entry[2]))
            _check_input_entry_contents(input_entry[1])  # raises on failure
        except (AssertionError, commons.UnexpectedInputError) as err:
            raise commons.UnexpectedInputError(
                ("Input data has unexpected format (stopped at first failed entry {}): {}"
                ).format(str(input_entry_i), str(err))) from err
        if ('type' not in input_entry[1]) or (input_entry[1]['type'] == 'tab'):
            output_data.entries.add(
                (input_entry[1]['data']['url'], input_entry[1]['data']['title']))
    return output_data

def load_input_data(input_db_filepath: str) -> commons.LoadedData:
    # passing all exceptions up
    parsed_input_data = _load_from_file(input_db_filepath)
    _precheck_parsed_input_data(parsed_input_data)  # raises on failure
    loaded_data = _convert_internal_data(parsed_input_data)
    return loaded_data
