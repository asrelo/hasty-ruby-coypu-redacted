#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2022 asrelo
# All rights reserved.
#
# This file is part of "hasty-ruby-coypu".

"""hasty-ruby-coypu

Data loader for Google Chrome bookmarks
"""

import datetime
import itertools
import json
import pathlib
import re
import string
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final, NoReturn

import commons

def _load_input_json(file_handle):
    def _json_parse_constant_custom(val) -> NoReturn:
        raise commons.UnexpectedInputError(
            "Encountered weird constant in input JSON: \"{}\"".format(str(val)))
    try:
        parsed_json = json.load(
            file_handle, object_hook=dict, parse_constant=_json_parse_constant_custom)
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
    parsed_data: dict = {}
    try:
        # reading only
        with commons.DetainSigint(), open(
            resolved_filepath, mode='r', encoding='utf-8') as file_handle:
            parsed_data = _load_input_json(file_handle)
        # returned data should be valid but not format-checked yet
    except commons.UnexpectedInputError as err:
        raise commons.UnexpectedInputError("Failed to parse the input file") from err
    except OSError as err:
        raise commons.FileAccessError(
            "An error occured while trying to read the input file") from err
    except ValueError as err:
        raise commons.UnexpectedInputError(
            "Encoding error occured in input file: {}".format(str(err))) from err
    return parsed_data

def _precheck_parsed_input_data(data) -> None:
    # raises 'commons.UnexpectedInputError' on check failure
    def _is_checksum_valid(obj) -> bool:
        # does not actually computes checksums, just verifies format
        return (isinstance(obj, str) and (len(obj) == 32)
                and all(map(lambda c: c in string.hexdigits, obj)))
    try:
        # not using 'assert's because their messages would be evaled every time
        # json moment
        assert isinstance(data['version'], (int, float))
        if not (isinstance(data['version'], int) and data['version'] == 1):
            raise commons.UnexpectedInputError(
                "Only \"version\" == 1 is known, {} encountered".format(str(data['id'])))
        if not _is_checksum_valid(data['checksum']):
            raise commons.UnexpectedInputError("\"checksum\" has unknown format")
        if not isinstance(data['roots'], Mapping):
            raise commons.UnexpectedInputError(
                ("JSON \"roots\" element is expected to be a mapping, \"{}\" encountered"
                ).format(type(data['data']).__name__))
        roots = data['roots']
        try:
            _required_keys: list[str] = ['bookmark_bar', 'other', 'synced']
            _optional_keys: list[str] = []
            assert all(map(lambda k: k in roots.keys(), _required_keys))
            assert all(map(lambda k: k in list(_required_keys, *_optional_keys), roots))
        except AssertionError as err:
            raise commons.UnexpectedInputError(
                "JSON \"roots\" element has unknown format") from err
        # the rest of elements have recursive structure and are to be checked during conversion
    except (AssertionError, commons.UnexpectedInputError) as err:
        raise commons.UnexpectedInputError(
            "Input data has unexpected format: {}".format(str(err))) from err
    return

# Shitty but embedded pattern declaration works too weird,
# see <https://docs.python.org/3/library/re.html>.
_NODE_GUID_RE: Final[re.Pattern] = re.compile(r'[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}')

def _check_node_contents(node, is_root: bool = False) -> None:
    # raises 'commons.UnexpectedInputError' on check failure
    # This copy is SHALLOW, the heavy 'children' is not to be copied,
    # see <https://docs.python.org/3/library/stdtypes.html#dict.copy>.
    obj = node.copy()
    try:
        assert ('type' in obj) and isinstance(obj['type'], str)
        node_type = obj['type']
        # basic checks (presense and type)
        _required_keys: dict[str, Callable[[Any], bool]] = {
            'date_added':
                lambda x: isinstance(x, str) and all(map(lambda c: c in string.digits, x)),
            'guid':         lambda x: isinstance(x, str) and re.fullmatch(_NODE_GUID_RE, x),
            'id':
                lambda x: isinstance(x, str) and all(map(lambda c: c in string.digits, x)),
            'name':         lambda x: isinstance(x, str),
            'type':         lambda x: isinstance(x, str),
        }
        _optional_keys: dict[str, Callable[[Any], bool]] = {
            'date_modified':    (lambda x: isinstance(x, str)
                                 and all(map(lambda c: c in string.digits, x))),
        }
        if node_type == 'url':
            _required_keys.update({
                'url':  lambda x: isinstance(x, str),
            })
            _optional_keys.update({
                'meta_info':    lambda x: isinstance(x, Mapping),
            })
        if node_type == 'folder':
            _required_keys.update({
                'children': lambda x: isinstance(x, Sequence),
            })
        assert all(map(lambda k: k in obj.keys(), _required_keys.keys()))
        assert all(map(
            lambda k: k in dict(_required_keys, **_optional_keys).keys(), obj.keys()))  #pylint: disable=consider-iterating-dictionary
        assert all(map(
            lambda i: dict(_required_keys, **_optional_keys)[i[0]](i[1]), obj.items()))
        # advanced checks
        assert int(obj['date_added']) >= 0
        if 'date_modified' in obj:
            assert int(obj['date_modified']) >=0
        loc_uuid = uuid.UUID(obj['guid'])
        assert loc_uuid.variant == uuid.RFC_4122
        if is_root:
            assert (loc_uuid.version is not None) and (loc_uuid.version == 5)
        else:
            assert (loc_uuid.version is not None) and (loc_uuid.version == 4)
        assert int(obj['id']) >= 0
        if 'meta_info' in obj:
            def _assert_meta_info(obj) -> None:
                _keys = ['last_visited_desktop']
                assert all(map(lambda x: x in _keys, obj.keys()))
                assert (('last_visited_desktop' in obj)
                        and isinstance(obj['last_visited_desktop'], str))
                return
            _assert_meta_info(obj['meta_info'])
    except (AssertionError, commons.UnexpectedInputError) as err:
        raise commons.UnexpectedInputError("JSON data node has unknown format") from err
    return

class _ErrorDuringTreeTraversal(RuntimeError):
    def __init__(self, *pargs, nums: Sequence[int]):
        self.nums: Sequence[int] = nums
        super().__init__(*pargs)

def _process_input_tree(
    node, callback: Callable, num: int = 0, started_at_root: bool = False) -> None:
    # ISSUE: using native references is unsafe
    # The Chrome Bookmarks file is unlikely to be extremely deep so native recursion is
    # acceptable.
    # Implementing neat error reporting with iterative stack is difficult.
    # Alternative: keeping stack of tuples (noderef, index of the node within original tree,
    #                                       index of the parent node within original tree).
    # Implementing this as generator makes returning stack numbers difficult and confusing to
    # user.
    try:
        callback(node, started_at_root)
    except Exception as err:
        raise _ErrorDuringTreeTraversal(nums=[num]) from err
    try:
        if 'children' in node:
            for child_i, child in zip(itertools.count(0), node['children']):
                _process_input_tree(child, callback, child_i, started_at_root=False)
    except _ErrorDuringTreeTraversal as err:
        err.nums.insert(0, num)
        raise err
    return

def _convert_internal_data(input_data) -> commons.LoadedData:
    output_data: commons.LoadedData = commons.LoadedData(
        datetime.datetime.now(tz=datetime.timezone.utc), set())
    def _entry_cb(node, is_root) -> None:
        # raises 'commons.UnexpectedInputError' on failure
        _check_node_contents(node, is_root)
        if node['type'] == 'url':
            output_data.entries.add((node['url'], node['name']))
        return
    for root_key, root_elem in input_data['roots'].items():
        try:
            _process_input_tree(root_elem, _entry_cb, started_at_root=True)
        except _ErrorDuringTreeTraversal as err:
            # '[1:]' removes leading '0' which is always there
            address_str: str = root_key + '.' + '.'.join(list(map(str, err.nums))[1:])
            raise commons.UnexpectedInputError(
                ("Input data has unexpected format (stopped at first failed node {})"
                ).format(address_str)) from err.__cause__
            # 'err' is dismissed here
    return output_data

def load_input_data(input_db_filepath: str) -> commons.LoadedData:
    # passing all exceptions up
    parsed_input_data = _load_from_file(input_db_filepath)
    _precheck_parsed_input_data(parsed_input_data)  # raises on failure
    loaded_data = _convert_internal_data(parsed_input_data)
    return loaded_data
