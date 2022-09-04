#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2021-2022 asrelo
# All rights reserved.
#
# This file is part of "hasty-ruby-coypu".

"""hasty-ruby-coypu

Commons
"""

#pylint: disable=ungrouped-imports

from typing import Final

# Using semantic versioning
VERSION: Final[str] = "0.0.0"

COPYRIGHT_NOTICE: Final[str] = """\
Copyright (C) 2021 asrelo
"""
# The copyright notice should have the line width of 80 characters as it may be displayed
# unchanged to a classic terminal.

import contextlib
import datetime
import enum
import faulthandler
import os.path
import pathlib
import signal
import sqlite3
import sys
import traceback
import uuid
from collections.abc import Callable, Collection, Generator, Mapping
from typing import Any, NamedTuple, Optional, Union

UUID_BASE_NAMESPACE: Final = uuid.UUID(
    hex='302274371da54e0f91cfcc4174fb6cc6', is_safe=uuid.SafeUUID.safe)

DEFAULT_PROG_NAME: Final[str] = "hasty-ruby-coypu"

def fun_project(mapping: Mapping, keys: Collection) -> Mapping:
    # see 'funcy.project' for reference
    _factory: Callable[[Generator], Mapping]
    if isinstance(mapping, dict):
        _factory = dict
    else:
        assert False
    return _factory((k, mapping[k]) for k in keys if k in mapping)

def silence_exc(func: Callable, excs: Union[type, Collection[type]] = Exception) -> tuple:
    """Executes a given callable, catches its exception (if any raised) and returns results

    Args:
        func: callable to be called
        excs: type of exception to be catched (or a collection of types); default is the general
            `Exception`

    Returns:
        A tuple with the first element being the callable returned result and the second element
        being the exception object catched (`None` if none was raised).

    Raises:
        None. (`TypeError` or `ValueError` on an invalid call.)
    """
    assert isinstance(func, Callable)
    assert ((isinstance(excs, Collection) and all(map(lambda e: isinstance(e, type), excs)))
            or isinstance(excs, type))
    raised: Optional[BaseException] = None
    result: Optional[Any] = None
    try:
        result = func()
    except BaseException as err:  #pylint: disable=broad-except
        if err in excs:
            raised = err
        else:
            raise err  # re-raise
    return (result, raised)

class ExitCode(enum.IntEnum):
    SUCCESS   = 0
    EUNSPEC   = 1
    EFILEACC  = 2
    EDBUNSPEC = 5
    EBADINPUT = 22
    EUSRINTER = 10054

class DetainSigint(contextlib.AbstractContextManager):
    """Helper context manager to temporarily delay or suppress `SIGINT` events

    Disables current processing of `SIGINT` events (`Ctrl+C`, `KeyboardInterrupt`, this kind of
    stuff) and re-enables it on context end. If any `SIGINT` occures during execution of a
    context, it will be raised again **once** after a context exits.

    Also can `suppress` the events completely.
    """
    def __init__(self, suppress: bool = False):
        assert isinstance(suppress, bool)
        self._suppress: bool = suppress
        self._sigint_occured: bool
        self._prev: Any
    def __enter__(self):
        self._sigint_occured = False
        def _detain_handler(signalnum, stackframe):
            #pylint: disable=unused-argument
            self._sigint_occured = True
        _handler = _detain_handler if not self._suppress else signal.SIG_IGN
        self._prev = signal.signal(signal.SIGINT, _handler)
        return self
    def __exit__(self, exc_type, exc_value, tb):
        signal.signal(signal.SIGINT, self._prev)
        if self._sigint_occured:
            signal.raise_signal(signal.SIGINT)
        return False  # do not suppress exceptions

class FaultHadler(contextlib.AbstractContextManager):
    """Helper context manager for the `faulthandler` module"""
    def __init__(self, file=sys.stderr, all_threads=True):
        """Accepts the same arguments as `faulthandler.enable` does

        See <https://docs.python.org/3/library/faulthandler.html#faulthandler.enable>
        for details."""
        assert isinstance(all_threads, bool)
        self._file = file
        self._all_threads = all_threads
    def __enter__(self):
        faulthandler.enable(self._file, self._all_threads)
        return self
    def __exit__(self, exc_type, exc_value, tb):
        faulthandler.disable()
        return False  # do not suppress exceptions

def print_exception_traceback(err: Exception) -> None:
    traceback.print_exception(err.__type__, err, err.__traceback__, chain=True)

# Application-specific part starts here

class FileAccessError(OSError):
    pass

class DatabaseAccessError(RuntimeError):
    pass

class UnexpectedInputError(ValueError):
    pass

# crutchy, for local uses only
class ExceptionOccuredWhile(contextlib.AbstractContextManager):
    def __init__(self, text: str):
        self._text: str = text
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, tb):
        if exc_value is not None:
            exc_value.occured_while = self._text  #pylint: disable=attribute-defined-outside-init
        return False  # do not suppress exception

class ProcessSigintAsInterruption(contextlib.AbstractContextManager):
    def __init__(self, mes: Union[str, Callable]):
        self._mes: Union[str, Callable] = mes
        self._prev: Callable
    def __enter__(self):
        def _sigint_custom_handler(signalnum, stackframe):
            #pylint: disable=unused-argument
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            for i in range(3):  #pylint: disable=unused-variable
                print(self._get_message_text(), end=' ')
                t = input().lower()
                if t in ("y", "n"):
                    break
            if t == "y":
                signal.signal(signal.SIGINT, _sigint_custom_handler)
                return
            # else
            signal.raise_signal(signal.SIGINT)
            return
        self._prev = signal.signal(signal.SIGINT, _sigint_custom_handler)
        return self
    def __exit__(self, exc_type, exc_value, tb):
        signal.signal(signal.SIGINT, self._prev)
        return False  # do not suppress exceptions
    def _get_message_text(self):
        temp: str
        if isinstance(self._mes, Callable):
            temp = self._mes()
        # else
        temp = self._mes
        return "{} Do terminate the program now? (y/n)".format(temp)

def get_cursor_from_api_obj_custom(api_obj: Union[sqlite3.Connection, sqlite3.Cursor]
    ) -> sqlite3.Cursor:
    if isinstance(api_obj, sqlite3.Connection):
        return api_obj.cursor(factory=Sqlite3CursorCustom)
    # else
    return api_obj

# used as factory
class Sqlite3CursorCustom(contextlib.AbstractContextManager, sqlite3.Cursor):
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, tb):
        self.close()
        return False  # do not suppress exceptions

class Sqlite3TransactionCustom(contextlib.AbstractContextManager):
    def __init__(self, api_obj: Union[sqlite3.Connection, sqlite3.Cursor], savepoint_name: str):
        assert savepoint_name.isidentifier()  # cheap but works :)
        self._savepoint_name: str = savepoint_name.lower()
        self._api_obj: Union[sqlite3.Connection, sqlite3.Cursor] = api_obj
    def __enter__(self):
        with get_cursor_from_api_obj_custom(self._api_obj) as cursor:
            cursor.execute('SAVEPOINT \'{}\''.format(self._savepoint_name))
        return self
    def __exit__(self, exc_type, exc_value, tb):
        with get_cursor_from_api_obj_custom(self._api_obj) as cursor:
            cursor.execute('RELEASE \'{}\''.format(self._savepoint_name))
        return False  # do not suppress exceptions

def path_resolve_custom(path_str: str) -> pathlib.Path:
    # 'pathlib.resolve' не резолвит путь, если он не существует уже в ФС,
    # а 'os.path.abspath' не умеет раскрывать ссылки
    path = pathlib.Path(os.path.abspath(path_str))
    try:
        resolved_filepath = path.resolve()
    except RuntimeError as err:
        raise FileAccessError("Loop in path detected") from err
    return resolved_filepath

class LoadedData(NamedTuple):
    #pylint: disable=too-few-public-methods
    output_timestamp: datetime.datetime
    entries: Collection[tuple[str, str]] = set()
