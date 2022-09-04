#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (C) 2021-2022 asrelo
# All rights reserved.
#
# This file is part of "hasty-ruby-coypu".

"""hasty-ruby-coypu
"""

import argparse
import collections
import locale
import sys
from typing import Final

import application
import commons

class ArgumentCustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                              argparse.RawDescriptionHelpFormatter):
    """The custom fomatter class for `argparse`

    Combines features of `argparse.ArgumentDefaultsHelpFormatter` (printing default values for
    arguments) and `argparse.RawDescriptionHelpFormatter` (printing description and epilog texts
    as formatted).
    """
    pass  #pylint: disable=unnecessary-pass

class ArgumentCustomParser(argparse.ArgumentParser):
    """The custom parser class for `argparse`

    Currently the additional features are:

    * Support for automatic adding of "slash-keys" (i.e. `/foo` for `--foo` and `/b` for `-b`)

    Attributes:
        enable_slash_keys: Boolean, whether it is requested to add slash keys with the dash ones.
    """
    def __init__(self, *pargs, **kwargs):
        """Initializes the same way as the `argparse.ArgumentParse.__init__` but with the
        additional argument

        Passes all non-own arguments to the parent's `__init__`.
        See <https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser>
        for details.

        Args:
            enable_slash_keys: boolean, whether to enable slash keys additions
            All other `argparse.ArgumentParse.__init__` arguments.

        Raises:
            Anything that `argparse.ArgumentParse.__init__` raises with the given arguments.
        """
        self._enable_slash_keys: bool
        kwargs_own = collections.defaultdict(
            lambda: None, commons.fun_project(kwargs, ('enable_slash_keys')))
        # bool converts `None` to `False`
        self._enable_slash_keys = bool(kwargs_own['enable_slash_keys'])
        super().__init__(*pargs, **kwargs)
    @property
    def enable_slash_keys(self):
        return self._enable_slash_keys
    def add_argument(self, *pargs, **kwargs):
        # note the `ret_val`
        ret_val = super().add_argument(*pargs, **kwargs)
        if self._enable_slash_keys:
            for key in pargs:
                if not isinstance(key, str):
                    continue
                if key.startswith('--') and len(key)>len('--'):
                    key = '/'+key[2:]
                elif key.startswith('-') and len(key)>len('-'):
                    key = '/'+key[1:]
            super().add_argument(*pargs, **kwargs)
        return ret_val

def parse_args(argv: list[str], name_passed: bool = True) -> argparse.Namespace:
    """Performs program arguments parsing

    Accepts either a full `sys.argv`-like list (with `name_passed` is `True`) or a sliced
    `sys.argv[1:]`-like list (with `name_passed` is `False`). Uses the explicitly passed program
    name if there is one, or inspects the `sys.argv` directly if not. If inspecting `sys.argv`
    has failed, uses the internal default name.

    Parses the program's arguments. **Prints custom info to console and terminates the program on
    a help call, version call or invalid arguments.**

    Does not raise on invalid args - **just terminates** the program!

    Raises:
        ValueError: Contradicting parameters were passed.
    """
    # Attention: this may terminate the program with either success (on a 'version' call) or
    # failure (on arguments error).
    assert (isinstance(argv, list) and all(map(lambda x: isinstance(x, str), argv)))
    assert isinstance(name_passed, bool)
    assert len(argv) > 0 or not name_passed
    prog_name: str
    if name_passed:
        prog_name = argv[0]
    elif not sys.argv:
        prog_name = sys.argv[0]
    else:
        prog_name = commons.DEFAULT_PROG_NAME
    parser: argparse.ArgumentParser = ArgumentCustomParser(
        prog=prog_name, description=__doc__, epilog=commons.COPYRIGHT_NOTICE,
        formatter_class=ArgumentCustomFormatter, add_help=True, allow_abbrev=True)
    # commons
    parser.add_argument('--version', action='version', version=commons.VERSION)
    parser.add_argument('--verbose-errors', action='store_true',
        help="verbose error messages (prints Python traceback for some errors)",
        dest='verbose_errors')
    mode_subparsers = parser.add_subparsers(
        title="Input source", prog=None, dest='conv_type', required=True,
        help=("Choose the source for input (mode)"))
    # subparsers
    tosqldb_parser = mode_subparsers.add_parser(
        'tosqldb', prog=None, description=(
            ("Loads data from Tabs Outliner SQLite DB to hasty-ruby-coypu DB. Note that this"
             " does not change anything in Tabs Outliner DB.")),
        formatter_class=ArgumentCustomFormatter, add_help=True, allow_abbrev=True)
    gcbf_parser = mode_subparsers.add_parser(
        'gcbf', prog=None, description=(
            ("Loads data from Google Chrome Bookmarks file to hasty-ruby-coypu DB. Note that"
              " this does not change anything in Google Chrome Bookmarks.")),
        formatter_class=ArgumentCustomFormatter, add_help=True, allow_abbrev=True)
    # tosqldb_parser
    tosqldb_parser.add_argument(
        '-i', '--input', nargs='?', required=True, help="input DB (from \"Tabs Outliner\")",
        metavar="input DB", dest='input_db')
    tosqldb_parser.add_argument(
        '-o', '--output', nargs='?', required=True, help="output DB (\"hasty-ruby-coypu\")",
        metavar="output DB", dest='output_db')
    # gcbf_parser
    gcbf_parser.add_argument(
        '-i', '--input', nargs='?', required=True, help="input file (from \"Google Chrome\")",
        metavar="input file", dest='input_db')
    gcbf_parser.add_argument(
        '-o', '--output', nargs='?', required=True, help="output DB (\"hasty-ruby-coypu\")",
        metavar="output DB", dest='output_db')
    args: argparse.Namespace = parser.parse_args(argv[(1 if name_passed else 0):])
    return args

def run():
    """The main entry point for some extended utility work

    Calls the working entry point doing some utility work around it (controls locales, invokes
    arguments parsing, configures logging etc.).

    May return anything, raise anything, or terminate the program.
    """
    # If `KeyboardInterrupt` occurs during uncovered code, the exception is intended to be passed
    # to the over-global processor.
    # Apply the user’s default settings for everything.
    locale.setlocale(locale.LC_ALL, '')
    # `parse_args` may terminate the program with either success (on a 'help' or 'version' call)
    # or failure (on arguments error).
    parsed_args: argparse.Namespace = parse_args(sys.argv, name_passed = True)
    return_code: Final[commons.ExitCode]
    try:
        with commons.FaultHadler():
            return_code = application.main(parsed_args, sys.argv[0])
    except KeyboardInterrupt:
        return_code = commons.ExitCode.EUNSPEC
        # pass work to the next context manager
    with commons.DetainSigint(suppress=True):
        assert ('return_code' in locals()) and isinstance(return_code, int)
        sys.exit(return_code)

if __name__=='__main__':
    try:
        run()  # may terminate! don't expect anything after it
    except KeyboardInterrupt as err:
        # here may be some custom global cleanup
        sys.exit(1)  # "unspecified error" code
    # Custom processors for `SystemExit` and a general `Exception` may be added later.
