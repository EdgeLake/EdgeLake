"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import sys

import edge_lake.cmd.user_cmd as entry_point

def main():
    argv = sys.argv
    argc = len(argv)

    user_input = entry_point.UserInput()
    user_input.process_input(arguments=argc, arguments_list=argv)


if __name__ == '__main__':
    main()
