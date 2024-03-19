import sys

import edge_lake.cmd.user_cmd as entry_point

def main():
    argv = sys.argv
    argc = len(argv)

    user_input = entry_point.UserInput()
    user_input.process_input(arguments=argc, arguments_list=argv)


if __name__ == '__main__':
    main()
