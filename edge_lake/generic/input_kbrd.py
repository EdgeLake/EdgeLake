'''
By using this source code, you acknowledge that this software in source code form remains a confidential information of AnyLog, Inc.,
and you shall not transfer it to any other party without AnyLog, Inc.'s prior written consent. You further acknowledge that all right,
title and interest in and to this source code, and any copies and/or derivatives thereof and all documentation, which describes
and/or composes such source code or any such derivatives, shall remain the sole and exclusive property of AnyLog, Inc.,
and you shall not edit, reverse engineer, copy, emulate, create derivatives of, compile or decompile or otherwise tamper or modify
this source code in any way, or allow others to do so. In the event of any such editing, reverse engineering, copying, emulation,
creation of derivative, compilation, decompilation, tampering or modification of this source code by you, or any of your affiliates (term
to be broadly interpreted) you or your such affiliates shall unconditionally assign and transfer any intellectual property created by any
such non-permitted act to AnyLog, Inc.
'''
import sys
import edge_lake.generic.utils_c as utils_c

'''
F1 (27) (79 - O)  (80 - P)    # same as Insert Key
F2 (27) (79 - O)  (81 - P)    # same as DEL Key
F3 (27) (79 - O)  (82 - P)    # same as Home Key
F4 (27) (79 - O)  (83 - P)    # Same as End Key

F5 (27) (91 - [)  (49 - 1)  (53 - 5)  (126 - ~)
F6 (27) (91 - [)  (49 - 1)  (53 - 7)  (126 - ~)
F7 (27) (91 - [)  (49 - 1)  (53 - 8)  (126 - ~)
F8 (27) (91 - [)  (49 - 1)  (53 - 9)  (126 - ~)
F9 (27) (91 - [)  (50 - 2)  (48 - 0)  (126 - ~)

ON WINDOWS
   # b 0xeo H	up
   # b 0xeo P	Down
   # b 0xeo K	Left
   # b 0xeo M	Right
   
   # b 0xeo G	Home
   # b 0xeo O	End
   # b 0xeo R	Insert
   # b 0xeo b'S'	Delete
   # b 0    b';'    F1
   # b 0    b'<'    F2
   # b 0    b'='    F3
'''

if sys.platform.startswith('win'):
    import msvcrt
else:
    import tty, termios

if sys.platform.startswith('win'):
    # Mapping windows keys to Linux
    w_f_keys_map = {
        b'H': ('[', 'A', 0),  # Up
        b'P': ('[', 'B', 0),  # Down
        b'K': ('[', 'D', 0),  # Right
        b'M': ('[', 'C', 0),  # Left
        b'G': ('[', 'H', 0),  # Home
        b'O': ('[', 'F', 0),  # End
        b'R': ('[', '2', '~'),  # Insert     '[' and ch2 == '2' and ch3 == '~':
        b'S': ('[', '3', '~'),  # Delete      '[' and ch2 == '3' and ch3 == '~':
        b';': ('[', '2', '~'),  # F1 - Insert   ch1 == '[' and ch2 == '2' and ch3 == '~':
        b'<': ('[', '3', '~'),  # F2 - Delete
        b'=': ('[', 'H', 0),  # F3 - Home
        b'>': ('[', 'F', 0),  # F4 - End

    }

# Assign strings to function keys
function_keys = {
    3: "run client "
}

MAX_INPUT_BYTES = 1000  # max input bytes
MAX_HISTORIES = 25

space_str_len = 64  # print multiple spaces at a time 64 is the default
space_str = " " * space_str_len

enter_counter_ = 0

def add_function_key(key, function_str):
    function_keys[key] = function_str

def get_enter_counter():
    return enter_counter_

# -------------------------------------------------------------------------------
# A class and method to read from the keyboard and support keyboard function keys
# in Pycharm - Check the Emulate terminal in output console setting checkbox in Run/Debug Configurations
# -------------------------------------------------------------------------------
class Keyboard:

    def __init__(self):

        self.c_lib = None  # utils_c.load_lib("al_getchar.so")    # a c function for input (supports arrows up/dowm keys)
        if self.c_lib:
            # prepare the values in and the returned value
            self.use_c = utils_c.prep_readline(self.c_lib)
            self.use_c = False
        else:
            self.use_c = False

        self.getch = _Getch()

        self.input_buff = bytearray(MAX_INPUT_BYTES + 1)
        self.history = [""] * MAX_HISTORIES
        self.index = 0
        self.last = 0
        self.insert_mode = True

    def get_cmd(self, place_in_buff):

        try:
            if self.getch.get_status():
                in_command = self.readline(place_in_buff)
            elif self.use_c:
                in_command = utils_c.readline(self.c_lib)
            else:
                in_command = sys.stdin.readline()
        except KeyboardInterrupt:
            in_command = ""

        return in_command

    # --------------------------------------------------------
    # Read a line of chars up to '\n'
    # --------------------------------------------------------
    def readline(self, place_in_buff):

        global enter_counter_

        self.insert_mode = True
        self.start_session()  # used to get previous data

        self.offset = 0  # cursor location
        self.data_size = 0  # size of data in the buffer
        while self.offset < MAX_INPUT_BYTES:
            ch = self.getch()

            #print("\n%c %u * " % (ch, ord(ch)), end='', flush=True)
            #continue

            if ch >= ' ' and ch <= '~':
                self.printable_char(ch)
                continue

            if ch == '\n' or ch == '\r':
                # add end of line at the end, regardless of cursor location
                self.input_buff[self.data_size] = ord('\n')
                self.data_size += 1
                print('\r\n', end='', flush=True)
                enter_counter_ += 1
                break

            if ch == '\b' or ord(ch) == 127:
                # backspace
                self.backspace()
                continue

            # functions
            if ord(ch) == 0x1b or ord(ch) == 0x7:       # Termius returns 7 as first char for up and down arrows
                # a function key
                ch1, ch2, ch3 = self.getch.get_function_keys()  # implemented differently for Linux and Windows
                if is_insert(ch1, ch2, ch3):
                    self.insert_mode = not self.insert_mode
                elif is_delete(ch1, ch2, ch3):
                    self.delete_char()
                elif is_up_key(ch1, ch2):
                    self.get_next()  # get the next history buffer
                elif is_down_key(ch1, ch2):
                    self.get_previous()  # Get the previous history buffer
                elif is_right_key(ch1, ch2):
                    if self.offset < self.data_size:
                        print(chr(self.input_buff[self.offset]), end='', flush=True)
                        self.offset += 1
                elif is_left_key(ch1, ch2):
                    if self.offset:
                        print('\b', end='', flush=True)
                        self.offset -= 1
                elif is_home_key(ch1, ch2, ch3):
                    #  home (1): 27 * 91 * 72
                    while self.offset:
                        print('\b', end='', flush=True)
                        self.offset -= 1
                elif is_end_key(ch1, ch2, ch3):
                    #  end (1): 27 * 91 * 70
                    while self.offset < self.data_size:
                        print(chr(self.input_buff[self.offset]), end='', flush=True)
                        self.offset += 1

        string_data = self.input_buff[:self.data_size].decode("utf-8")

        if len(string_data) != 1:
            # not enter with no data
            if not string_data[0] == '<' and place_in_buff:
                # Avoid a large data buffer in cut and paste
                self.add_buff(string_data)

        return string_data

    # --------------------------------------------------------
    # Manage backspace
    # --------------------------------------------------------
    def backspace(self):
        # backspace
        if self.offset:
            # no backspace from first position
            if self.offset == self.data_size:
                # push from the last
                print('\b', end='', flush=True)
                print(' ', end='', flush=True)  # delete char
                print('\b', end='', flush=True)
                self.offset -= 1
                self.data_size -= 1
            else:
                # fix the buffer
                self.input_buff[self.offset - 1:self.data_size - 1] = self.input_buff[self.offset:self.data_size]

                delete_output(self.data_size - self.offset, self.data_size - self.offset + 1)  # delete output on screen

                self.offset -= 1
                self.data_size -= 1
                data_string = self.input_buff[self.offset:self.data_size].decode("utf-8")
                print(data_string, end='', flush=True)  # print new buffer

                for i in range(self.offset, self.data_size):
                    print('\b', end='', flush=True)

    # --------------------------------------------------------
    # Manage a printable char
    # --------------------------------------------------------
    def printable_char(self, ch):

        older = self.input_buff[self.offset]  # the char that is overwritten
        self.input_buff[self.offset] = ord(ch)
        print(ch, end='', flush=True)
        self.offset += 1
        if self.offset > self.data_size:
            self.data_size = self.offset

        elif self.insert_mode:

            self.input_buff[self.offset + 1:self.data_size + 1] = self.input_buff[self.offset:self.data_size]
            self.input_buff[self.offset] = older
            self.data_size += 1

            data_string = self.input_buff[self.offset:self.data_size].decode("utf-8")
            print(data_string, end='', flush=True)  # print new buffer

            for i in range(self.offset, self.data_size):
                print('\b', end='', flush=True)

    # --------------------------------------------------------
    # Get the function keys 1-10
    # --------------------------------------------------------
    def get_function_key(self, ch1, ch2):

        if ch1 == 'O':
            if ch2 >= 'P' and ch2 <= 'S':
                return ord(ch2) - ord('O')  # keys F1 - F4
            return 0

        if ch1 == '[' and ch2 == '1':
            ch3 = self.getch()
            if ch3 == '5':
                ch4 = self.getch()
                if ch4 == '~':
                    return 5  # F5
                return 0
            elif ch3 == '7':
                ch4 = self.getch()
                if ch4 == '~':
                    return 6  # F6
                return 0
            elif ch3 == '8':
                ch4 = self.getch()
                if ch4 == '~':
                    return 7  # F7
                return 0
            elif ch3 == '9':
                ch4 = self.getch()
                if ch4 == '~':
                    return 8  # F8
                return 0
        return 0

    # --------------------------------------------------------
    # Delete one char
    # Delete: 27+91+51+126
    # --------------------------------------------------------
    def delete_char(self):
        if self.data_size and self.offset < self.data_size:
            self.input_buff[self.offset:self.data_size - 1] = self.input_buff[self.offset + 1:self.data_size]

            delete_output(self.data_size - self.offset,
                          self.data_size - self.offset)  # delete output on screen

            self.data_size -= 1
            data_string = self.input_buff[self.offset:self.data_size].decode("utf-8")
            print(data_string, end='', flush=True)  # print new buffer

            for i in range(self.offset, self.data_size):
                print('\b', end='', flush=True)

    # --------------------------------------------------------
    # Set state on next buffer and adjust output
    # --------------------------------------------------------
    def get_next(self):
        # goto the previous buffer
        current = self.index
        if self.index:
            self.index -= 1
        else:
            if self.history[MAX_HISTORIES - 1]:
                self.index = MAX_HISTORIES - 1
        if current != self.index:
            self.adjust_to_index()

    # --------------------------------------------------------
    # Set state on previous buffer and adjust output
    # --------------------------------------------------------
    def get_previous(self):
        current = self.index
        if self.index < MAX_HISTORIES - 1:
            if self.history[self.index + 1]:
                self.index += 1
        else:
            self.index = 0
        if current != self.index:
            # next with data
            self.adjust_to_index()

    # --------------------------------------------------------
    # Adjust buffers and output
    # --------------------------------------------------------
    def adjust_to_index(self):
        data_string = self.history[self.index]  # get older data
        if data_string:
            data_length = len(data_string)  # length of older data
            if data_length > 1:
                data_length -= 1  # ignore the new line at the end
                delete_output(self.data_size - self.offset, self.data_size)  # delete output on screen
                print(data_string[:data_length], end='', flush=True)  # print new buffer
                self.input_buff[:data_length] = data_string[:data_length].encode()  # put data in buffer

                self.offset = data_length
                self.data_size = data_length

    # --------------------------------------------------------
    # Save new buffer
    # --------------------------------------------------------
    def add_buff(self, new_buff):

        new_string = False  # no need to insert the same string as before
        if self.last:
            if new_buff != self.history[self.last - 1]:
                new_string = True
        else:
            if new_buff != self.history[MAX_HISTORIES - 1]:
                new_string = True
        if new_string:
            self.history[self.last] = new_buff
            self.last += 1
            if self.last >= MAX_HISTORIES:
                self.last = 0

    # --------------------------------------------------------
    # Start input session - reset the history index
    # --------------------------------------------------------
    def start_session(self):
        self.index = self.last


# --------------------------------------------------------
# Unifies a read of a single char from Linux and Windows
# --------------------------------------------------------
class _Getch:
    """Gets a single character from standard input.  Does not echo to the screen."""

    def __init__(self):
        try:
            if sys.platform.startswith('win'):
                self.impl = _GetchWindows()
            else:
                self.impl = _GetchUnix()
        except:
            self.impl = _GetchFailed()  # provides only get_status

    def __call__(self):
        return self.impl()

    def get_status(self):
        return self.impl.get_status()

    def get_function_keys(self):
        return self.impl.get_function_keys()


# --------------------------------------------------------
# single char from Linux
# --------------------------------------------------------
class _GetchUnix:
    def __init__(self):
        try:
            self.fd = sys.stdin.fileno()
            self.old_settings = termios.tcgetattr(self.fd)
        except:
            self.status = False
        else:
            self.status = True

    def __call__(self):
        return self.get_char()

    def get_char(self):
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(self.fd, termios.TCSADRAIN, self.old_settings)
        return ch

    def get_status(self):
        return self.status

    def get_function_keys(self):
        ch1 = self.get_char()
        ch2 = self.get_char()
        ch3 = 0
        if ch1 == '[':
            if ch2 >= '1' and ch2 <= '6':
                ch3 = self.get_char()

        reply_list = [ch1, ch2, ch3]
        return reply_list


# --------------------------------------------------------
# single char from Windows
# --------------------------------------------------------
class _GetchWindows:

    def __init__(self):
        self.status = True

    def __call__(self):
        while True:
            bin_ch = msvcrt.getch()
            if bin_ch >= b' ' and bin_ch <= b'~':
                return bin_ch.decode("ascii")
            if bin_ch == b'\r':
                return '\r'
            if bin_ch == b'\x08':
                return '\b'

            if bin_ch == b'\xe0' or bin_ch == b'\x00':
                return chr(0x1b)  # Make it like Linux function keys

    def get_status(self):
        return self.status

    # ------------------------------------------------------
    # Make the function keys like linux
    # ------------------------------------------------------
    def get_function_keys(self):
        bin_ch = msvcrt.getch()
        if bin_ch in w_f_keys_map.keys():  # Map windows to Linux
            map = w_f_keys_map[bin_ch]
            ch1 = map[0]
            ch2 = map[1]
            ch3 = map[2]
        else:
            ch1 = 0
            ch2 = 0
            ch3 = 0

        reply_list = [ch1, ch2, ch3]
        return reply_list


class _GetchFailed:
    def get_status(self):
        return False


# --------------------------------------------------------
# Delete the entire line of output on the screen
# --------------------------------------------------------
def delete_output(forward, backwords):
    # delete forward in chuncks of 64 bytes
    spaces = forward
    while spaces:
        if spaces <= space_str_len:
            print(space_str[:spaces], end='', flush=True)
            spaces = 0
        else:
            print(space_str[:space_str_len], end='', flush=True)
            spaces -= space_str_len

    for i in range(0, backwords):
        # delete backwords
        print('\b', end='', flush=True)
        if i >= forward:
            # THe case of up/down Arrow - delete chars all the way to the firs char
            print(' ', end='', flush=True)  # delete char
            print('\b', end='', flush=True)


# --------------------------------------------------------
# Test if HOME key
# --------------------------------------------------------
def is_home_key(ch1, ch2, ch3):
    if ch1 == '[' and ch2 == 'H':
        ret_val = True
    elif ch1 == 'O' and ch2 == 'R':
        ret_val = True  # F3 Key
    elif ch1 == '[' and ch2 == '1' and ch3 == '~':
        ret_val = True      # This works on Termius
    else:
        ret_val = False
    return ret_val


# --------------------------------------------------------
# Test if END key
# --------------------------------------------------------
def is_end_key(ch1, ch2, ch3):
    if ch1 == '[' and ch2 == 'F':
        ret_val = True
    elif ch1 == 'O' and ch2 == 'S':
        ret_val = True  # F4 KEY
    elif ch1 == '[' and ch2 == '4' and ch3 == '~':
        ret_val = True      # This works on Termius
    else:
        ret_val = False
    return ret_val

# --------------------------------------------------------
# Test if LEFT key
# --------------------------------------------------------
def is_left_key(ch1, ch2):
    if ch1 == '[' and ch2 == 'D':
        # Up Arrow keys
        ret_val = True
    else:
        ret_val = False
    return ret_val


# --------------------------------------------------------
# Test if Right key
# --------------------------------------------------------
def is_right_key(ch1, ch2):
    if ch1 == '[' and ch2 == 'C':
        # Up Arrow keys
        ret_val = True
    else:
        ret_val = False
    return ret_val


# --------------------------------------------------------
# Test if down arrow key -  get the previous history buffer
# --------------------------------------------------------
def is_down_key(ch1, ch2):
    if ch1 == '[' and ch2 == 'B':
        # Up Arrow keys
        ret_val = True
    else:
        ret_val = False
    return ret_val


# --------------------------------------------------------
# Test if up arrow key -  get the next history buffer
# --------------------------------------------------------
def is_up_key(ch1, ch2):
    if ch1 == '[' and ch2 == 'A':
        # Up Arrow keys
        ret_val = True
    else:
        ret_val = False
    return ret_val


# --------------------------------------------------------
# Test if an delete key
# 2 options: DEL or F2
# --------------------------------------------------------
def is_delete(ch1, ch2, ch3):
    if ch1 == '[' and ch2 == '3' and ch3 == '~':
        # Delete: 27+91+51+126
        ret_val = True
    elif ch1 == 'O' and ch2 == 'Q':
        # F2 function keys
        ret_val = True
    else:
        ret_val = False
    return ret_val


# --------------------------------------------------------
# Test if an insert key
# 2 options: INS or F1
# --------------------------------------------------------
def is_insert(ch1, ch2, ch3):
    if ch1 == '[' and ch2 == '2' and ch3 == '~':
        # insert: 27+91+50+126
        ret_val = True
    elif ch1 == 'O' and ch2 == 'P':
        # F1 function keys
        ret_val = True
    else:
        ret_val = False
    return ret_val
