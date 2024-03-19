/*
By using this source code, you acknowledge that this software in source code form remains a confidential information of AnyLog, Inc.,
and you shall not transfer it to any other party without AnyLog, Inc.'s prior written consent. You further acknowledge that all right,
title and interest in and to this source code, and any copies and/or derivatives thereof and all documentation, which describes
and/or composes such source code or any such derivatives, shall remain the sole and exclusive property of AnyLog, Inc.,
and you shall not edit, reverse engineer, copy, emulate, create derivatives of, compile or decompile or otherwise tamper or modify
this source code in any way, or allow others to do so. In the event of any such editing, reverse engineering, copying, emulation,
creation of derivative, compilation, decompilation, tampering or modification of this source code by you, or any of your affiliates (term
to be broadly interpreted) you or your such affiliates shall unconditionally assign and transfer any intellectual property created by any
such non-permitted act to AnyLog, Inc.
*/

/* Set terminal (tty) into "raw" mode: no line or other processing done
   Terminal handling documentation:
       curses(3X)  - screen handling library.
       tput(1)     - shell based terminal handling.
       terminfo(4) - SYS V terminal database.
       termcap     - BSD terminal database. Obsoleted by above.
       termio(7I)  - terminal interface (ioctl(2) - I/O control).
       termios(3)  - preferred terminal interface (tc* - terminal control).
*/
#include <termios.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
void tty_atexit(void);
int tty_reset(void);
void tty_raw(void);
int screenio(void);
void fatal(char *mess);
static struct termios orig_termios;  /* TERMinal I/O Structure */
static int ttyfd = STDIN_FILENO;     /* STDIN_FILENO is 0 by default */
char *al_readline()
   {
    /* check that input is from a tty */
    if (! isatty(ttyfd)) fatal("not on a tty");
    /* store current tty settings in orig_termios */
    if (tcgetattr(ttyfd,&orig_termios) < 0) fatal("can't get tty settings");
    /* register the tty reset with the exit handler */
    if (atexit(tty_atexit) != 0) fatal("atexit: can't register tty reset");
    tty_raw();      /* put tty in raw mode */
    screenio();     /* run application code */
    return 0;       /* tty_atexit will restore terminal */
   }
/* exit handler for tty reset */
void tty_atexit(void)  /* NOTE: If the program terminates due to a signal   */
{                      /* this code will not run.  This is for exit()'s     */
   tty_reset();        /* only.  For resetting the terminal after a signal, */
}                      /* a signal handler which calls tty_reset is needed. */
/* reset tty - useful also for restoring the terminal when this process
   wishes to temporarily relinquish the tty
*/
int tty_reset(void)
   {
    /* flush and reset */
    if (tcsetattr(ttyfd,TCSAFLUSH,&orig_termios) < 0) return -1;
    return 0;
   }
/* put terminal in raw mode - see termio(7I) for modes */
void tty_raw(void)
   {
    struct termios raw;
    raw = orig_termios;  /* copy original and then modify below */
    /* input modes - clear indicated ones giving: no break, no CR to NL,
       no parity check, no strip char, no start/stop output (sic) control */
    raw.c_iflag &= ~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
    /* output modes - clear giving: no post processing such as NL to CR+NL */
    raw.c_oflag &= ~(OPOST);
    /* control modes - set 8 bit chars */
    raw.c_cflag |= (CS8);
    /* local modes - clear giving: echoing off, canonical off (no erase with
       backspace, ^U,...),  no extended functions, no signal chars (^Z,^C) */
    raw.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
    /* control chars - set return condition: min number of bytes and timer */
    raw.c_cc[VMIN] = 5; raw.c_cc[VTIME] = 8; /* after 5 bytes or .8 seconds
                                                after first byte seen      */
    raw.c_cc[VMIN] = 0; raw.c_cc[VTIME] = 0; /* immediate - anything       */
    raw.c_cc[VMIN] = 2; raw.c_cc[VTIME] = 0; /* after two bytes, no timer  */
    raw.c_cc[VMIN] = 0; raw.c_cc[VTIME] = 8; /* after a byte or .8 seconds */
    /* put terminal in raw mode after flushing */
    if (tcsetattr(ttyfd,TCSAFLUSH,&raw) < 0) fatal("can't set raw mode");
   }
/* Read and write from tty - this is just toy code!!
   Prints T on timeout, quits on q input, prints Z if z input, goes up
   if u input, prints * for any other input character
*/
int screenio(void)
   {
    int bytesread;
    char c_in, c_out, up[]="\033[A";
    char eightbitchars[256];                  /* will not be a string! */
    /* A little trick for putting all 8 bit characters in array */
    {int i;  for (i = 0; i < 256; i++) eightbitchars[i] = (char) i; }
    for (;;)
       {bytesread = read(ttyfd, &c_in, 1 /* read up to 1 byte */);
        if (bytesread < 0) fatal("read error");
        if (bytesread == 0)        /* 0 bytes inputed, must have timed out */
           {c_out = 'T';           /* straight forward way to output 'T' */
            write(STDOUT_FILENO, &c_out, 1);
           }
        else switch (c_in)         /* 1 byte inputed */
           {case 'q' : return 0;   /* quit - no other way to quit - no EOF */
            case 'z' :             /* tricky way to output 'Z' */
                write(STDOUT_FILENO, eightbitchars + 'Z', 1);
                break;
            case 'u' :
            	write(STDOUT_FILENO, up, 3);  /* write 3 bytes from string */
            	break;
            default :
                c_out = '*';
                write(STDOUT_FILENO, &c_out, 1);
           }
       }
   }
void fatal(char *message)
   {
    fprintf(stderr,"fatal error: %s\n",message);
    exit(1);
   }


// keyboard input - https://viewsourcecode.org/snaptoken/kilo/03.rawInputAndOutput.html


/*
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <termios.h>
#include <unistd.h>


#define CTRL_KEY(k) ((k) & 0x1f)

struct editorConfig {
  struct termios orig_termios;
};
struct editorConfig E;

void disableRawMode() {
  if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &E.orig_termios) == -1)
    printf("\nError");
}
void enableRawMode() {
  if (tcgetattr(STDIN_FILENO, &E.orig_termios) == -1) {
    printf("\nError");
    disableRawMode();
  }
  struct termios raw = E.orig_termios;
  raw.c_iflag &= ~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
  raw.c_oflag &= ~(OPOST);
  raw.c_cflag |= (CS8);
  raw.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
  raw.c_cc[VMIN] = 0;
  raw.c_cc[VTIME] = 1;
  if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) == -1) printf("\nError");
}




char *al_readline() {
  enableRawMode();
  while (1) {
    char c = '\0';
    if (read(STDIN_FILENO, &c, 1) == -1 && errno != EAGAIN)
        printf("\nError");
    if (iscntrl(c)) {
      printf("%d\r\n", c);
    } else {
      printf("%d ('%c')\r\n", c, c);
    }
    if (c == CTRL_KEY('q')) break;
  }
  return 0;
}


*/
// Find package libcurses.so location:
// dpkg -L libncurses5-dev

// dynamicaly link the curses package:
// gcc al_getchar.c /usr/lib/x86_64-linux-gnu/libcurses.a

// gcc -std=c11 -Wall -Wextra -pedantic -c -fPIC al_getchar.c -o al_getchar.o
// gcc -shared al_getchar.o -o al_getchar.so /usr/lib/x86_64-linux-gnu/libcurses.so

/* nucurses need to be installed
In deb based Distros use
sudo apt-get install libncurses5-dev libncursesw5-dev
And in rpm based distros use
sudo yum install ncurses-devel ncurses */

/*
#include <stdio.h>
#include <curses.h>


#define MAX_STRING_SIZE 1000

char *al_readline()
{
    char buffer[MAX_STRING_SIZE];

    int ch;
    int offset = 0;

    //newterm();          // first thing to do with curses
    //WINDOW *win = initscr();          // first thing to do with curses
    //cbreak();           // each char is returned immediately (without newline)
    //keypad(stdscr, TRUE); // capture Backspace, Delete and the 4 arrow keys
    timeout(-1);         // waits indefinitely for input

    while (1){

        ch = getch();
        buffer[offset] = ch;
        //putchar(ch);
        printf("-%u-",ch);

        if (ch == '\b'){
            if (offset){
                --offset;
                putchar(ch); // move back
            }else{
                putchar('\b');
                ++offset;
                if (offset >= MAX_STRING_SIZE){
                    offset = MAX_STRING_SIZE - 1;
                }
            }
        }


    }

    endwin();       // restore terminal setting

    return "test string";
}
*/