import datetime

DEBUG = True
LEVEL = 1
INDENT = 0

def __get_indent():
    indent = ''
    for i in range(INDENT):
        indent += '  '
    return indent


def debug(fmt, *args):
    if LEVEL > 0:
        return
    print(__get_indent()+'['+str(datetime.datetime.now())+'][DEBUG] '+(fmt % args))

def info(fmt, *args):
    if LEVEL > 1:
        return

    indent = ''
    for i in range(INDENT):
        indent += '  '
    print(__get_indent()+'['+str(datetime.datetime.now())+'][INFO] '+(fmt % args))
