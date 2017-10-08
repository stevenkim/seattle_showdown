import datetime

DEBUG = True
INDENT = 0

def __get_indent():
    indent = ''
    for i in range(INDENT):
        indent += '  '
    return indent


def debug(fmt, *args):
    if not DEBUG:
        return
    print(__get_indent()+'['+str(datetime.datetime.now())+'][DEBUG] '+(fmt % args))

def info(fmt, *args):
    indent = ''
    for i in range(INDENT):
        indent += '  '
    print(__get_indent()+'['+str(datetime.datetime.now())+'][INFO] '+(fmt % args))
