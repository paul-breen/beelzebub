import logging
import argparse
import json
import sys
import os

from beelzebub.base import INIT_CONF, BaseWorkflow

PROGNAME = 'beelzebub.base'
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)

def parse_cmdln():
    """
    Parse the command line

    :returns: An object containing the command line arguments and options
    :rtype: argparse.Namespace
    """

    parser = argparse.ArgumentParser(prog=PROGNAME)

    parser.add_argument('in_file', help='file or - for stdin')
    parser.add_argument('out_file', help='file or - for stdout')
    parser.add_argument('-c', '--conf-file', help='full path to a JSON configuration file')
    parser.add_argument('-e', '--encoding', help='input file encoding')

    args = parser.parse_args()

    return args

def update_conf_from_cmdln(conf, args):
    """
    Update the configuration from command line arguments

    :param conf: The configuration
    :type conf: dict
    :param args: The command line arguments
    :type args: argparse.Namespace
    :returns: The updated configuration
    :rtype: dict
    """

    if args.conf_file:
        with open(args.conf_file, 'r') as fp:
            conf = json.load(fp)

    if args.encoding:
        conf['reader'].update({'encoding': args.encoding})

    if args.in_file == '-':
        conf['reader'].update({'iotype': 'str'})
        args.in_file = sys.stdin.read()
    elif '://' in args.in_file:
        conf['reader'].update({'iotype': 'url'})

    if args.out_file == '-':
        conf['writer'].update({'iotype': 'str'})
    elif '://' in args.out_file:
        conf['writer'].update({'iotype': 'url'})

    return conf

def main():
    args = parse_cmdln()
    conf = update_conf_from_cmdln(INIT_CONF, args)

    x = BaseWorkflow(conf=conf)
    x.run(args.in_file, args.out_file)

    if args.out_file == '-':
        print(x.writer.output, end='')

if __name__ == '__main__':
    main()

