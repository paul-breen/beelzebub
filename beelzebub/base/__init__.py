###############################################################################
# Project: beelzebub
# Purpose: Classes to manage transformation of input to output
# Author:  Paul M. Breen
# Date:    2020-10-01
###############################################################################

__version__ = '1.0.0'

import logging
import logging.config
import io
import urllib.parse
import jinja2
import fsspec

logger = logging.getLogger(__name__)

INIT_CONF = {
    'reader': {'iotype': 'file'},
    'writer': {'iotype': 'file'},
}

class BaseContextManager(object):
    """
    Context manager base class
    """

    def __init__(self, iostream=None, iotype='', conf={}):
        """
        Constructor

        :param iostream: Iostream (file path, URL, or str)
        :type iostream: str
        :param iotype: Type of the iostream ['file','url','str']
        :type iotype: str
        :param conf: Optional configuration
        :type conf: dict
        """

        self.iostream = iostream
        self.iotype = iotype
        self.conf = conf
        self.fp = None

        if not self.iotype:
            try:
                self.iotype = self.conf['iotype']
            except KeyError:
                pass

    def __enter__(self):
        """
        Enter the runtime context for this object

        The iostream is opened

        :returns: This object
        :rtype: BeelzebubBaseContextManager
        """

        return self.open(iostream=self.iostream)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Exit the runtime context for this object

        The iostream is closed

        :returns: False
        :rtype: bool
        """

        self.close()

        return False         # This ensures any exception is re-raised

    def _construct_open_opts(self):
        """
        Construct the options for the iostream open() call based on
        self.iotype, the kwargs passed to self.open() and self.conf

        :returns: opts
        :rtype: dict
        """

        opts = {}

        if self.iotype.lower() == 'file' or self.iotype.lower() == 'url':
            'mode' in self.conf and opts.update({'mode': self.conf['mode']})
            'encoding' in self.conf and opts.update({'encoding': self.conf['encoding']})
        elif self.iotype.lower() == 'str':
            'encoding' in self.conf and opts.update({'encoding': self.conf['encoding']})

        logger.debug(f"effective open() options = {opts}")

        return opts

    def open(self, iostream=None, iotype='', mode=None, encoding=None, **fs_opts):
        """
        Open the given iostream

        The effective configuration is established in the following order
        of precedence:

        * Any non-None kwargs are used to update corresponding self.conf item
        * The self.conf values are used as-provided
        * Any missing self.conf values will use system built-in defaults

        If a local iostream (iotype=file) or a remote iostream (iotype=url)
        is binary, then the mode should include the binary flag (e.g. 'rb')
        and encoding must not be specified.  If the local or remote iostream
        is text, then omit the binary flag and optionally provide an encoding
        if required.  If iotype=str, then mode must not be specified and
        encoding is only required if it is not the system default

        :param iostream: Iostream (file path, URL, or str)
        :type iostream: str
        :param iotype: Type of the iostream ['file','url','str'].  Raises
        TypeError if iotype is not one of these supported types
        :type iotype: str
        :param mode: Mode in which to open if iostream is a file or url
        :type mode: str
        :param encoding: Encoding if iostream is text
        :type encoding: str
        :param fs_opts: Any kwargs required for opening a url type iostream
        using fsspec
        :type fs_opts: dict
        :returns: This object
        :rtype: BeelzebubBaseContextManager
        :raises: TypeError
        """

        if iostream:
            self.iostream = iostream

        if iotype:
            self.iotype = iotype

        if mode:
            self.conf['mode'] = mode

        if encoding:
            self.conf['encoding'] = encoding

        if fs_opts:
            'fs_opts' in self.conf or self.conf.update({'fs_opts': {}})
            self.conf['fs_opts'].update(**fs_opts)

        logger.debug(f"effective iostream configuration = {self.conf}")

        opts = self._construct_open_opts()

        if self.iotype.lower() == 'file':
            self.fp = io.open(self.iostream, **opts)
        elif self.iotype.lower() == 'url':
            protocol = urllib.parse.urlparse(self.iostream).scheme
            fs = fsspec.filesystem(protocol, **fs_opts)
            self.fp = fs.open(self.iostream, **opts)
            self.fp.remote = True
            self.fp.protocol = protocol
        elif self.iotype.lower() == 'str':
            self.fp = io.StringIO(self.iostream, **opts)
        else:
            raise TypeError(f"unsupported iotype {self.iotype}")

        return self

    def close(self):
        """
        Close the iostream

        :returns: This object
        :rtype: BeelzebubBaseContextManager
        """

        self.fp.close()
        self.fp = None

        return self

class BaseReader(BaseContextManager):
    """
    Context manager for reading input
    """

    def __init__(self, **kwargs):
        """
        Constructor

        Takes the same arguments as super.__init__()
        """

        super().__init__(**kwargs)
        self.input = None

    def open(self, mode=None, **kwargs):
        """
        Open the given iostream

        If mode is not provided nor set in self.conf, then it defaults to 'r'

        Takes the same arguments as super.open()
        """

        mode = mode or self.conf.get('mode', 'r')

        return super().open(mode=mode, **kwargs)

    def read(self):
        """
        Read the input iostream

        The input is loaded as a str and accessible via self.input

        :returns: The input
        :rtype: str
        """

        self.input = self.fp.read()

        return self.input

class BaseWriter(BaseContextManager):
    """
    Context manager for writing output
    """

    DEFAULTS = {
       'template': None
    }

    def __init__(self, **kwargs):
        """
        Constructor

        Takes the same arguments as super.__init__()
        """

        super().__init__(**kwargs)
        self.output = None

    def open(self, mode=None, **kwargs):
        """
        Open the given iostream

        If mode is not provided nor set in self.conf, then it defaults to 'w'

        Takes the same arguments as super.open()
        """

        mode = mode or self.conf.get('mode', 'w')

        return super().open(mode=mode, **kwargs)

    def infill_template(self, data, template=None):
        """
        Use the input dict to infill the template

        The infilled template ouput is accessible via self.output

        If no template is specified nor set as default, then self.output is
        just the input data

        :param data: Input data
        :type data: dict
        :param template: Optionally specify an alternative template
        :type template: str
        :returns: The output
        :rtype: str
        """

        if not template:
            template = self.DEFAULTS['template']

        if not template:
            self.output = data
        else:
            self.output = jinja2.Environment(loader=jinja2.FileSystemLoader('/'), trim_blocks=True, lstrip_blocks=True).get_template(template).render(data)

        return self.output

    def transform(self, data):
        """
        Transform the input dict to the output str

        The transformed ouput str is accessible via self.output

        :param data: Input data
        :type data: dict
        :returns: The output
        :rtype: str
        """

        self.infill_template(data)

        return self.output

    def write(self, data):
        """
        Write the output iostream

        The output is written as a str and accessible via self.output

        :param data: Input data
        :type data: dict
        :returns: The output
        :rtype: str
        """

        self.transform(data)
        self.fp.write(self.output)

        return self.output

class BaseProcessor(object):
    """
    Execute an input to output processing workflow
    """

    def __init__(self, reader, writer, conf={}):
        """
        Constructor

        :param reader: reader class
        :type reader: class
        :param writer: writer class
        :type writer: class
        :param conf: Optional configuration
        :type conf: dict
        """

        self.reader = reader
        self.writer = writer
        self.conf = conf
        self.input = None
        self.output = None

    def process(self):
        """
        Process the input to output
        """

        with self.reader as fp:
            self.input = fp.read()

        with self.writer as fp:
            fp.write(self.input)

class BaseWorkflow(object):
    """
    Setup an input to output processing workflow
    """

    def __init__(self, reader_class=BaseReader, writer_class=BaseWriter, processor_class=BaseProcessor, conf={}):
        """
        Constructor

        :param reader: reader class
        :type reader: class
        :param writer: writer class
        :type writer: class
        :param processor: input to output processor class
        :type processor: class
        :param conf: Optional configuration
        :type conf: dict
        """

        self.reader_class = reader_class
        self.writer_class = writer_class
        self.processor_class = processor_class
        self.conf = conf
        self.source = None
        self.sink = None

    def get_conf_section(self, section):
        """
        Get the named section from the configuration

        :param section: The name of the configuration section
        :type section: str
        :returns: The configuration section
        :rtype: dict
        """

        conf_section = None

        try:
            conf_section = self.conf[section]
        except KeyError:
            pass

        return conf_section

    def setup_logging(self):
        """
        Optionally setup logging for the workflow

        If the optional configuration contains a `logger` section, then it
        is used to configure logging for the workflow
        """

        logging_conf = self.get_conf_section('logger')

        if logging_conf:
            logging.config.dictConfig(logging_conf)

    def process(self):
        """
        Process the input to output
        """

        self.reader = self.reader_class(iostream=self.source, conf=self.get_conf_section('reader'))
        self.writer = self.writer_class(iostream=self.sink, conf=self.get_conf_section('writer'))
        self.processor = self.processor_class(self.reader, self.writer, conf=self.get_conf_section('processor'))
        self.processor.process()

    def run(self, source, sink):
        """
        Run the input to output workflow
        """

        self.source = source
        self.sink = sink
        self.setup_logging()
        self.process()

