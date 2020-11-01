import os

import pytest

from beelzebub.base import BaseReader, BaseWriter, BaseProcessor, BaseWorkflow

class TestBeelzebub():

    base = os.path.dirname(__file__)
    rfws_conf = {'reader': {'iotype': 'file'}, 'writer': {'iotype': 'str'}}

    def test_override_iostream_in_open(self):
        non_existent_file = self.base + '/non-existent-file'
        existent_file = self.base + '/short-test-data.json'

        f = BaseReader(iostream=non_existent_file, conf=self.rfws_conf['reader'])

        with pytest.raises(FileNotFoundError):
            f.open()
            f.read()
            f.close()

        # Override the iostream given to the constructor with a valid iostream
        f.open(iostream=existent_file)
        f.read()
        f.close()

    def test_optional_iostream_in_open(self):
        in_file = self.base + '/short-test-data.json'

        f = BaseReader(iostream=in_file, conf=self.rfws_conf['reader'])
        f.open()
        f.read()
        f.close()

    def test_optional_iostream_in_constructor(self):
        in_file = self.base + '/short-test-data.json'

        f = BaseReader(conf=self.rfws_conf['reader'])
        f.open(iostream=in_file)
        f.read()
        f.close()

    def test_iostream_not_specified(self):
        in_file = self.base + '/short-test-data.json'

        f = BaseReader()

        # File iostream cannot be None
        with pytest.raises(TypeError):
            f.open(iotype='file')
            f.read()
            f.close()

        # Str iostream can be None
        f.open(iotype='str')
        f.read()
        f.close()

    def test_iotype_set_via_kwarg_in_constructor(self):
        f = BaseReader(iotype='file')
        assert f.iotype == 'file'

    def test_iotype_set_via_conf_in_constructor(self):
        f = BaseReader(conf=self.rfws_conf['reader'])
        assert f.iotype == 'file'

    def test_iotype_absent_from_conf_in_constructor(self):
        f = BaseReader(conf={})
        assert f.iotype == ''

    def test_override_iotype_in_open(self):
        non_existent_str = 42
        existent_file = self.base + '/short-test-data.json'

        f = BaseReader(iostream=non_existent_str, conf={'iotype': 'str'})

        with pytest.raises(TypeError):
            f.open()
            f.read()
            f.close()

        # Override the iotype given to the constructor with a valid iotype
        f.open(iostream=existent_file, iotype='file')
        f.read()
        f.close()

    def test_str_to_str_workflow(self):
        conf = {'reader': {'iotype': 'str'}, 'writer': {'iotype': 'str'}}
        source = 'this is the input'
        sink = None

        f = BaseWorkflow(conf=conf)
        f.run(source, sink)

        assert f.writer.output == source

