import os

import pytest
from aioresponses import aioresponses

from beelzebub.base import BaseContextManager, BaseReader, BaseWriter, BaseProcessor, BaseWorkflow

class TestBeelzebub():

    base = os.path.dirname(__file__)
    rfws_conf = {'reader': {'iotype': 'file'}, 'writer': {'iotype': 'str'}}

    def test_override_iostream_in_open(self):
        non_existent_file = self.base + '/non-existent-file'
        existent_file = self.base + '/short-test-data.json'

        f = BaseReader(iostream=non_existent_file, conf=self.rfws_conf['reader'])
        assert f.iostream == non_existent_file

        with pytest.raises(FileNotFoundError):
            f.open()
            f.read()
            f.close()

        # Override the iostream given to the constructor with a valid iostream
        f.open(iostream=existent_file)
        assert f.iostream == existent_file
        f.read()
        f.close()

    def test_optional_iostream_in_open(self):
        in_file = self.base + '/short-test-data.json'

        f = BaseReader(iostream=in_file, conf=self.rfws_conf['reader'])
        f.open()
        assert f.iostream == in_file
        f.read()
        f.close()

    def test_optional_iostream_in_constructor(self):
        in_file = self.base + '/short-test-data.json'

        f = BaseReader(conf=self.rfws_conf['reader'])
        f.open(iostream=in_file)
        assert f.iostream == in_file
        f.read()
        f.close()

    def test_iostream_not_specified(self):
        in_file = self.base + '/short-test-data.json'

        f = BaseReader()
        assert f.iostream == None

        # File iostream cannot be None
        with pytest.raises(TypeError):
            f.open(iotype='file')
            f.read()
            f.close()

        # Str iostream can be None
        f.open(iotype='str')
        assert f.iostream == None
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
        assert f.iotype == 'str'

        with pytest.raises(TypeError):
            f.open()
            f.read()
            f.close()

        # Override the iotype given to the constructor with a valid iotype
        f.open(iostream=existent_file, iotype='file')
        assert f.iotype == 'file'
        f.read()
        f.close()

    def test_invalid_iotype(self):
        in_file = self.base + '/short-test-data.json'

        f = BaseReader(iostream=in_file, conf={'iotype': 'non-existent'})
        assert f.iotype == 'non-existent'

        with pytest.raises(TypeError):
            f.open()

    def test_str_to_str_workflow(self):
        conf = {'reader': {'iotype': 'str'}, 'writer': {'iotype': 'str'}}
        source = 'this is the input'
        sink = None

        f = BaseWorkflow(conf=conf)
        f.run(source, sink)
        assert f.writer.output == source

    @pytest.mark.parametrize(['conf', 'source', 'sentinel_text'], [
    ({'reader': {'iotype': 'file'}, 'writer': {'iotype': 'str'}},  base + '/short-test-data.json', 'schema'),
    ({'reader': {'iotype': 'file', 'encoding': 'windows-1252'}, 'writer': {'iotype': 'str'}},  base + '/non-utf8.txt', 'hello'),
    ])
    def test_file_encoding(self, conf, source, sentinel_text):
        sink = None

        f = BaseWorkflow(conf=conf)
        f.run(source, sink)
        assert sentinel_text in f.writer.output

    @pytest.mark.parametrize(['conf', 'source', 'sentinel_text'], [
    ({'reader': {'iotype': 'file', 'encoding': 'utf-8'}, 'writer': {'iotype': 'str'}},  base + '/non-utf8.txt', 'hello'),
    ])
    def test_file_wrong_encoding(self, conf, source, sentinel_text):
        sink = None

        with pytest.raises(UnicodeDecodeError):
            f = BaseWorkflow(conf=conf)
            f.run(source, sink)
            assert sentinel_text in f.writer.output

    @pytest.mark.parametrize(['conf', 'mode', 'expected'], [
    ({'iotype': 'str'}, None, 'r'),                   # Unset uses fallback
    ({'iotype': 'str', 'mode': 'r'}, None, 'r'),
    ({'iotype': 'str'}, 'r', 'r'),
    ({'iotype': 'str', 'mode': 'rb'}, None, 'rb'),
    ({'iotype': 'str'}, 'rb', 'rb'),
    ({'iotype': 'str', 'mode': 'r'}, 'rb', 'rb'),     # mode trumps conf
    ])
    def test_reader_mode(self, conf, mode, expected):
        in_str = 'this is the input'

        f = BaseReader(iostream=in_str, conf=conf)

        f.open(mode=mode)
        assert f.conf['mode'] == expected
        f.close()

    @pytest.mark.parametrize(['conf', 'mode', 'expected'], [
    ({'iotype': 'str'}, None, 'w'),                   # Unset uses fallback
    ({'iotype': 'str', 'mode': 'w'}, None, 'w'),
    ({'iotype': 'str'}, 'w', 'w'),
    ({'iotype': 'str', 'mode': 'wb'}, None, 'wb'),
    ({'iotype': 'str'}, 'wb', 'wb'),
    ({'iotype': 'str', 'mode': 'w'}, 'wb', 'wb'),     # mode trumps conf
    ])
    def test_writer_mode(self, conf, mode, expected):
        f = BaseWriter(conf=conf)

        f.open(mode=mode)
        assert f.conf['mode'] == expected
        f.close()

    @pytest.mark.parametrize(['init_conf', 'open_kwargs', 'expected'], [
    ({'iotype': 'file', 'mode': 'r'}, {}, {'iotype': 'file', 'mode': 'r'}),
    ({'iotype': 'file', 'mode': 'r', 'encoding': 'utf-8'}, {}, {'iotype': 'file', 'mode': 'r', 'encoding': 'utf-8'}),
    ({'iotype': 'file', 'mode': 'r', 'encoding': 'ascii'}, {}, {'iotype': 'file', 'mode': 'r', 'encoding': 'ascii'}),
    ({'iotype': 'file', 'mode': 'rb'}, {}, {'iotype': 'file', 'mode': 'rb'}),
    # N.B.: Write tests must have iotype=str to avoid overwriting test data
    ({'iotype': 'str', 'mode': 'w'}, {}, {'iotype': 'str', 'mode': 'w'}),
    ({'iotype': 'file', 'mode': 'r'}, {'mode': 'rb'}, {'iotype': 'file', 'mode': 'rb'}),
    ({'iotype': 'file', 'mode': 'r'}, {'encoding': 'latin-1'}, {'iotype': 'file', 'mode': 'r', 'encoding': 'latin-1'}),
    # iotype passed as open kwarg, so isn't present in expected conf
    ({}, {'iotype': 'file'}, {}),
    ({}, {'iotype': 'file', 'mode': 'rb'}, {'mode': 'rb'}),
    ({}, {'iotype': 'file', 'mode': 'r', 'encoding': 'utf-8'}, {'mode': 'r', 'encoding': 'utf-8'}),
    ])
    def test_effective_conf(self, init_conf, open_kwargs, expected):
        in_file = self.base + '/short-test-data.json'

        f = BaseContextManager(iostream=in_file, conf=init_conf)
        f.open(**open_kwargs)
        assert f.conf == expected
        f.close()

    @pytest.mark.parametrize(['url', 'conf', 'protocol', 'body'], [
    ('https://host/path/data.txt', {}, 'https', 'test'),
    ])
    def test_url_iostream(self, url, conf, protocol, body):
        with aioresponses() as m:
            # repeat=True is required because multiple calls are made by fsspec
            m.get(url, repeat=True, body=body)

            f = BaseReader(iostream=url, iotype='url')
            f.conf.update(conf)

            f.open()
            assert f.fp.protocol == protocol
            f.read()
            assert f.input == body
            f.close()

