# beelzebub

Beelzebub is a lightweight framework to transform input to output.  The base classes aren't meant to be used directly, rather they establish the interfaces of the framework, and provide a basis with which to derive classes for defining a particular transformation workflow.

A workflow consists of a reader class, a writer class, and a processor class.  The workflow class instantiates one of each of these classes, and then executes the workflow of reading input from a given *source* via the reader class, writing output to a given *sink* via the writer class, and the processor class calls the reader and passes the read input to the writer.

Both the reader and writer classes are based on a common context manager class.  In particular, the `open()` method can read/write to one of a set of supported *iostream* types.  The *iotype* should be one of `['file','url','str']` and defaults to `str`.

The workflow class can optionally setup logging for the workflow (based on the existence of a `logger` section in the optional configuration dict), and then calls the `run()` method, passing the source and sink.

As mentioned, an optional configuration dict can be passed when instantiating the workflow object.  As a particular workflow will have specific reader, writer and processor classes, the configuration items for each of these components is arbitrary, suited to the particular workflow.  However, the framework will look for a toplevel key called `reader` to pass to the reader class, `writer` to pass to the writer class, and `processor` to pass to the processor class.  In addition, if a `logger` key exists, then this will be used to configure logging, via a call to `logging.config.dictConfig(logging_conf)`.

One of the main uses of the configuration is to specify the iotype for the reader and writer.  For example, if the input is read from a file, but the output is to be written to a string, then at least a `reader` configuration is required, as `file` is not the default iotype for the reader:

```python
conf = {'reader': {'iotype': 'file'}, 'writer': {'iotype': 'str'}}
in_file = sys.argv[1]
out_file = None

x = BaseWorkflow(conf=conf)
x.run(in_file, out_file)
print(x.writer.output)
```

Note that if the output is to be written to a string, then the sink argument (here, `out_file`) to `run()` is redundant, and can be set to `None`.  In this case, access the output string via the workflow's writer's `output` attribute.

