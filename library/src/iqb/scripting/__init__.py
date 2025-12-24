"""
Support for writing scripts using the IQB framework.

The expected usage of core IQB types is like:

    from iqb import IQBPipeline

However, the scripting package is not part of the core
IQB framework, and contains extensions.

Therefore, the expected usage is as follows:

    from iqb.scripting import iqb_logging

That is, each module whose name starts with `iqb_` in this
package is an independent extension module you may want
to optionally load for writing IQB based scripts.
"""
