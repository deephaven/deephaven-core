Top-level class: Client
=======================

The main entry point to the Deephaven Client API. Use
:cpp:func:`connect <deephaven::client::highlevel::Client::connect>`
to connect to a Deephaven instance, then call
:cpp:func:`getManager <deephaven::client::highlevel::Client::getManager>`
to get a
:cpp:class:`TableHandleManager <deephaven::client::highlevel::TableHandleManager>`

.. doxygenclass:: deephaven::client::highlevel::Client
   :members:
