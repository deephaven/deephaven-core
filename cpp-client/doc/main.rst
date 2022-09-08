Top-level class: Client
=======================

The main entry point to the Deephaven Client API. Use
:cpp:func:`connect <deephaven::client::Client::connect>`
to connect to a Deephaven instance, then call
:cpp:func:`getManager <deephaven::client::Client::getManager>`
to get a
:cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>`

.. doxygenclass:: deephaven::client::Client
   :members:
