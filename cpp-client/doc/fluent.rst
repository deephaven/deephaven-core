The Fluent interface
====================
These classes participate in the various method chaining and operator
overloading techniques used for the Deephaven C++ Client's fluent interface.

Many of these classes are not named explicitly in client code. Rather they
just exist as intermediate values or as the result of various overloaded
operators. Consider the following code fragment:

.. code:: c++

   TableHandle table = ...;
   auto [symbol, price] = table.getCols<StrCol, NumCol>("Symbol", "Price");
   auto t2 = t1.where(symbol == "AAPL" && price < 50.0).tail(5);

In this code, the fluent expression ``symbol == "AAPL" && price < 50.0``
happens to have the type
:cpp:class:`BooelanExpression <deephaven::client::BooleanExpression>`.
However that type is not explicitly spelled out in the code and many programs
would never need to explicitly name that type. Rather, most programs start
with a
:cpp:class:`TableHandle <deephaven::client::TableHandle>`
and various column types (
e.g. :cpp:class:`StrCol <deephaven::client::StrCol>`,
:cpp:class:`NumCol <deephaven::client::NumCol>`,
and :cpp:class:`DateTimeCol <deephaven::client::DateTimeCol>`)
and perform various method calls and overloaded operators on them.

.. toctree::

    fluent_columns
    fluent_expressions
