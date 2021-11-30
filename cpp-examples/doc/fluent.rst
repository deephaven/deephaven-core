The Fluent Interface
====================

The Deephaven client has numerous methods that take expressions (e.g.
:cpp:func:`TableHandle::select <deephaven::client::highlevel::TableHandle::select>` or
:cpp:func:`TableHandle::view <deephaven::client::highlevel::TableHandle::view>`),
boolean conditions (e.g.
:cpp:func:`TableHandle::where <deephaven::client::highlevel::TableHandle::where>`),
column names (e.g.
:cpp:func:`TableHandle::sort <deephaven::client::highlevel::TableHandle::sort>`),
and so on.  These methods generally come in two flavors: a string version and a more
structured *typed* version. The reason both flavors exist
is because the string versions are convenient and simple to use for small programs, whereas the
typed versions are typically more maintainable in larger programs.

Consider these two ways of doing a
:cpp:func:`TableHandle::where <deephaven::client::highlevel::TableHandle::where>`,
using literal strings versus using the "fluent" syntax.

.. code:: c++

  auto table = tableManager.fetchTable("trades");
  auto filtered1 = table.where("ImportDate == `2017-11-01` && Ticker == `AAPL`");
  
  auto (importDate, ticker) = table.getColumns<StrCol, StrCol>("ImportDate", "Ticker");
  var filtered2 = table.where(importDate == "2017-11-01" && ticker == "AAPL");

The advantage of the ``filtered1`` query is that it is simple and compact.  On the other hand,
the advantage of the ``filtered2`` query is that it is able to do much more error checking
at compile time. Consider the following query syntax errors:

.. code:: c++

  // typo in Ticker
  auto filtered1 = table.where(
      "ImportDate == `2017-11-01` && Thicker == `AAPL`");
  // nonsensical string multiplication
  auto filtered1 = table.where(
      "ImportDate == `2017-11-01` && Ticker * 12 == `AAPL`");
  // extra closing parenthesis
  auto filtered1 = table.Where(
      "(ImportDate == `2017-11-01`) && (Ticker == `AAPL`))");

Because the code is using the literal string syntax, these errors would not be caught until the
server attempted to parse and execute them. However, none of the corresponding fluent versions
will even *compile*!

.. code:: c++

  auto (importDate, ticker) =
      table.getColumns<StrCol, StrCol>("ImportDate", "Ticker");
  // typo in Ticker
  auto filtered2 = table.where(
      importDate == "2017-11-01" && thicker == "AAPL");
  // nonsensical string multiplication
  auto filtered2 = table.where(
      importDate == "2017-11-01" && ticker * 12 == "AAPL");
  // extra closing parenthesis
  auto filtered2 = table.where(
      (importDate == "2017-11-01") && (ticker * 12) == "AAPL"));


How the fluent syntax works
---------------------------

The fluent syntax uses certain C++ types along with operator overloading to build up an abstract
syntax tree of your expression on the client side. Then, library methods pass that tree to the
server to be executed. Because the fluent syntax is built on top of C++ syntax, it needs to be legal
according to the rules of C++. One advantage of following C# syntax is that many potential errors are
caught at compile time, or even sooner, e.g. by the programmer's IDE.

Consider the following code fragment:

.. code:: c++

  auto (a, b, c, d) =
      table.getColumns<NumCol, NumCol, NumCol, NumCol>("A", "B", "C", "D");
  auto filtered = table.where(a + b + c <= d);

The transformation of the expression into an abstract syntax tree is done automatically by the
compiler. Basically, infix operators like ``+`` and ``<=`` are transformed into method calls, and
certain implicit type conversions are performed. Below is a sketch of the equivalent code after the
infix operators are transformed to method calls:

.. code:: c++

  NumericExpression temp1 = operator+(a, b);
  NumericExpression temp2 = operator+(temp1, c);
  BooleanExpression temp3 = operator<=(temp2, d);


Building expressions with the fluent syntax
-------------------------------------------

The fluent syntax is designed to capture the kinds of "natural" expressions one would write in
a programming language. Rather than formally describing the syntax here, we instead provide an
informal description.

There are basically four kinds of expressions in the system:
:cpp:class:`NumericExpression <deephaven::client::highlevel::NumericExpression>`,
:cpp:class:`StringExpression <deephaven::client::highlevel::StringExpression>`,
:cpp:class:`DateTimeExpression <deephaven::client::highlevel::DateTimeExpression>`, and
:cpp:class:`BooleanExpression <deephaven::client::highlevel::BooleanExpression>`.
These model the four types of expressions we want to represent in the system.

In typical usage, client programs do not explicitly declare variables of these types. Instead,
these objects are created as anonymous temporaries (as the intermediate results of overloaded
operators) which are then consumed by other operators or by Deephaven methods like
:cpp:func:`TableHandle::select <deephaven::client::highlevel::TableHandle::select>` or
:cpp:func:`TableHandle::where <deephaven::client::highlevel::TableHandle::where>`.

Local vs Remote Evaluation
--------------------------

Because the fluent syntax interoperates with ordinary C++ expression syntax, it might not be readily
apparent which parts of a complicated C++ expression are executed locally on the client machine,
and which parts are participating in an expression tree to be evaluated on the server. Generally,
the rules are:

Evaluated locally
^^^^^^^^^^^^^^^^^

* Numeric literals
* Variables
* Method calls
* Unary and binary operators involving the above

Evaluated at the server
^^^^^^^^^^^^^^^^^^^^^^^

* Column terminals
* Local values implicitly converted into Fluent values
* Unary operators, binary operators, and certain special methods involving Fluent expressions

Note that both of these definitions are intentionally recursive in nature. Also note that when
one of the arguments to a binary operator is a Fluent expression, the other argument will be
implicitly converted to a Fluent expression.

Consider the following examples:

.. code:: c++

  auto table = tableManager.fetchTable("trades");
  auto (importDate, ticker, close) =
      table.GetColumns<StrCol, StrCol, NumCol>("ImportDate", "Ticker", "Close");
  auto t0 = table.where(importDate == "2017-11-01" && ticker == "AAPL");

  var x = 1;

  int myFunc(int arg)
  {
      return arg + 10;
  }

  // Equivalent Deephaven Code Studio expression is "Result = 100 + Close"
  var t1a = t0.select((100 + close).as("Result"));
  // Equivalent Deephaven Code Studio expression is "Result = 300 + Close"
  var t2a = t0.select((100 + 200 + close).as("Result"));
  // Equivalent Deephaven Code Studio expression is "Result = 101 + Close"
  var t3a = t0.select((100 + x + close).as("Result"));
  // Equivalent Deephaven Code Studio expression is "Result = 111 + Close"
  var t4a = t0.select((100 + myFunc(x) + close).as("Result"));

A binary operator with at least one
:cpp:class:`NumericExpression <deephaven::client::highlevel::NumericExpression>`
yields a
:cpp:class:`NumericExpression <deephaven::client::highlevel::NumericExpression>`.
Because binary operators like left-to-right associativity, mathematically equivalent
but differently-ordered expressions get sent to the server as a different tree:

.. code:: c++

  // Equivalent Deephaven Code Studio expression is "Result = Close + 100"
  auto t1b = t0.select((close + 100).as("Result"));
  // Equivalent Deephaven Code Studio expression is "Result = (Close + 100) + 200"
  auto t2b = t0.select((close + 100 + 200).as("Result"));
  // Equivalent Deephaven Code Studio expression is "Result = (Close + 100) + 1"
  auto t3b = t0.select((close + 100 + x).as("Result"));
  // Equivalent Deephaven Code Studio expression is "Result = (Close + 100) + 11"
  auto t4b = t0.Select((close + 100 + myFunc(x)).as("Result"));

Note that the library is does *not* collapse `(Close + 100) + 11` into the mathematically-equivalent
`(Close + 111)`. This difference is largely of academic interest, because the final result is the
same due to the commutative property of addition. It would probably matter only in cases of numeric
over/underflow.

Building Fluent Expressions
---------------------------

In more advanced use cases, users may want to write methods that derive fluent expressions from
other fluent expressions. Some programming languages call such methods "combinators".  In
this simple example we write an ``add5`` function that yields the fluent expression ``e + 5`` for
whatever expression ``e`` is passed into it:

.. code:: c++

  NumericExpression add5(NumericExpression e)
  {
      return e + 5;
  }

  // Equivalent Deephaven Code Studio expression is "Result = (Close * Volume) + 5"
  auto t1 = t0.select(add5(close * volume).as("Result"));

NumericExpression
^^^^^^^^^^^^^^^^^

:cpp:class:`NumericExpression <deephaven::client::highlevel::NumericExpression>`
objects are either ``Numeric terminals`` or the result of an operator applied to
some combination of `Numeric terminals` and
:cpp:class:`NumericExpression <deephaven::client::highlevel::NumericExpression>` objects.

``Numeric terminals`` are:

* C# numeric literals of various primitive types such as ``3`` and ``-8.2``
* Client-side numeric variables such as `int x`` or ``double x``
* Client-side numeric expressions such as ``x * 2 + 5``
* Numeric columns, which are typically obtained from a call like
  :cpp:func:`getCols <deephaven::client::highlevel::TableHandle::getCols>`.

The operators are the the usual unary arithmetic operators ``+``, ``-``, ``~``, and
the usual binary operators ``+``, ``-``, ``*``, ``/``, ``%``, ``&``, ``|``, ``^``.

In this example, the table ``t1`` contains two columns: the ``Ticker`` column and a ``Result``
columns which holds the product ``Price * Volume + 12``. Notice that in a
:cpp:func:`TableHandle::select <deephaven::client::highlevel::TableHandle::select>`
statement, when we are creating a new column that is the result of a calculation, we need to give that new column
a name (using the
:cpp:func:`Expression::as <deephaven::client::highlevel::Expression::as>`
method).
In general, the fluent syntax ``expr.as("X")``
corresponds to Deephaven Code Studio expression ``X = expr``.

.. code:: c++

  auto table = tableManager.fetchTable("trades");
  auto (importDate, ticker, close, volume) =
      table.getColumns<StrCol, StrCol, NumCol, NumCol>("ImportDate", "Ticker",
      "Close", "Volume");
  auto t0 = table.where(importDate == "2017-11-01" && ticker == "AAPL");
  auto t1 = t0.select(ticker, (close * volume).As("Result"));
  // string literal equivalent
  auto t1_literal = t0.Select("Ticker", "Result = Close * Volume");

StringExpression
^^^^^^^^^^^^^^^^

:cpp:class:`StringExpression <deephaven::client::highlevel::StringExpression>`
objects are either ``String terminals``
or the result of the `+` operator applied to some combination of ``String terminals`` and
:cpp:class:`StringExpression <deephaven::client::highlevel::StringExpression>` objects.

``String terminals`` are:

* C++ numeric literals like ``"hello"``.
* Client-side string variables such as ``string x``.
* Client-side string expressions such as ``x + "QQQ"``
* String columns, which are typically obtained from a call like
  :cpp:func:`getCols <deephaven::client::highlevel::TableHandle::getCols>`.

Example:

.. code:: c++

  auto t2 = t0.select(ticker, (ticker + "XYZ").as("Result"));
  auto t2_literal = t0.select("Ticker", "Result = Ticker + `XYZ`");

:cpp:class:`StringExpression <deephaven::client::highlevel::StringExpression>`
provides four additional methods that work on
:cpp:class:`StringExpression <deephaven::client::highlevel::StringExpression>`
objects. These operations have the semantics described in the Deephaven documentation, and they yield
:cpp:class:`BooleanExpression <deephaven::client::highlevel::BooleanExpression>`
(described in the :ref:`BooleanExpression` subsection). For example:

.. code:: c++

  var t1 = t0.where(ticker.startsWith("AA"));
  var t1_literal = t0.where("ticker.startsWith(`AA`)");
  var t2 = t0.where(ticker.matches(".*P.*"));
  var t2_literal = t0.where("ticker.matches(`.*P.*`)");

DateTimeExpression
^^^^^^^^^^^^^^^^^^

`DateTime terminals` are:

* C++ string literals, variables or string expressions in Deephaven
  :cpp:class:`StringExpression <deephaven::client::highlevel::DateTime>`
  format, e.g. ``"2020-03-01T09:45:00.123456 NY"``.
* Client-side variables/expressions of type
  :cpp:class:`StringExpression <deephaven::client::highlevel::DateTime>`

:cpp:class:`StringExpression <deephaven::client::highlevel::DateTime>`
is the standard Deephaven Date/Time type, representing nanoseconds since January 1, 1970 UTC.

.. _BooleanExpression:

BooleanExpression
^^^^^^^^^^^^^^^^^

:cpp:class:`BooleanExpression <deephaven::client::highlevel::BooleanExpression>`
objets can be used to represent expressions involving boolean-valued columns (e.g.
``!boolCol1 || boolCol2``) but more commonly, they are used to represent the result of
relational operators applied to other expression types.
:cpp:class:`BooleanExpression <deephaven::client::highlevel::BooleanExpression>` objects
support the unary ``!``, as well as the binary operators ``&&`` and ``||`` and their cousins
``&`` and ``|``.

Note that the shortcutting operators ``&&`` and ``||`` do not exhibit their usual shortcutting behavior
when used with Deephaven fluent expressions. Because the value of either side of the expression isn't
knowable until it is evaluated at the server, it is not possible (nor even particularly meaningful)
to do shortcutting on the client.
As a consequence of this, ``&&`` is a synonym for the (non-shortcutting) boolean ``&`` operator; likewise
``||`` is a synonym for the non-shortcutting boolean ``|`` operator.

For example, in ``t1 = t0.where(col0 < 5 && col1 > 12)`` we would send the whole expression to
the server for evaluation. There would be no attempt to first determine the "truth" of
``col0 < 5`` (a concept that doesn't even make much sense anyway in the context of a full column of
data) in order to try shortcut the evaluation of ``col1 > 12``.

This example creates two boolean-valued columns and does simplistic filtering on them:

.. code:: c++

  // TODO(kosak): This example doesn't work yet. Need BoolCol and boolean literals
  auto empty = manager.emptyTable(5, {}, {});
  auto t = empty.update( ((BooleanExpression)true).as("A"),
      ((BooleanExpression)false).as("B"));
  // Deephaven Code Studio equivalent
  auto t_literal = empty.Update("A = true", "B = false");
  auto (a, b) = t.GetColumns<BoolCol, BoolCol>("A", "B");
  auto t2 = t.where(a);
  auto t3 = t.where(a && b);

More commonly,
:cpp:class:`BooleanExpression <deephaven::client::highlevel::BooleanExpression>`  
are created as the result of relational operators on other expressions. For example we might say

.. code:: c++

  std::vector<int> aValues{10, 20, 30};	  
  std::vector<std::string> sValues{"x", "y", "z"};
  TableMaker tm;
  tm.addColumn("A", aValues);
  tm.addColumn("S", sValues);
  auto temp = tm.makeTable(manager);
  auto a = temp.getNumCol("A");
  auto result = temp.where(a > 15);

Here ``a > 15`` applies the ``>`` operator to two
:cpp:class:`NumericExpression <deephaven::client::highlevel::NumericExpression>` objects
yielding a
:cpp:class:`BooleanExpression <deephaven::client::highlevel::BooleanExpression>`  
suitable for passing to the
:cpp:func:`TableHandle <deephaven::client::highlevel::TableHandle::where>`  
method and being evaluated on the server. The library supports the usual relational
operators (``<``, ``<=``, ``==``, ``>=``, ``>``, ``!=``) on
:cpp:class:`NumericExpression <deephaven::client::highlevel::NumericExpression>`,
:cpp:class:`StringExpression <deephaven::client::highlevel::StringExpression>`, and
:cpp:class:`DateTimeExpression <deephaven::client::highlevel::DateTimeExpression>`; meanwhile
:cpp:class:`BooleanExpression <deephaven::client::highlevel::BooleanExpression>`
itself supports only ``==`` and ``!=``.

Column Terminals
^^^^^^^^^^^^^^^^

A Column Terminal is used to represent a database column symbolically, so it can be used in a
fluent invocation such as ``t.where(a > 5)``. 
To do this, the program needs to know the name of
the database column (in this example, "A") as well as its type (in this example,
:cpp:class:`NumCol <deephaven::client::highlevel::NumCol>`).

.. code:: c++

  auto a = temp.getCol<NumCol>("A");

The Column Terminal types are:

* :cpp:class:`NumCol <deephaven::client::highlevel::NumCol>`
* :cpp:class:`StrCol <deephaven::client::highlevel::StrCol>`
* :cpp:class:`DateTimeCol <deephaven::client::highlevel::DateTimeCol>`
* :cpp:class:`BoolCol <deephaven::client::highlevel::BoolCol>`

Note that the single fluent type
:cpp:class:`NumCol <deephaven::client::highlevel::NumCol>`
stands in for all the
numeric types (``short``, ``int``, ``double``, and so on). This does *not* mean that the server represents
all these types as the same thing, or that there is some kind of loss of precision involved. Rather
it is simply a reflection of the fact that the numeric types generally interoperate with each other
and support all the same operators; from the point of view of the fluent layer, when building an
abstract syntax tree for an expression like ``x + y`` for evaluation at the server, it's not necessary
to know the exact types of ``x`` and ``y`` at this point, other than knowing that they behave like
numbers.

The syntax for creating a single Column Terminal is

.. code:: c++

  auto col = table.getXXX(name);

where ``getXXX`` is one of `
:cpp:func:`getNumCol <deephaven::client::highlevel::TableHandle::getNumCol>`,
:cpp:func:`getStrCol <deephaven::client::highlevel::TableHandle::getStrCol>`,
:cpp:func:`getDateTimeCol <deephaven::client::highlevel::TableHandle::getDateTimeCol>`,
or     
:cpp:func:`getBoolCol <deephaven::client::highlevel::TableHandle::getBoolCol>`,
and ``name`` is the name of the column.

To conveniently bind more than one column at a time, the program can use
:cpp:func:`getCols <deephaven::client::highlevel::TableHandle::getCols>`.

For example this statement binds three columns at once:

.. code:: c++

  auto (importDate, ticker, close) =
    table.getCols<StrCol, StrCol, NumCol>("ImportDate", "Ticker", "Close");

SelectColumns
^^^^^^^^^^^^^

A
:cpp:class:`SelectColumn <deephaven::client::highlevel::SelectColumn>`
is an object suitable to be passed to a
:cpp:func:`select <deephaven::client::highlevel::TableHandle::select>`,
:cpp:func:`update <deephaven::client::highlevel::TableHandle::update>`,
:cpp:func:`view <deephaven::client::highlevel::TableHandle::view>`, or
:cpp:func:`updateView <deephaven::client::highlevel::TableHandle::updateView>`
method. It either needs to either refer to an already-existing column,
or it is an expression bound to a column name, which will cause a new column
to be created. Examples:

.. code:: c++

  // Assume "close" is already a column, so we can use it directly
  auto t1 = t0.select(close);
  // "100 + close" is an expression; to turn it into a SelectColumn
  // we need to bind it to a new column name with the "as" method.
  auto t2 = t0.select((100 + close).as("Result"));
  // The above would be expressed in the Deephaven Code Studio as:
  var t2_literal = t0.select("Result = 100 + Close")
