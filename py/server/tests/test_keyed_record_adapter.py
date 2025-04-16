#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
import unittest
from typing import List

import jpy

from deephaven import new_table
from deephaven.column import *
from deephaven.constants import *
from deephaven.experimental import *
from tests.testbase import BaseTestCase

_JTableTools = jpy.get_type("io.deephaven.engine.util.TableTools")


class KraTestCase(BaseTestCase):

    def testCustomKeyedRecordAdapterWithOneStrKeyCol(self):
        # source = Table(_JTstTools.testRefreshingTable(
        #     _JTstTools.i(2, 4).copy().toTracking(),
        #     _JTableTools.intCol("Sym", 'AAPL', None),
        #     _JTableTools.doubleCol("Price", 1.1, NULL_DOUBLE),
        #     _JTableTools.longCol("Size", 10_000_000_000, NULL_LONG),
        #     _JTableTools.col("Exch", "ARCA", None),
        # ))

        source = new_table([
            string_col("Sym", ['AAPL', None]),
            double_col("Price", [1.1, NULL_DOUBLE]),
            long_col("Size", [10_000_000_000, NULL_LONG]),
            string_col("Exch", ["ARCA", None]),
        ])
        _JTableTools.show(source.j_table)

        class StockTrade:
            sym: str
            price: float
            size: int
            exch: str

            def __init__(self, sym: str, price: float, size: int, exch: str):
                self.sym = sym
                self.price = price
                self.size = size
                self.exch = exch

        keyed_record_adapter: KeyedRecordAdapter[str, StockTrade] = KeyedRecordAdapter(
            source,
            StockTrade,
            'Sym',
            ["Price", "Size", "Exch"]
        )

        records = keyed_record_adapter.get_records(["AAPL", None])

        self.assertEquals(dict, type(records))
        self.assertEquals(2, len(records))

        aapl_trade = records['AAPL']
        self.assertEquals('AAPL', aapl_trade.sym)
        self.assertEquals(1.1, aapl_trade.price)
        self.assertEquals(10_000_000_000, aapl_trade.size)
        self.assertEquals('ARCA', aapl_trade.exch)

    def testCustomKeyedRecordAdapterWithOneIntKeyCol(self):
        # source = Table(_JTstTools.testRefreshingTable(
        #     _JTstTools.i(2, 4).copy().toTracking(),
        #     _JTableTools.intCol("Sym", 'AAPL', None),
        #     _JTableTools.doubleCol("Price", 1.1, NULL_DOUBLE),
        #     _JTableTools.longCol("Size", 10_000_000_000, NULL_LONG),
        #     _JTableTools.col("Exch", "ARCA", None),
        # ))

        source = new_table([
            long_col("Id", [0, NULL_LONG]),
            double_col("Price", [1.1, NULL_DOUBLE]),
            long_col("Size", [10_000_000_000, NULL_LONG]),
            string_col("Exch", ["ARCA", None]),
        ])
        _JTableTools.show(source.j_table)

        class StockTradeById:
            id: int
            price: float
            size: int
            exch: str

            def __init__(self, id: int, price: float, size: int, exch: str):
                self.id = id
                self.price = price
                self.size = size
                self.exch = exch

        keyed_record_adapter: KeyedRecordAdapter[int, StockTradeById] = KeyedRecordAdapter(
            source,
            StockTradeById,
            'Id',
            ["Price", "Size", "Exch"]
        )

        records = keyed_record_adapter.get_records([0, NULL_LONG])

        self.assertEquals(dict, type(records))
        self.assertEquals(2, len(records))

        aapl_trade = records[0]
        self.assertEquals(0, aapl_trade.id)
        self.assertEquals(1.1, aapl_trade.price)
        self.assertEquals(10_000_000_000, aapl_trade.size)
        self.assertEquals('ARCA', aapl_trade.exch)

    def testCustomKeyedRecordAdapterWithStrIntKeyCols(self):
        # source = Table(_JTstTools.testRefreshingTable(
        #     _JTstTools.i(2, 4).copy().toTracking(),
        #     _JTableTools.intCol("Sym", 'AAPL', 'AAPL', None),
        #     _JTableTools.doubleCol("Price", 1.1, 1.2, NULL_DOUBLE),
        #     _JTableTools.longCol("Size", 10_000_000_000, 1_000, NULL_LONG),
        #     _JTableTools.col("Exch", "ARCA", 'NASDAQ', None),
        # ))

        source = new_table([
            string_col("Sym", ['AAPL', 'AAPL', None]),
            double_col("Price", [1.1, 1.2, NULL_DOUBLE]),
            long_col("Size", [10_000_000_000, 1_000, NULL_LONG]),
            int_col("Exch", [0, 1, NULL_INT]),
        ])
        _JTableTools.show(source.j_table)

        class StockTradeIntExchange:
            sym: str
            exch: int
            price: float
            size: int

            def __init__(self, sym: str, exch: int, price: float, size: int, ):
                self.sym = sym
                self.exch = exch
                self.price = price
                self.size = size

        keyed_record_adapter: KeyedRecordAdapter[
            List[str], StockTradeIntExchange] = make_record_adapter_with_constructor(
            source,
            StockTradeIntExchange,
            ['Sym', 'Exch'],
            ["Price", "Size"]
        )

        records = keyed_record_adapter.get_records([
            ["AAPL", 0],
            ["AAPL", 1],
            ["AAPL", NULL_INT],
        ])

        self.assertEquals(list, type(records))
        self.assertEquals(3, len(records))

        aapl_trade = records[0]
        self.assertEquals('AAPL', aapl_trade.sym)
        self.assertEquals(1.1, aapl_trade.price)
        self.assertEquals(10_000_000_000, aapl_trade.size)
        self.assertEquals(0, aapl_trade.exch)

        aapl_trade = records[1]
        self.assertEquals('AAPL', aapl_trade.sym)
        self.assertEquals(1.2, aapl_trade.price)
        self.assertEquals(1_000, aapl_trade.size)
        self.assertEquals(1, aapl_trade.exch)

        # TODO: this key doesn't exist -- should it be an exception or None? Also what does Java do?
        null_trade = records[2]
        self.assertIsNone(null_trade)

    def testCustomKeyedRecordAdapterWithStrStrKeyCols(self):
        # source = Table(_JTstTools.testRefreshingTable(
        #     _JTstTools.i(2, 4).copy().toTracking(),
        #     _JTableTools.intCol("Sym", 'AAPL', 'AAPL', None),
        #     _JTableTools.doubleCol("Price", 1.1, 1.2, NULL_DOUBLE),
        #     _JTableTools.longCol("Size", 10_000_000_000, 1_000, NULL_LONG),
        #     _JTableTools.col("Exch", "ARCA", 'NASDAQ', None),
        # ))

        source = new_table([
            string_col("Sym", ['AAPL', 'AAPL', None]),
            double_col("Price", [1.1, 1.2, NULL_DOUBLE]),
            long_col("Size", [10_000_000_000, 1_000, NULL_LONG]),
            string_col("Exch", ["ARCA", 'NASDAQ', None]),
        ])
        _JTableTools.show(source.j_table)

        class StockTradeAlternateConstructor:
            sym: str
            exch: str
            price: float
            size: int

            def __init__(self, sym: str, exch: str, price: float, size: int, ):
                self.sym = sym
                self.exch = exch
                self.price = price
                self.size = size

        keyed_record_adapter: KeyedRecordAdapter[
            List[str], StockTradeAlternateConstructor] = make_record_adapter_with_constructor(
            source,
            StockTradeAlternateConstructor,
            ['Sym', 'Exch'],
            ["Price", "Size"]
        )

        records = keyed_record_adapter.get_records([
            ["AAPL", "ARCA"],
            ["AAPL", "NASDAQ"],
            ["AAPL", "BADKEY"],
        ])

        self.assertEquals(list, type(records))
        self.assertEquals(3, len(records))

        aapl_trade = records[0]
        self.assertEquals('AAPL', aapl_trade.sym)
        self.assertEquals(1.1, aapl_trade.price)
        self.assertEquals(10_000_000_000, aapl_trade.size)
        self.assertEquals('ARCA', aapl_trade.exch)

        aapl_trade = records[1]
        self.assertEquals('AAPL', aapl_trade.sym)
        self.assertEquals(1.2, aapl_trade.price)
        self.assertEquals(1_000, aapl_trade.size)
        self.assertEquals('NASDAQ', aapl_trade.exch)

        self.assertEquals(None, records[2])

    def testCustomKeyedRecordAdapterWithCompositeKeyColAndAdapter(self):
        # source = Table(_JTstTools.testRefreshingTable(
        #     _JTstTools.i(2, 4).copy().toTracking(),
        #     _JTableTools.intCol("Sym", 'AAPL', 'AAPL', None),
        #     _JTableTools.doubleCol("Price", 1.1, 1.2, NULL_DOUBLE),
        #     _JTableTools.longCol("Size", 10_000_000_000, 1_000, NULL_LONG),
        #     _JTableTools.col("Exch", "ARCA", 'NASDAQ', None),
        # ))

        source = new_table([
            string_col("Sym", ['AAPL', 'AAPL', None]),
            double_col("Price", [1.1, 1.2, NULL_DOUBLE]),
            long_col("Size", [10_000_000_000, 1_000, NULL_LONG]),
            string_col("Exch", ["ARCA", 'NASDAQ', None]),
        ])
        _JTableTools.show(source.j_table)

        class StockTrade:
            sym: str
            price: float
            size: int
            exch: str

            def __init__(self, sym: str, price: float, size: int, exch: str):
                self.sym = sym
                self.price = price
                self.size = size
                self.exch = exch

        # Function that just calls StockTrades() with args in correct order (Exch last; when calling
        # this method it is second arg because keys come first):
        def create_stock_trade(sym: str, exch: str, price: float, size: int):
            return StockTrade(sym, price, size, exch)

        keyed_record_adapter: KeyedRecordAdapter[List[str], StockTrade] = make_record_adapter(
            source,
            create_stock_trade,
            ['Sym', 'Exch'],
            ["Price", "Size"]
        )

        records = keyed_record_adapter.get_records([
            ["AAPL", "ARCA"],
            ["AAPL", "NASDAQ"],
            ["AAPL", "BADKEY"],
        ])

        self.assertEquals(list, type(records))
        self.assertEquals(3, len(records))

        aapl_trade = records[0]
        self.assertEquals('AAPL', aapl_trade.sym)
        self.assertEquals(1.1, aapl_trade.price)
        self.assertEquals(10_000_000_000, aapl_trade.size)
        self.assertEquals('ARCA', aapl_trade.exch)

        aapl_trade = records[1]
        self.assertEquals('AAPL', aapl_trade.sym)
        self.assertEquals(1.2, aapl_trade.price)
        self.assertEquals(1_000, aapl_trade.size)
        self.assertEquals('NASDAQ', aapl_trade.exch)

        self.assertEquals(None, records[2])


if __name__ == '__main__':
    unittest.main()
