#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys
import jpy

from deephaven import TableTools, Aggregation

_JTWD = jpy.get_type("io.deephaven.engine.table.impl.TableWithDefaults")
_JAggHolder = jpy.get_type("io.deephaven.engine.table.impl.TableWithDefaults$AggHolder")

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestAggregation(unittest.TestCase):
    """
    Test cases for the deephaven.AggregationFactory module (performed locally) -
    """

    def testAggMethods(self):
        # create a silly table
        tab = TableTools.emptyTable(10)
        tab = tab.update("dumb=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)")

        # try the various aggregate methods - just a coverage test
        _JTWD.aggBy(tab, _JAggHolder(*[
            Aggregation.AggGroup("aggGroup=var"),
            Aggregation.AggAvg("aggAvg=var"),
            Aggregation.AggCount("aggCount"),
            Aggregation.AggFirst("aggFirst=var"),
            Aggregation.AggLast("aggLast=var"),
            Aggregation.AggMax("aggMax=var"),
            Aggregation.AggMed("aggMed=var"),
            Aggregation.AggMin("aggMin=var"),
            Aggregation.AggPct(0.20, "aggPct=var"),
            Aggregation.AggStd("aggStd=var"),
            Aggregation.AggSum("aggSum=var"),
            Aggregation.AggAbsSum("aggAbsSum=var"),
            Aggregation.AggVar("aggVar=var"),
            Aggregation.AggWAvg("var", "weights")]), "dumb")
        # TODO: AggFormula - this is terrible
        del tab
