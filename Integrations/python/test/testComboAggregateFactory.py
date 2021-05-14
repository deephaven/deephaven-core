#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

##############################################################################
# NOTE: the jvm should have been initialized, or this test will certainly fail
##############################################################################

import sys

from deephaven import TableTools, ComboAggregateFactory

if sys.version_info[0] < 3:
    import unittest2 as unittest
    # not part of the standard library, installed via pip (or the like)
    # it provides backward compatibility with python3 style subTest context manager (handy for complex tests)
else:
    import unittest


class TestComboAggregateFactory(unittest.TestCase):
    """
    Test cases for the deephaven.ComboAggregateFactory module (performed locally) -
    """

    def testAggMethods(self):
        # create a silly table
        tab = TableTools.emptyTable(10)
        tab = tab.update("dumb=(int)(i/5)", "var=(int)i", "weights=(double)1.0/(i+1)")

        # try the various aggregate methods - just a coverage test
        tab.by(ComboAggregateFactory.AggCombo(
            ComboAggregateFactory.AggArray("aggArray=var"),
            ComboAggregateFactory.AggAvg("aggAvg=var"),
            ComboAggregateFactory.AggCount("aggCount"),
            ComboAggregateFactory.AggFirst("aggFirst=var"),
            ComboAggregateFactory.AggLast("aggLast=var"),
            ComboAggregateFactory.AggMax("aggMax=var"),
            ComboAggregateFactory.AggMed("aggMed=var"),
            ComboAggregateFactory.AggMin("aggMin=var"),
            ComboAggregateFactory.AggPct(0.20, "aggPct=var"),
            ComboAggregateFactory.AggStd("aggStd=var"),
            ComboAggregateFactory.AggSum("aggSum=var"),
            ComboAggregateFactory.AggAbsSum("aggAbsSum=var"),  # TODO: add this in...
            ComboAggregateFactory.AggVar("aggVar=var"),
            ComboAggregateFactory.AggWAvg("var", "weights")),  # how to specify the name of this column?
            "dumb")
        # TODO: AggFormula - this is terrible
        del tab

