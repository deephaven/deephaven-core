#
# Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
#

import sys
import argparse
import xmlrunner  # not part of the standard library, installed via pip (or the like)

import init

if sys.version_info[0] < 3:
    import unittest2 as unittest  # not part of the standard library, installed via pip (or the like)
else:
    import unittest

from testCalendars import TestCalendars  # pass 2.7 & 3.7
from testAggregation import TestAggregation # pass 2.7 & 3.7
from testDateTimeUtil import TestDateTimeUtil  # missing method - updated jar should solve
from testPlot import TestPlot
from testFigureWrapper import TestFigureWrapper  # axis methods - how to resolve
from testTableTools import TestTableTools  # bad call signature inspection
from testTableDataframeConversion import TestTableDataframeConversion  # pass 2.7 & 3.7
from testParquetTools import TestParquetTools
from testConsumeKafka import TestConsumeKafka
from testProduceKafka import TestProduceKafka


# add specific arguments for testing, including the arguments for jvm initialization
parser = argparse.ArgumentParser(parents=[init.initParser, ])

parser.add_argument("-o", "--output", default=None,
                    help="path for output location of junit style xml associated with each test. "
                         "Test results wil simply print to the console if this is unset.")

parser.add_argument("--tests", default=None, help="If desired, provide comma-delimited list of tests to run. "
                                                  "Valid entry should be the case-insensitive string <entry> "
                                                  "for indicate test class Test<entry>.")


if __name__ == "__main__":
    args = parser.parse_args()
    init.main(args)

    if args.output is not None:
        # set up the runner which will produce junit sytle xml files...
        runner = xmlrunner.XMLTestRunner(output=args.output, verbosity=2)
    else:
        # run only on the console
        runner = unittest.TextTestRunner(verbosity=2)

    if args.tests is None:
        theTests = None
    else:
        theTests = [el.lower().strip() for el in args.tests.strip().split(',')]
        print("Limiting testing to tests {}".format(theTests))


    goodClasses = {
        'Cals'.lower(): TestCalendars,
        'AGG'.lower(): TestAggregation,
        'DTU'.lower(): TestDateTimeUtil,
        'Plot'.lower(): TestPlot,
        'FigW'.lower(): TestFigureWrapper,
        'TTools'.lower(): TestTableTools,
        'TConv'.lower(): TestTableDataframeConversion,
        'TPqt'.lower(): TestParquetTools,
        'TCK'.lower(): TestConsumeKafka,
        'TPK'.lower(): TestProduceKafka
    }

    suite = []

    if theTests is None:
        for cls in goodClasses.values():
            tests = unittest.TestLoader().loadTestsFromTestCase(cls)
            suite.append(tests)
    else:
        for testName in theTests:
            cls = goodClasses.get(testName, None)
            if cls is None:
                print("Found no tests for name = {}".format(testName))
            else:
                suite.append(unittest.TestLoader().loadTestsFromTestCase(cls))
    # Now, run the tests
    runner.run(unittest.TestSuite(suite))
