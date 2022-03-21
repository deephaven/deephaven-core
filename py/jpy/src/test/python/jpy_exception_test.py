# This file was modified by Deephaven Data Labs.
import unittest

import jpyutil
import string

jpyutil.init_jvm(jvm_maxmem='512M', jvm_classpath=['target/test-classes'])
import jpy

class TestExceptions(unittest.TestCase):
    def setUp(self):
        self.Fixture = jpy.get_type('org.jpy.fixtures.ExceptionTestFixture')
        self.assertIsNotNone(self.Fixture)


    def test_NullPointerException(self):
        fixture = self.Fixture()

        self.assertEqual(fixture.throwNpeIfArgIsNull("123456"), 6)

        with  self.assertRaises(RuntimeError, msg='Java NullPointerException expected') as e:
            fixture.throwNpeIfArgIsNull(None)
        self.assertEqual(str(e.exception), 'java.lang.NullPointerException')


    def test_ArrayIndexOutOfBoundsException(self):
        fixture = self.Fixture()
        self.assertEqual(fixture.throwAioobeIfIndexIsNotZero(0), 101)

        with self.assertRaises(RuntimeError, msg='Java ArrayIndexOutOfBoundsException expected') as e:
            fixture.throwAioobeIfIndexIsNotZero(1)
        exceptText = str(e.exception)
        if not exceptText == "java.lang.ArrayIndexOutOfBoundsException: Index 1 out of bounds for length 1":
            print("Except Text", exceptText)
            self.assertEqual(str(e.exception), 'java.lang.ArrayIndexOutOfBoundsException: 1')

        with self.assertRaises(RuntimeError, msg='Java ArrayIndexOutOfBoundsException expected') as e:
            fixture.throwAioobeIfIndexIsNotZero(-1)
        exceptText = str(e.exception)
        if not exceptText == "java.lang.ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 1":
            self.assertEqual(str(e.exception), 'java.lang.ArrayIndexOutOfBoundsException: -1')


    def test_RuntimeException(self):
        fixture = self.Fixture()
        fixture.throwRteIfMessageIsNotNull(None)

        with  self.assertRaises(RuntimeError, msg='Java RuntimeException expected') as e:
            fixture.throwRteIfMessageIsNotNull("Evil!")
        self.assertEqual(str(e.exception), 'java.lang.RuntimeException: Evil!')


    def test_IOException(self):
        fixture = self.Fixture()
        fixture.throwIoeIfMessageIsNotNull(None)

        with  self.assertRaises(RuntimeError, msg='Java IOException expected') as e:
            fixture.throwIoeIfMessageIsNotNull("Evil!")
        self.assertEqual(str(e.exception), 'java.io.IOException: Evil!')

    # Checking the exceptions for differences (e.g. in white space) can be a huge pain, this helps)
    #def hexdump(self, s):
    #    for i in xrange(0, len(s), 32):
    #        sl = s[i:min(i + 32, len(s))]
    #        fsl = map(lambda x : x if (x in string.printable and not x in "\n\t\r") else ".", sl)
    #        print "%08d %s %s %s" % (i, " ".join("{:02x}".format(ord(c)) for c in sl), ("   ".join(map(lambda x : "", xrange(32 - len(sl))))), sl)

    def test_VerboseException(self):
        fixture = self.Fixture()

        jpy.VerboseExceptions.enabled = True

        self.assertEqual(fixture.throwNpeIfArgIsNull("123456"), 6)

        with self.assertRaises(RuntimeError) as e:
            fixture.throwNpeIfArgIsNullNested(None)
        actualMessage = str(e.exception)
        expectedMessage = "java.lang.RuntimeException: Nested exception\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNullNested(ExceptionTestFixture.java:43)\ncaused by java.lang.NullPointerException\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNull(ExceptionTestFixture.java:32)\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNullNested(ExceptionTestFixture.java:41)\n"

        #self.hexdump(actualMessage)
        #self.hexdump(expectedMessage)
        #print [i for i in xrange(min(len(expectedMessage), len(actualMessage))) if actualMessage[i] != expectedMessage[i]]

        self.assertEquals(actualMessage, expectedMessage)

        with self.assertRaises(RuntimeError) as e:
            fixture.throwNpeIfArgIsNullNested3(None)
        actualMessage = str(e.exception)
        expectedMessage = "java.lang.RuntimeException: Nested exception 3\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNullNested3(ExceptionTestFixture.java:55)\ncaused by java.lang.RuntimeException: Nested exception\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNullNested(ExceptionTestFixture.java:43)\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNullNested2(ExceptionTestFixture.java:48)\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNullNested3(ExceptionTestFixture.java:53)\ncaused by java.lang.NullPointerException\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNull(ExceptionTestFixture.java:32)\n\tat org.jpy.fixtures.ExceptionTestFixture.throwNpeIfArgIsNullNested(ExceptionTestFixture.java:41)\n\t... 2 more\n"

        #self.hexdump(actualMessage)
        #self.hexdump(expectedMessage)
        #print [i for i in xrange(min(len(expectedMessage), len(actualMessage))) if actualMessage[i] != expectedMessage[i]]

        self.assertEquals(actualMessage, expectedMessage)

        jpy.VerboseExceptions.enabled = False


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
