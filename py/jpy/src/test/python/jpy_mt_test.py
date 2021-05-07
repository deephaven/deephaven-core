import threading
import unittest
import jpyutil

jpyutil.init_jvm(jvm_maxmem='512M')
import jpy


class MyThread(threading.Thread):

    def __init__(self, value):
        threading.Thread.__init__(self)
        Integer = jpy.get_type('java.lang.Integer')
        self.intObj = Integer(value)

    def run(self):
        # perform some operation on the local object using a new thread
        self.intValue = self.intObj.intValue()


class TestMultipleThreads(unittest.TestCase):


    def test_multi_thread_access(self):

        t1 = MyThread(123)
        t2 = MyThread(234)
        t3 = MyThread(345)
        t4 = MyThread(456)

        t1.start()
        t2.start()
        t3.start()
        t4.start()

        t1.join()
        t2.join()
        t3.join()
        t4.join()

        self.assertEqual(123, t1.intValue)
        self.assertEqual(234, t2.intValue)
        self.assertEqual(345, t3.intValue)
        self.assertEqual(456, t4.intValue)


if __name__ == '__main__':
    print('\nRunning ' + __file__)
    unittest.main()
