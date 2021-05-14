import unittest
import pkgutil

#import importlib
# upgrade to python 3 eventually https://stackoverflow.com/questions/14050281/how-to-check-if-a-python-module-exists-without-importing-it
class TestPackaging(unittest.TestCase):

  def test_find_deephaven(self):
    spec = pkgutil.find_loader("deephaven")
    self.assertTrue(spec is not None)

if __name__ == '__main__':
  unittest.main()
