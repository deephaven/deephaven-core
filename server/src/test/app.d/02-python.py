import jpy
from deephaven import TableTools
from typing import Callable

ApplicationState = jpy.get_type("io.deephaven.appmode.ApplicationState")
def demo_app(app: ApplicationState):
  print("Running Strict App Demo.")
  size = 42
  app.setField("hello", TableTools.emptyTable(size))
  app.setField("world", TableTools.timeTable("00:00:01"))

def initialize(func: Callable[[ApplicationState], None]):
  app = jpy.get_type("io.deephaven.appmode.ApplicationContext").get()
  func(app)

initialize(demo_app)

# TODO (core#1134): Identify an ideal implicit field export pattern for python users.
def demo_implicit():
  print("Running Implicit Demo.")
  size_imp = 42
  global hello_imp; hello_imp = TableTools.emptyTable(size_imp)
  global world_imp; world_imp = TableTools.timeTable("00:00:01")

def initialize_implicitly(func: Callable[[], None]):
  app = jpy.get_type("io.deephaven.appmode.ApplicationContext").get()
  original_fields = dict(globals())
  candidate_fields = dict(original_fields)
  exec(func.__code__, candidate_fields)
  for key, value in candidate_fields.items():
    if not key in original_fields.keys() or original_fields[key] != value:
        print("Found Key ", key)

initialize_implicitly(demo_implicit)