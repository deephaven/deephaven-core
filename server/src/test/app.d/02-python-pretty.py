import jpy
from deephaven import TableTools
from deephaven import ApplicationContext, ApplicationState

def demo_app(app: ApplicationState):
  print("Running Strict App Demo.")
  size = 42
  app.setField("hello", TableTools.emptyTable(size))
  app.setField("world", TableTools.timeTable("00:00:01"))

ApplicationContext.initialize(demo_app)

# TODO (core#1134): Identify an ideal implicit field export pattern for python users.
def demo_implicit():
  print("Running Implicit Demo.")
  size_imp = 42
  hello_imp = TableTools.emptyTable(size)
  world_imp = TableTools.timeTable("00:00:01")

ApplicationContext.initialize_implicitly(demo_implicit)