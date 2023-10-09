class Parent:
  def __init__(self):
    pass

  def create_child(self, on_del):
    return Child(on_del)

class Child:
  def __init__(self, on_del):
    self.on_del = on_del

  def __del__(self):
    # this is a hacky workaround b/c JPY doesn't like to map java objects with __call__ as callable
    #self.on_del()
    self.on_del.__call__()