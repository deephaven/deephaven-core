class SomeClass:
  def __init__(self):
    self.closed = False

  def get_closed(self):
    return self.closed

  def close(self):
    self.closed = True