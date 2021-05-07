import ast

class EnsureIsExpression(ast.NodeVisitor):
  """
  Ensure the node is an Expression node
  """
  def __init__(self):
    self.expression = None

  def visit_Expression(self, node):
    self.expression = node

  def generic_visit(self, node):
    raise Exception('Expecting an AST Expression node, is instead {}'.format(node))

class ExtractNamesVisitor(ast.NodeVisitor):
  """
  Extract all Name node ids recursively
  """

  def __init__(self):
    self.names = set()

  def visit_Name(self, node):
    # In Python 3, these nodes are of type NameConstant, and not Name, so they
    # won't be included. With Python 2 though, it's best to explicitly exclude
    # them.
    if not node.id in ["True", "False", "None"]:
      self.names.add(node.id)
    self.generic_visit(node)

def extract_expression_names(expression_str):
  """
  Extract names from an expression string

  :param expression_str: the python expression, of the form <expression>
  :return: a sorted list of the named elements from <expression>
  """
  node = ast.parse(expression_str, mode='eval')

  expression_visitor = EnsureIsExpression()
  expression_visitor.visit(node)

  # f(x[y]) + 2 - A * B
  names_visitor = ExtractNamesVisitor()
  names_visitor.visit(expression_visitor.expression)

  # [A, B, f, x, y]
  return sorted(list(names_visitor.names))
