import inspect
from sphinx.ext.autodoc import ClassDocumenter


class ExperimentalDocumenter(ClassDocumenter):
    # See: https://pydoc.dev/sphinx/latest/sphinx.ext.autodoc.ClassDocumenter.html?private=1

    objtype = 'table'
    directivetype = 'class'
    priority = 10  # Higher than ClassDocumenter's priority (10)

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        # handle functions in the deephaven.testdocs module that are inherited
        return (
                inspect.isfunction(member)
                and member.__module__ == 'deephaven.testdocs'
                and member.__qualname__.split('.')[0] != parent.object.__name__
        )

    def process_doc(self, docstrings):
        def class_details(member):
            return (member, member.__module__, member.__name__, member.__qualname__, member.__class__, member.__bases__,
                    dir(member))

        print(f"CALLING process_doc: {self.parent} {self.object} {docstrings}")
        print(f"PARENT: {class_details(self.parent)}")
        print(f"PARENT_PARENT: {class_details(self.parent.__bases__[0])}")

        orig_doc = list(super().process_doc(docstrings))
        new_doc = ["*** This is an inherited method ***", ""] + orig_doc

        print(f"DOCSTRINGS: {docstrings}")
        print(f"ORIG_DOC: {orig_doc}")
        print(f"NEW_DOC: {new_doc}")

        return new_doc


def setup(app):
    app.add_autodocumenter(ExperimentalDocumenter)

    return {
        'version': '0.1',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
