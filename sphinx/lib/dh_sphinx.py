import os
import shutil
import pkgutil


def glob_package_names(packages):
    rst = []

    for package in packages:
        rst.append(package.__name__)

        if hasattr(package, "__path__"):
            for importer, modname, ispkg in pkgutil.walk_packages(path=package.__path__, prefix=package.__name__ + '.',
                                                                  onerror=lambda x: None):
                rst.append(modname)

    return rst


def _add_package(tree, package):
    n = package[0]

    if n not in tree:
        tree[n] = {}

    if len(package) > 1:
        _add_package(tree[n], package[1:])


def package_tree(package_names):
    rst = {}
    for pn in package_names:
        spn = pn.split('.')
        _add_package(rst, spn)
    return rst


def make_rst_tree(package, tree):
    package_name = ".".join(package)

    if len(tree) == 0:
        toctree = ""
    else:
        toctree = ".. toctree::\n"
        for k in tree:
            p = package.copy()
            p.append(k)
            pn = ".".join(p)
            toctree += "%s%s <%s>\n" % (" " * 4, k, pn)

    rst = "%s\n%s\n\n%s\n.. automodule:: %s\n    :members:\n    :no-undoc-members:\n    :show-inheritance:\n    :inherited-members:\n\n" % (
    package_name, "=" * len(package_name), toctree, package_name)

    if len(package) > 0:
        filename = f"code/{package_name}.rst"

        with open(filename, "w") as file:
            file.write(rst)

    for k, v in tree.items():
        p = package.copy()
        p.append(k)
        make_rst_tree(p, v)


def make_rst_modules(docs_title, package_roots):
    rst = f'''
Python Modules
##############

{docs_title}

.. toctree::
    :glob:

'''

    for pr in package_roots:
        rst += "\n%s./code/%s" % (" " * 4, pr.__name__)

    filename = "modules.rst"

    with open(filename, "w") as file:
        file.write(rst)


def gen_sphinx_modules(docs_title, package_roots, package_excludes):
    pn = glob_package_names(package_roots)
    pn = [p for p in pn if not any(exclude in p for exclude in package_excludes)]
    pt = package_tree(pn)

    if os.path.exists("code"):
        shutil.rmtree("code")
    os.mkdir("code")

    make_rst_modules(docs_title, package_roots)
    make_rst_tree([], pt)
