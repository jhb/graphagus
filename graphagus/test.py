import doctest
doctest.testfile("README.adoc",optionflags=doctest.ELLIPSIS)
doctest.testfile("test_traversal.rst", optionflags=doctest.ELLIPSIS)
