import sys
sys.path.append(".")
import tests.qt_test as qt_test
import rsql_test
import tests.row_test as row_test, unittest
suite = unittest.TestLoader().loadTestsFromModule(row_test)
unittest.TextTestRunner(verbosity=2).run(suite)



