#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from argparse import ArgumentParser
from unittest import TestCase, TestLoader, TextTestRunner, TextTestResult

from src.models import tests as tests_models
from src.connectors import tests as tests_connectors
from src.monitoring import tests as tests_monitoring
from src.interfaces import tests as tests_interfaces
from src.utils import Log

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class CustomTestResult(TextTestResult):
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def addFailure(self, test, err):
        super().addFailure(test, err)
        Log.opt(exception = err).error("{} failed", test.id())
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def addError(self, test, err):
        super().addError(test, err)
        Log.opt(exception = err).error("{} errored", test.id())
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def printErrors(self): pass

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
def main():
    parser = ArgumentParser()
    parser.add_argument("agent", nargs = "?", type = str)
    name: str = getattr(parser.parse_args(), "agent", None)
    assert isinstance(name, str), "Agent name is required"
    tests = dict[str, type[TestCase]](
        **tests_models,
        **tests_connectors,
        **tests_monitoring,
        **tests_interfaces,
    )
    Test: type[TestCase] = tests.get(name.lower(), None)
    if Test is None: parser.error(f"\"{name}\" not found")
    suite = TestLoader().loadTestsFromTestCase(Test)
    result = TextTestRunner(verbosity = 2,
        resultclass = CustomTestResult).run(suite)
    if result.wasSuccessful():
        Log.success("All tests passed")

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): main()
