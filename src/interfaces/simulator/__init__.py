#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from .datareader import TSDBReader
from .datareader import FileReader, TestFileReader
from .dataprovider import DataProvider, TestDataProvider
from .execreceiver import ExecReceiver, TestExecReceiver
from .simulator import Simulator

agents = {
    "interfaces-dataprovider": DataProvider,
    "interfaces-execreceiver": ExecReceiver,
    "interfaces-simulator": Simulator,
}
tests = {
    "simulator/filereader": TestFileReader,
    "simulator/dataprovider": TestDataProvider,
    "simulator/execreceiver": TestExecReceiver,
}
__all__ = ["DataProvider", "ExecReceiver", "Simulator", "agents", "tests"]

#███████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀