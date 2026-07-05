#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from argparse import ArgumentParser

from src.connectors import connectors_agents
from src.monitoring import monitoring_agents
from src.models import BaseAgent
from src.utils import Log, EventLoop

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
def main():
    parser = ArgumentParser()
    parser.add_argument("agent", nargs = "?", type = str)
    name: str = getattr(parser.parse_args(), "agent", None)
    assert isinstance(name, str), "Agent name is required"
    agents = dict[str, BaseAgent](
        **connectors_agents,
        **monitoring_agents
    )
    agent: BaseAgent = agents.get(name.lower(), None)
    if agent is None: parser.error(f"\"{name}\" not found")
    try: EventLoop.run_until_complete(agent().start())
    except Exception as EXC: Log.exception(EXC)
    finally: Log.success(f"Exiting \"{name}\"...")

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): main()
