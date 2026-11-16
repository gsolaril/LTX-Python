#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from argparse import ArgumentParser

from src.connectors import agents as agents_con
from src.monitoring import agents as agents_mon
from src.models import BaseAgent
from src.utils import Log, EventLoop

#███████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄
def main():
    parser = ArgumentParser()
    parser.add_argument("agent", nargs = "?", type = str)
    name: str = getattr(parser.parse_args(), "agent", None)
    assert isinstance(name, str), "Agent name is required"
    agents = dict[str, BaseAgent](
        **agents_con,
        **agents_mon
    )
    agent: BaseAgent = agents.get(name.lower(), None)
    if agent is None:
        agent_list = str.join("\n", map("\t * {}".format, sorted(agents.keys())))
        parser.error(f"\n => \"{name}\" not found.\n ...Available:\n{agent_list}\n")

    Log.info(f"Agent: \"{agent.__name__}\"")
    try: EventLoop.run_until_complete(agent().start())
    except KeyboardInterrupt: print(); Log.success("See ya :)")
    except Exception as EXC: Log.exception("Fatal error:", EXC)
    finally: Log.warning(f"Exiting \"{name}\"...")

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"): main()
