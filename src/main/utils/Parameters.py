from dataclasses import dataclass, field
from typing import List, Optional
import argparse


@dataclass
class Parameters:
    endpoint: str = ""
    access: str = ""
    secret: str = ""
    master: str = ""
    config: str = ""
    parameters: Optional[List[str]] = field(default_factory=list)


def parse_args(args):
    parser = argparse.ArgumentParser(description="Neogrid")

    parser.add_argument("--endpoint", type=str, help="Set EndPoint Config")
    parser.add_argument("--access", type=str, help="Set Access Config")
    parser.add_argument("--secret", type=str, help="Set Secret Config")
    parser.add_argument("--master", type=str, help="Set master")
    parser.add_argument("--config", type=str, required=True, help="Set config")
    parser.add_argument("--parameters", type=str, help="Set parameters")

    parsed_args = parser.parse_args(args)

    return Parameters(
        endpoint=parsed_args.endpoint,
        access=parsed_args.access,
        secret=parsed_args.secret,
        master=parsed_args.master,
        config=parsed_args.config,
        parameters=parsed_args.parameters.split(
            ",") if parsed_args.parameters else []
    )


if __name__ == "__main__":
    import sys
    parameters = parse_args(sys.argv[1:])
    print(parameters)
