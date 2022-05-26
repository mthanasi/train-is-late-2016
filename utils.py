import getpass
import sys


def read_or_ask(idx: int, prompt: str, hidden: bool = False) -> str:
    """Reads an argument from the command line or asks for it via STDIN.

    :param idx: Index of argument to read from command line.
    :param prompt: Prompt to ask in case the argument is not available.
    :param hidden: Indicates if a password prompt should be used.
    """

    if len(sys.argv) > idx:
        return sys.argv[idx]
    elif hidden:
        return getpass.getpass(prompt)
    else:
        return input(prompt)
