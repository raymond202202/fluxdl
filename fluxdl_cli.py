"""FluxDL 入口封装。

内核的 main(argv) 需要显式传参，setuptools 的 console_scripts
调用时不带参数，所以在包里加一层薄封装。
"""

import sys

import fluxdl


def run() -> int:
    return fluxdl.main(sys.argv[1:])


if __name__ == "__main__":
    sys.exit(run())
