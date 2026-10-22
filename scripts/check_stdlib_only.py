"""校验 FluxDL 只依赖标准库。

"零第三方依赖"是 FluxDL 的核心卖点之一（不需要 pip install、离线可用、
不受供应链攻击影响），所以它必须由 CI 守着，而不是靠 README 中的承诺。
"""
import ast
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
STDLIB = set(sys.stdlib_module_names)
LOCAL = {"fluxdl", "fluxdl_server", "fluxdl_cli"}
TARGETS = ("fluxdl.py", "fluxdl_server.py", "fluxdl_cli.py")


def main() -> int:
    bad = []
    for name in TARGETS:
        tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                mods = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                mods = [(node.module or "").split(".")[0]]
            else:
                continue
            for m in mods:
                if m and m not in STDLIB and m not in LOCAL:
                    bad.append((name, m, node.lineno))

    if bad:
        print("发现第三方依赖（FluxDL 要求零依赖）：")
        for f, m, ln in bad:
            print(f"  {f}:{ln}  导入 {m}")
        return 1

    print("零第三方依赖校验通过：%s" % ", ".join(TARGETS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
