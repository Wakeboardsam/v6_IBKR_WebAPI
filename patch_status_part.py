import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    new_func = """def _remove_status_part(status: str, prefix: str) -> str:
    parts = status.split('|')
    kept = [p for p in parts if not p.startswith(prefix)]
    if not any(p.startswith('OWNED:') for p in kept) and not any(p.startswith('WORKING_SELL:') for p in kept):
        kept.insert(0, "OWNED:0")
    return '|'.join(kept)"""

    old_func = """def _remove_status_part(status: str, prefix: str) -> str:
    parts = status.split('|')
    kept = [p for p in parts if not p.startswith(prefix)]
    if not any(p.startswith('OWNED:') for p in kept):
        kept.insert(0, "OWNED:0")
    return '|'.join(kept)"""

    text = text.replace(old_func, new_func)
    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
