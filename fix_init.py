with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if "_bridge_state: str =" in line:
        lines.insert(i+1, "        self._pending_trim_qty = 0\n")
        break

with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
    f.writelines(lines)
