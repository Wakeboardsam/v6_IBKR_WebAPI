import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    # Fix test_bridge_trim_status_written: UnboundLocalError for asyncio
    text = text.replace("asyncio.create_task(self._sync_to_sheet())", "import asyncio\n                                        asyncio.create_task(self._sync_to_sheet())", 1)

    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
