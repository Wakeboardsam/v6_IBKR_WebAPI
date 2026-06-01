import re

def main():
    with open("v6_IBKR_WebAPI/engine/engine.py", "r") as f:
        text = f.read()

    # Revert first replacement and do a precise one
    text = text.replace("import asyncio\n                                        asyncio.create_task(self._sync_to_sheet())", "asyncio.create_task(self._sync_to_sheet())")

    precise_old = """                                    # Append TRIM_SELL to row 7 status to persist
                                    current_status = row7.status
                                    if "TRIM_SELL" not in current_status:
                                        new_status = f"{current_status}|TRIM_SELL:{trim_order_id}"
                                        self._update_row_status_in_memory(7, new_status)
                                        asyncio.create_task(self._sync_to_sheet())"""

    precise_new = """                                    # Append TRIM_SELL to row 7 status to persist
                                    current_status = row7.status
                                    if "TRIM_SELL" not in current_status:
                                        new_status = f"{current_status}|TRIM_SELL:{trim_order_id}"
                                        self._update_row_status_in_memory(7, new_status)
                                        import asyncio
                                        asyncio.create_task(self._sync_to_sheet())"""

    text = text.replace(precise_old, precise_new)

    with open("v6_IBKR_WebAPI/engine/engine.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
