import re

def main():
    with open("v6_IBKR_WebAPI/tests/test_bridge_anchor_bugfixes.py", "r") as f:
        text = f.read()

    new_func = """@pytest.fixture
def config():
    return AppConfig(
        enable_bridge_anchor=True,
        bridge_max_auto_trim_shares=5,
        anchor_buy_offset=1.5,
        google_sheet_id="test",
        google_credentials_json="{}"
    )"""

    old_func = """@pytest.fixture
def config():
    return AppConfig(
        enable_bridge_anchor=True,
        bridge_max_auto_trim_shares=5,
        anchor_buy_offset=1.5
    )"""

    text = text.replace(old_func, new_func)
    with open("v6_IBKR_WebAPI/tests/test_bridge_anchor_bugfixes.py", "w") as f:
        f.write(text)

if __name__ == "__main__":
    main()
