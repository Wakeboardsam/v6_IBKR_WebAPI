with open("v6_IBKR_WebAPI/brokers/ibkr/adapter.py", "r") as f:
    text = f.read()

text = text.replace("'status': trade.orderStatus.status\n                    'aux_price':", "'status': trade.orderStatus.status,\n                    'aux_price':")

with open("v6_IBKR_WebAPI/brokers/ibkr/adapter.py", "w") as f:
    f.write(text)
