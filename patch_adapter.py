with open("v6_IBKR_WebAPI/brokers/ibkr/adapter.py", "r") as f:
    lines = f.readlines()

new_lines = []
in_get_open_orders = False
for line in lines:
    if "async def get_open_orders(self) -> list[dict]:" in line:
        in_get_open_orders = True
        new_lines.append(line)
        continue

    if in_get_open_orders:
        if "return orders" in line:
            in_get_open_orders = False
            new_lines.append(line)
        elif "status': trade.orderStatus.status" in line:
            new_lines.append(line.replace("})", ","))
            new_lines.append("                    'aux_price': trade.order.auxPrice,\n")
            new_lines.append("                    'order_type': trade.order.orderType,\n")
            new_lines.append("                    'tif': trade.order.tif,\n")
            new_lines.append("                    'exchange': trade.contract.exchange\n")
            new_lines.append("                })\n")
        elif "})" in line and "status':" not in new_lines[-1]:
            continue
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open("v6_IBKR_WebAPI/brokers/ibkr/adapter.py", "w") as f:
    f.writelines(new_lines)
