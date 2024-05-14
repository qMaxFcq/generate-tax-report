import pandas as pd
from decimal import Decimal, ROUND_DOWN


def round_down(x, ndigits):
    return Decimal(x).quantize(Decimal("1e-{0}".format(ndigits)), rounding=ROUND_DOWN)


def calculate_realized_pnl(row, prev_left_amount, prev_left_cost, prev_avg_price):
    left_amount = round_down(prev_left_amount + Decimal(row["amount"]), 8)
    realized_pnl = Decimal("0.0")
    if (
        prev_left_amount > Decimal("0.0") and Decimal(row["amount"]) < Decimal("0.0")
    ) or (
        prev_left_amount < Decimal("0.0") and Decimal(row["amount"]) > Decimal("0.0")
    ):
        if abs(prev_left_amount) < abs(row["amount"]):
            realized_pnl = round_down(
                prev_left_amount * (row["price"] - prev_avg_price), 8
            )
            left_cost = round_down(left_amount * row["price"], 8)
            avg_price = row["price"]
        else:
            realized_pnl = round_down(
                row["amount"] * (prev_avg_price - row["price"]), 4
            )
            left_cost = round_down(left_amount * prev_avg_price, 8)
            avg_price = prev_avg_price
    else:
        left_cost = round_down(prev_left_cost + row["cost"], 8)
        avg_price = round_down(left_cost / left_amount, 8)
    return left_amount, left_cost, avg_price, realized_pnl


def realized_pnl(
    transaction_data: pd.DataFrame,
    left_amount_input: Decimal = Decimal("0.0"),
    left_cost_input: Decimal = Decimal("0.0"),
    avg_price_input: Decimal = Decimal("0.0"),
) -> pd.DataFrame:
    transaction_data["created_time"] = pd.to_datetime(transaction_data["created_time"])
    # transaction_data["completed_at"] = pd.to_datetime(transaction_data["completed_at"])

    ### cal avg price ###
    # buy -> low recrive, sell -> more sell -> binance p2p get only crypto
    transaction_data.loc[transaction_data["side"] == "BUY", "amount_n"] = (
        transaction_data["amount"] - transaction_data["fee"]
    )
    transaction_data.loc[transaction_data["side"] == "SELL", "amount_n"] = (
        transaction_data["amount"] + transaction_data["fee"]
    )
    transaction_data["amount"] = transaction_data["amount_n"]
    transaction_data = transaction_data.drop("amount_n", axis=1)
    transaction_data.loc[transaction_data["fee"] != Decimal("0.0"), "price"] = (
        transaction_data["cost"] / transaction_data["amount"]
    )

    # Change side buy +, sell -
    transaction_data["amount"] = transaction_data["amount"].where(
        transaction_data["side"] == "BUY", -1 * transaction_data["amount"]
    )
    transaction_data["amount"] = transaction_data["amount"].apply(
        lambda x: round_down(x, ndigits=8)
    )
    # Calculate cost
    transaction_data["cost"] = transaction_data["amount"] * transaction_data["price"]
    transaction_data["cost"] = transaction_data["cost"].apply(
        lambda x: round_down(x, ndigits=8)
    )
    transaction_data["price"] = transaction_data["price"].apply(
        lambda x: round_down(x, ndigits=8)
    )
    # Initialize columns
    transaction_data["realized_pnl"] = Decimal("0.0")
    transaction_data["left_amount"] = Decimal("0.0")
    transaction_data["left_cost"] = Decimal("0.0")
    transaction_data["avg_price"] = Decimal("0.0")
    # Init value first columns
    flag_init = (
        left_amount_input == Decimal("0.0")
        and left_cost_input == Decimal("0.0")
        and avg_price_input == Decimal("0.0")
    )
    transaction_data.at[0, "left_amount"] = (
        (transaction_data.at[0, "amount"]) if flag_init else left_amount_input
    )
    transaction_data.at[0, "left_cost"] = (
        (transaction_data.at[0, "cost"]) if flag_init else left_cost_input
    )
    transaction_data.at[0, "avg_price"] = (
        (transaction_data.at[0, "price"]) if flag_init else avg_price_input
    )

    for i, row in transaction_data.iloc[1:].iterrows():
        prev_left_amount = transaction_data.at[i - 1, "left_amount"]
        prev_left_cost = transaction_data.at[i - 1, "left_cost"]
        prev_avg_price = transaction_data.at[i - 1, "avg_price"]
        left_amount, left_cost, avg_price, realized_pnl = calculate_realized_pnl(
            row, prev_left_amount, prev_left_cost, prev_avg_price
        )
        transaction_data.at[i, "left_amount"] = left_amount
        transaction_data.at[i, "left_cost"] = left_cost
        transaction_data.at[i, "avg_price"] = avg_price
        transaction_data.at[i, "realized_pnl"] = realized_pnl
    if flag_init:
        return transaction_data[
            [
                "created_time",
                "symbol",
                "side",
                "amount",
                "cost",
                "price",
                "fee",
                "left_amount",
                "left_cost",
                "avg_price",
                "realized_pnl",
            ]
        ]
    else:
        return transaction_data[1:]


if __name__ == "__main__":

    # import local .csv
    data = pd.read_csv("all-order.csv")
    # print(data)

    # rename colume if name not match
    data = data.rename(
        columns={
            "unitPrice": "price",
            "create_time": "created_time",
            "tradeType": "side",
            "commission": "fee",
        }
    )
    data["price"] = data["price"].apply(lambda x: Decimal(x))
    data["amount"] = data["amount"].apply(lambda x: Decimal(x))
    data["cost"] = data["price"] * data["amount"]
    data["fee"] = data["fee"].apply(lambda x: Decimal(x))
    # file_name = "test2-vatayakorn.csv"
    # data.to_csv(file_name, index=False)
    # print(data)
    result = realized_pnl(data)
    print(result)
    file_name = "pnl-10.csv"
    result.to_csv(file_name, index=False)
