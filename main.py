from datetime import datetime
import asyncio
from os import getcwd, getenv
from sys import path
from tqdm import tqdm

path.append(getcwd())

from dotenv import load_dotenv
from app.get_order_p2p import get_all_order_binance_with_date
from app.get_binance_pay import get_all_order_binance_pay
from app.get_deposit_history import get_deposit_history
from app.get_withdraw_history import get_withdraw_history

try:
    load_dotenv("text.env")
except ImportError:
    pass

API_KEY = getenv("API_KEY")
SECRET_KEY = getenv("SECRET_KEY")
START_DATE = datetime(2024, 1, 1)
TRAGET_END_DATE = datetime(2024, 4, 30)


async def get_all_data():
    await asyncio.gather(
        *(
            func(API_KEY, SECRET_KEY, START_DATE, TRAGET_END_DATE)
            for func in [
                get_all_order_binance_with_date,
                get_all_order_binance_pay,
                get_deposit_history,
                get_withdraw_history,
            ]
        )
    )


if __name__ == "__main__":
    asyncio.run(get_all_data())
