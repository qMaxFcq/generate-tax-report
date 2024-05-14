from arrow import utcnow
import aiohttp
import asyncio
from os import getcwd, getenv
from sys import path
from tqdm import tqdm

path.append(getcwd())

from dotenv import load_dotenv
from pandas import DataFrame, to_datetime
from datetime import datetime
from app.helper.get_signature import get_signature
from app.helper.get_range_time import get_range_time
from app.config.config import BINANCE_MAX_ROW, BASE_URL

try:
    load_dotenv("text.env")
except ImportError:
    pass

API_KEY = getenv("API_KEY")
SECRET_KEY = getenv("SECRET_KEY")


async def async_get_binance_pay_history(
    api_key: str,
    secret_key: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
) -> dict:
    endpoint = "/sapi/v1/pay/transactions"
    timestamp = utcnow().format("x")[:-3]
    headers = {
        "X-MBX-APIKEY": api_key,
    }

    params = {
        "startTime": start_timestamp,
        "endTime": end_timestamp,
        "limit": BINANCE_MAX_ROW,
        "timestamp": timestamp,
    }

    params = {key: value for key, value in params.items() if value is not None}

    query_string, signature = await get_signature(params, secret_key)
    url = f"{BASE_URL}{endpoint}?{query_string}&signature={signature}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            return await response.json()


async def get_all_order_binance_pay(
    api_key: str,
    secret_key: str,
    start_date: int = None,
    target_end_date: int = None,
) -> DataFrame:

    res_date = get_range_time(start_date, target_end_date)
    responses: list = []

    for data in tqdm(range(len(res_date))):
        response = await async_get_binance_pay_history(
            api_key,
            secret_key,
            res_date["start_timestamp"][data],
            res_date["end_timestamp"][data],
        )
        responses.append(response)

    response_data = []
    [response_data.extend(data.get("data", [])) for data in responses]
    all_data = DataFrame(response_data)

    if not all_data.empty:
        df = DataFrame(all_data)
        df["transactionTime"] = to_datetime(df["transactionTime"], unit="ms", utc=True)
        df["transactionTime"] = df["transactionTime"].dt.tz_convert("Asia/Bangkok")
        df["transactionTime"] = df["transactionTime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df_subset = df[["transactionTime", "amount", "currency", "note"]]
        df_subset["payerInfo.name"] = df["payerInfo"].apply(lambda x: x.get("name"))
        df_subset["receiverInfo.name"] = df["receiverInfo"].apply(
            lambda x: x.get("name")
        )
        df_subset = df_subset.sort_values(
            by=["transactionTime"], ascending=True
        ).reset_index(drop=True)
        file_name = "binance_pay.csv"
        df_subset.to_csv(file_name, index=False, mode="a")
        return responses


if __name__ == "__main__":

    async def main():
        start_date = datetime(2024, 1, 1)
        target_end_date = datetime(2024, 4, 30)
        await get_all_order_binance_pay(
            API_KEY, SECRET_KEY, start_date, target_end_date
        )

    asyncio.run(main())
