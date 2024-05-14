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
from app.config.config import BINANCE_MAX_ROW_DEPO_WITH, BASE_URL

try:
    load_dotenv("text.env")
except ImportError:
    pass

API_KEY = getenv("API_KEY")
SECRET_KEY = getenv("SECRET_KEY")


async def async_get_deposit_history(
    api_key: str,
    secret_key: str,
    start_timestamp: int = None,
    end_timestamp: int = None,
) -> dict:
    endpoint = "/sapi/v1/capital/deposit/hisrec"
    timestamp = utcnow().format("x")[:-3]
    headers = {
        "X-MBX-APIKEY": api_key,
    }

    params = {
        "transactionType": 0,
        "startTime": start_timestamp,
        "endTime": end_timestamp,
        "limit": BINANCE_MAX_ROW_DEPO_WITH,
        "timestamp": timestamp,
    }
    params = {key: value for key, value in params.items() if value is not None}

    query_string, signature = await get_signature(params, secret_key)
    url = f"{BASE_URL}{endpoint}?{query_string}&signature={signature}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            return await response.json()


async def get_deposit_history(
    api_key: str,
    secret_key: str,
    start_timestamp: int = None,
    target_end_date: int = None,
) -> DataFrame:
    res_date = get_range_time(start_timestamp, target_end_date)
    responses = []

    for data in tqdm(range(len(res_date))):
        response = await async_get_deposit_history(
            api_key,
            secret_key,
            res_date["start_timestamp"][data],
            res_date["end_timestamp"][data],
        )
        responses.extend(response if isinstance(response, list) else [response])

    if responses:
        df = DataFrame(responses)
        all_data = DataFrame(
            responses,
            columns=[
                "id",
                "amount",
                "coin",
                "network",
                "address",
                "addressTag",
                "txId",
                "insertTime",
            ],
        )
        if df.empty:
            return DataFrame([])
        all_data["insertTime"] = to_datetime(
            all_data["insertTime"], unit="ms", utc=True
        )
        all_data["insertTime"] = all_data["insertTime"].dt.tz_convert("Asia/Bangkok")
        all_data["insertTime"] = all_data["insertTime"].dt.strftime("%Y-%m-%d %H:%M:%S")
        all_data = all_data.sort_values(by=["insertTime"], ascending=True).reset_index(
            drop=True
        )
        csv_filename = "deposit.csv"
        all_data.to_csv(csv_filename, index=False, mode="a")


if __name__ == "__main__":

    async def main():
        start_date = datetime(2024, 1, 1)
        target_end_date = datetime(2024, 4, 30)
        await get_deposit_history(API_KEY, SECRET_KEY, start_date, target_end_date)

    asyncio.run(main())
