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
from app.config.config import BINANCE_MAX_ROW, BASE_URL_TH

try:
    load_dotenv("text.env")
except ImportError:
    pass

API_KEY_TH = getenv("API_KEY_TH")
SECRET_KEY_TH = getenv("SECRET_KEY_TH")


async def async_get_order_binance_th(
    api_key: str,
    secret_key: str,
    start_timestamp: int = None,
    end_datetime: int = None,
    # page: int = 1,
) -> dict:
    base_url = BASE_URL_TH
    endpoint = "/api/v1/capital/deposit/history"
    timestamp = utcnow().format("x")[:-3]
    headers = {
        "X-MBX-APIKEY": api_key,
    }

    params = {
        "timestamp": timestamp,
        "startTime": start_timestamp,
        "endTime": end_datetime,
        "limit": BINANCE_MAX_ROW,
        # "page": page,
    }
    params = {key: value for key, value in params.items() if value is not None}
    query_string, signature = await get_signature(params, secret_key)
    url = f"{base_url}{endpoint}?{query_string}&signature={signature}"

    print(url)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            # print(await response.json())
            return await response.json()


async def get_all_order_binance_th_with_date(
    api_key: str,
    secret_key: str,
    start_date: int = None,
    target_end_date: int = None,
) -> DataFrame:
    res_date = get_range_time(start_date, target_end_date)
    responses = []

    for data in tqdm(range(len(res_date))):
        response = await async_get_order_binance_th(
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
        target_end_date = datetime(2024, 4, 22)
        await get_all_order_binance_th_with_date(
            API_KEY_TH, SECRET_KEY_TH, start_date, target_end_date
        )

    asyncio.run(main())
