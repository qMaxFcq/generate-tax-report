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


async def async_get_order_binance(
    api_key: str,
    secret_key: str,
    start_timestamp: int = None,
    end_datetime: int = None,
    page: int = 1,
) -> dict:
    base_url = BASE_URL
    endpoint = "/sapi/v1/c2c/orderMatch/listUserOrderHistory"
    timestamp = utcnow().format("x")[:-3]
    headers = {
        "X-MBX-APIKEY": api_key,
    }

    params = {
        "timestamp": timestamp,
        "startTimestamp": start_timestamp,
        "endTimestamp": end_datetime,
        "rows": BINANCE_MAX_ROW,
        "page": page,
    }
    params = {key: value for key, value in params.items() if value is not None}
    query_string, signature = await get_signature(params, secret_key)
    url = f"{base_url}{endpoint}?{query_string}&signature={signature}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            return await response.json()


async def get_all_order_binance_with_date(
    api_key: str,
    secret_key: str,
    start_date: int = None,
    target_end_date: int = None,
) -> DataFrame:
    res_date = get_range_time(start_date, target_end_date)
    responses: list = []

    for data in tqdm(range(len(res_date))):
        page = 1
        response_length = BINANCE_MAX_ROW + 1
        while response_length >= BINANCE_MAX_ROW:
            response = await async_get_order_binance(
                api_key,
                secret_key,
                res_date["start_timestamp"][data],
                res_date["end_timestamp"][data],
                page,
            )
            responses.append(response)
            response_length = len(response.get("data", []))
            page += 1

    response_data = []
    [response_data.extend(data.get("data", [])) for data in responses]
    all_data = DataFrame(response_data)
    all_data = DataFrame(
        response_data,
        columns=[
            "orderNumber",
            "tradeType",
            "asset",
            "fiat",
            "amount",
            "totalPrice",
            "unitPrice",
            "orderStatus",
            "createTime",
            "commission",
            "counterPartNickName",
            "create_time",
        ],
    )
    if all_data.empty:
        return DataFrame([])
    all_data = all_data[all_data["orderStatus"] == "COMPLETED"]
    all_data["create_time"] = to_datetime(all_data["createTime"], unit="ms", utc=True)
    all_data["create_time"] = all_data["create_time"].dt.tz_convert("Asia/Bangkok")
    all_data["create_time"] = all_data["create_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    all_data["symbol"] = all_data["asset"] + "_" + all_data["fiat"]
    all_data = all_data.sort_values(by=["createTime"], ascending=True).reset_index(
        drop=True
    )
    file_name = "order_p2p.csv"
    all_data.to_csv(file_name, index=False, mode="a")
    return responses


if __name__ == "__main__":

    async def main():
        start_date = datetime(2024, 1, 1)
        target_end_date = datetime(2024, 4, 30)
        await get_all_order_binance_with_date(
            API_KEY, SECRET_KEY, start_date, target_end_date
        )

    asyncio.run(main())
