from functools import lru_cache

import httpx
from fastapi import APIRouter, Depends, HTTPException

from app.config import Settings, get_settings
from app.models.dashboard import CardanoData

router = APIRouter()


@lru_cache()
def get_cmc_client(settings: Settings = Depends(get_settings)):
    return httpx.AsyncClient(
        base_url="https://pro-api.coinmarketcap.com/v1",
        headers={"X-CMC_PRO_API_KEY": settings.cmc_api_key}
    )


@router.get("/cardano/data", response_model=CardanoData)
async def get_cardano_data(
        client: httpx.AsyncClient = Depends(get_cmc_client)
):
    try:
        response = await client.get("/cryptocurrency/quotes/latest", params={"symbol": "ADA"})
        response.raise_for_status()
        data = response.json()["data"]["ADA"]

        return CardanoData(
            price=data["quote"]["USD"]["price"],
            market_cap=data["quote"]["USD"]["market_cap"],
            volume_24h=data["quote"]["USD"]["volume_24h"],
            percent_change_24h=data["quote"]["USD"]["percent_change_24h"]
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except KeyError:
        raise HTTPException(status_code=500, detail="Unexpected response format from CoinMarketCap")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
