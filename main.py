import uvicorn
from sqlalchemy import Select, func, cast, Integer, DateTime
import httpx
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, async_session
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from typing import Annotated
from datetime import datetime

app = FastAPI()

engine = create_async_engine('sqlite+aiosqlite:///cats.db')
new_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_session():
    async with new_session() as session:
        yield session

SessionDep = Annotated[async_session, Depends(get_session)]

class Base(DeclarativeBase):
    pass

class CatBreed(Base):
    __tablename__ =  "cat_info"
    id: Mapped[int] = mapped_column(primary_key = True)
    time_create: Mapped[int] = mapped_column(DateTime, default=datetime.utcnow)
    city: Mapped[str]
    price: Mapped[str]
    breed: Mapped[str]

class DataRequest(BaseModel): #вводные параметры
    data: dict
    city: str
    price: int
    breed: str

class DataResponse(BaseModel): #выходные параметры
    original_data: dict
    city_original:str
    breed_name:str
    breed_country: str #страна происхождения
    breed_child_friendly: int #дрюжелюбность к детям
    breed_temperament: str #темперамент породы
    max_price: int #максимальная стоимость по городу и породе
    min_price: int #минимальная стоимость по городу и породе
    avg_price:float #минимальная стоимость по городу и породе

#переходим по API и берем данные по всем породам
async def fetch_cat_breeds():
    url = "https://api.thecatapi.com/v1/breeds"
    headers = {
        "x-api-key": "YOUR_API_KEY"
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

#находим минимальную стоимость породы в городе
async def get_min_price_find( session : SessionDep, city: str, breed: str):
    query = Select(func.count().label('price'),
        func.min(cast(CatBreed.price, Integer)).label('price')).filter(
        CatBreed.city == city,
        CatBreed.breed == breed
    )
    result = await session.execute(query)

    count,min_price = result.fetchone()

    # выводим ошибку по городу, т.к. по породе обработка идет выше
    if count == 0:
        raise HTTPException(status_code=404, detail="В этом городе ничего не найдено")
    return min_price

#находим макс стоимость породы в городе
async def get_max_price_find( session : SessionDep,city: str, breed: str):
    query = Select(func.max(cast(CatBreed.price, Integer)).label('price')).filter(
        CatBreed.city == city,
        CatBreed.breed == breed
    )
    result = await session.execute(query)
    return result.scalar()

async def get_avg_price(session : SessionDep, city: str, breed: str):
    query = Select(func.avg(cast(CatBreed.price, Integer)).label('price')).filter(
        CatBreed.city == city,
        CatBreed.breed == breed
    )
    result = await session.execute(query)
    return round(result.scalar(),2)

#создаем endpoint
@app.post("/process_data/", response_model=DataResponse)
async def process_data(data: DataRequest, session: SessionDep):
    # Получаем  список пород
    cat_breeds = await fetch_cat_breeds()
    FindBreed = 0

    #ищем породу по внешнему Api
    for breed in cat_breeds:
        if breed["name"].lower() == data.breed.lower():
             breed_cntry = breed["origin"]
             breed_child_status = breed["child_friendly"]
             breed_temper_status = breed["temperament"]
             FindBreed = 1

    #если породы нет, то вывести ошибку
    if FindBreed == 0:
        raise HTTPException(status_code=404, detail="Порода не найдена")

    # добавим новую введеную запись о стоимости в БД
    new_cat = CatBreed(
        city=data.city,
        price=data.price,
        breed=data.breed,
    )
    session.add(new_cat)
    await session.commit()

    #найдем необходимые значения
    min_price_find = await(get_min_price_find( session, data.city, data.breed))
    max_price_find = await(get_max_price_find( session, data.city, data.breed))
    avg_price_find = await (get_avg_price(session, data.city, data.breed))

    # Возвращаем результат
    return DataResponse(
        original_data=data.data,
        city_original = data.city,
        breed_name = data.breed,
        breed_country = breed_cntry,
        breed_temperament = breed_temper_status,
        breed_child_friendly = breed_child_status,
        max_price = max_price_find,
        min_price = min_price_find,
        avg_price = avg_price_find,
    )

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)