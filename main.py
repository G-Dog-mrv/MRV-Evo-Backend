from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
import os

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://wonderful-smoke-0751a6310.6.azurestaticapps.net",
        "https://wonderful-smoke-0751a6310-preview.centralus.6.azurestaticapps.net"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
connection_string = (
    f"mssql+pyodbc://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
    "&Connection Timeout=60"
)

engine = create_engine(connection_string)
metadata = MetaData()

# Define Items table
items_table = Table(
    "Items",
    metadata,
    Column("Id", Integer, primary_key=True, autoincrement=True),
    Column("Name", String),
    Column("Description", String),
    Column("Category", String),
)

metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Pydantic model
class Item(BaseModel):
    name: str
    description: str
    category: str

# CRUD Endpoints
@app.post("/items/")
def create_item(item: Item):
    with Session() as session:
        result = session.execute(
            items_table.insert().values(
                Name=item.name,
                Description=item.description,
                Category=item.category
            )
        )
        session.commit()
        item_id = result.inserted_primary_key[0]
        new_item = session.execute(
            items_table.select().where(items_table.c.Id == item_id)
        ).fetchone()
        return {col.name: value for col, value in zip(items_table.columns, new_item)}

@app.get("/items/")
def get_items():
    with Session() as session:
        items = session.execute(items_table.select()).fetchall()
        return [{col.name: value for col, value in zip(items_table.columns, item)} for item in items]

@app.get("/items/{item_id}")
def get_item(item_id: int):
    with Session() as session:
        item = session.execute(
            items_table.select().where(items_table.c.Id == item_id)
        ).fetchone()
        if item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return {col.name: value for col, value in zip(items_table.columns, item)}

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    with Session() as session:
        existing_item = session.execute(
            items_table.select().where(items_table.c.Id == item_id)
        ).fetchone()
        if existing_item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        session.execute(
            items_table.update().where(items_table.c.Id == item_id).values(
                Name=item.name,
                Description=item.description,
                Category=item.category
            )
        )
        session.commit()
        updated_item = session.execute(
            items_table.select().where(items_table.c.Id == item_id)
        ).fetchone()
        return {col.name: value for col, value in zip(items_table.columns, updated_item)}

@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    with Session() as session:
        existing_item = session.execute(
            items_table.select().where(items_table.c.Id == item_id)
        ).fetchone()
        if existing_item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        session.execute(
            items_table.delete().where(items_table.c.Id == item_id)
        )
        session.commit()
        return {"message": "Item deleted"}