from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Float, SmallInteger, MetaData, Table
from sqlalchemy.orm import sessionmaker
from typing import Optional
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load environment variables based on ENV
env = os.getenv('ENV', 'dev')  # Default to 'dev' if ENV is not set
if env == 'dev':
    load_dotenv('.env.dev')  # Load .env.dev for development
else:
    load_dotenv()  # Load default .env for other environments

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
db_username = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_server = os.getenv('DB_SERVER')
db_name = os.getenv('DB_NAME')

if not all([db_username, db_password, db_server, db_name]):
    raise ValueError("Missing one or more environment variables: DB_USERNAME, DB_PASSWORD, DB_SERVER, DB_NAME")

connection_string = (
    f"mssql+pyodbc://{db_username}:{db_password}"
    f"@{db_server}/{db_name}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Encrypt=yes"
    "&TrustServerCertificate=no"
    "&Connection+Timeout=120"
)

logger.debug(f"Connection String: {connection_string}")

try:
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        logger.debug("Database connection successful")
except Exception as e:
    logger.error(f"Database connection failed: {str(e)}")
    raise

metadata = MetaData()

# Define tables
items_table = Table(
    "Items",
    metadata,
    Column("Id", Integer, primary_key=True, autoincrement=True),
    Column("Name", String(100), nullable=False),
    Column("Description", String(255), nullable=True),
    Column("Category", String(50), nullable=True),
)

mrv_master_products_table = Table(
    "MRVMasterProducts",
    metadata,
    Column("Id", SmallInteger, primary_key=True, autoincrement=True),
    Column("MRV_PRODUCT_NUMBER", Integer, nullable=False),
    Column("ProdTypKey", Integer, nullable=True),
    Column("ProdDescKey", Integer, nullable=False),
    Column("ProdCatKey", Integer, nullable=False),
    Column("PRODUCT_FMMO_CLASSIFICATION", Integer, nullable=True),
    Column("PRODUCT_FAT_CONTENT", Float, nullable=True),
    Column("UoMKey", Integer, nullable=True),
    Column("CONVERSION_OUNCES", Float, nullable=True),
    Column("MRV_TYPE", Integer, nullable=True),
)

product_description_table = Table(
    "ProductDescription",
    metadata,
    Column("Id", Integer, primary_key=True),
    Column("Product Description", String(100), nullable=False),  # Removed square brackets from definition
)

uom_table = Table(
    "UoM",
    metadata,
    Column("Id", Integer, primary_key=True),
    Column("UNIT_OF_MEASURE", String(50), nullable=False),
)

product_category_table = Table(
    "ProductCategory",
    metadata,
    Column("Id", Integer, primary_key=True),
    Column("PRODUCT_CATEGORY", String(50), nullable=False),
)

product_types_table = Table(
    "ProductTypes",
    metadata,
    Column("Id", Integer, primary_key=True),
    Column("Product Type", String(50), nullable=False),
)

# Create all tables (if not exist)
try:
    metadata.create_all(engine)
    logger.debug("Tables created successfully")
except Exception as e:
    logger.error(f"Failed to create tables: {str(e)}")
    raise

Session = sessionmaker(bind=engine)

# Pydantic models with validation
class Item(BaseModel):
    id: Optional[int] = None  # Optional since auto-incremented
    name: str = Field(..., min_length=1)
    description: Optional[str] = None
    category: Optional[str] = None

class MRVMasterProducts(BaseModel):
    id: Optional[int] = None  # Optional since auto-incremented
    mrv_product_number: int = Field(..., ge=0)
    prod_typ_key: Optional[int] = None
    prod_desc_key: int = Field(..., ge=1)  # References ProductDescription.Id
    prod_cat_key: int = Field(..., ge=1)   # References ProductCategory.Id
    product_fmmoclassification: Optional[int] = None
    product_fat_content: Optional[float] = None
    uom_key: Optional[int] = None             # References UoM.Id
    conversion_ounces: Optional[float] = None
    mrv_type: Optional[int] = None

    @classmethod
    def __get_validators__(cls):
        yield cls.validate_to_json

    @classmethod
    def validate_to_json(cls, values):
        with Session() as session:
            if values.get("prod_desc_key") and not session.execute(
                product_description_table.select().where(product_description_table.c.Id == values["prod_desc_key"])
            ).fetchone():
                raise ValueError("Invalid ProductDescription ID")
            if values.get("prod_cat_key") and not session.execute(
                product_category_table.select().where(product_category_table.c.Id == values["prod_cat_key"])
            ).fetchone():
                raise ValueError("Invalid ProductCategory ID")
            if values.get("prod_typ_key") and not session.execute(
                product_types_table.select().where(product_types_table.c.Id == values["prod_typ_key"])
            ).fetchone():
                raise ValueError("Invalid ProductTypes ID")
            if values.get("uom_key") and not session.execute(
                uom_table.select().where(uom_table.c.Id == values["uom_key"])
            ).fetchone():
                raise ValueError("Invalid UoM ID")
        return values

class ProductDescription(BaseModel):
    id: int = Field(..., ge=0)
    product_description: str = Field(..., min_length=1)  # Matches column name with space

class UoM(BaseModel):
    id: int = Field(..., ge=0)
    unit_of_measure: str = Field(..., min_length=1)

class ProductCategory(BaseModel):
    id: int = Field(..., ge=0)
    product_category: str = Field(..., min_length=1)

class ProductTypes(BaseModel):
    id: int = Field(..., ge=0)
    product_type: str = Field(..., min_length=1)

# CRUD Endpoints for Items
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

# CRUD Endpoints for MRVMasterProducts
@app.post("/mrv-master-products/")
def create_mrv_master_product(product: MRVMasterProducts):
    with Session() as session:
        # Validate all foreign keys
        if product.prod_desc_key and not session.execute(
            product_description_table.select().where(product_description_table.c.Id == product.prod_desc_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid ProductDescription ID")
        if product.prod_cat_key and not session.execute(
            product_category_table.select().where(product_category_table.c.Id == product.prod_cat_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid ProductCategory ID")
        if product.prod_typ_key and not session.execute(
            product_types_table.select().where(product_types_table.c.Id == product.prod_typ_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid ProductTypes ID")
        if product.uom_key and not session.execute(
            uom_table.select().where(uom_table.c.Id == product.uom_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid UoM ID")

        result = session.execute(
            mrv_master_products_table.insert().values(
                MRV_PRODUCT_NUMBER=product.mrv_product_number,
                ProdTypKey=product.prod_typ_key,
                ProdDescKey=product.prod_desc_key,
                ProdCatKey=product.prod_cat_key,
                PRODUCT_FMMO_CLASSIFICATION=product.product_fmmoclassification,
                PRODUCT_FAT_CONTENT=product.product_fat_content,
                UoMKey=product.uom_key,
                CONVERSION_OUNCES=product.conversion_ounces,
                MRV_TYPE=product.mrv_type
            )
        )
        session.commit()
        product_id = result.inserted_primary_key[0]
        new_product = session.execute(
            mrv_master_products_table.select().where(mrv_master_products_table.c.Id == product_id)
        ).fetchone()
        return {col.name: value for col, value in zip(mrv_master_products_table.columns, new_product)}

@app.get("/mrv-master-products/")
def get_mrv_master_products():
    with Session() as session:
        products = session.execute(mrv_master_products_table.select()).fetchall()
        return [{col.name: value for col, value in zip(mrv_master_products_table.columns, product)} for product in products]

@app.get("/mrv-master-products/{id}")
def get_mrv_master_product(id: int):
    with Session() as session:
        product = session.execute(
            mrv_master_products_table.select().where(mrv_master_products_table.c.Id == id)
        ).fetchone()
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        return {col.name: value for col, value in zip(mrv_master_products_table.columns, product)}

@app.put("/mrv-master-products/{id}")
def update_mrv_master_product(id: int, product: MRVMasterProducts):
    with Session() as session:
        existing_product = session.execute(
            mrv_master_products_table.select().where(mrv_master_products_table.c.Id == id)
        ).fetchone()
        if existing_product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        # Validate all foreign keys
        if product.prod_desc_key and not session.execute(
            product_description_table.select().where(product_description_table.c.Id == product.prod_desc_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid ProductDescription ID")
        if product.prod_cat_key and not session.execute(
            product_category_table.select().where(product_category_table.c.Id == product.prod_cat_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid ProductCategory ID")
        if product.prod_typ_key and not session.execute(
            product_types_table.select().where(product_types_table.c.Id == product.prod_typ_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid ProductTypes ID")
        if product.uom_key and not session.execute(
            uom_table.select().where(uom_table.c.Id == product.uom_key)
        ).fetchone():
            raise HTTPException(status_code=400, detail="Invalid UoM ID")

        session.execute(
            mrv_master_products_table.update().where(mrv_master_products_table.c.Id == id).values(
                MRV_PRODUCT_NUMBER=product.mrv_product_number,
                ProdTypKey=product.prod_typ_key,
                ProdDescKey=product.prod_desc_key,
                ProdCatKey=product.prod_cat_key,
                PRODUCT_FMMO_CLASSIFICATION=product.product_fmmoclassification,
                PRODUCT_FAT_CONTENT=product.product_fat_content,
                UoMKey=product.uom_key,
                CONVERSION_OUNCES=product.conversion_ounces,
                MRV_TYPE=product.mrv_type
            )
        )
        session.commit()
        updated_product = session.execute(
            mrv_master_products_table.select().where(mrv_master_products_table.c.Id == id)
        ).fetchone()
        return {col.name: value for col, value in zip(mrv_master_products_table.columns, updated_product)}

@app.delete("/mrv-master-products/{id}")
def delete_mrv_master_product(id: int):
    with Session() as session:
        existing_product = session.execute(
            mrv_master_products_table.select().where(mrv_master_products_table.c.Id == id)
        ).fetchone()
        if existing_product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        session.execute(
            mrv_master_products_table.delete().where(mrv_master_products_table.c.Id == id)
        )
        session.commit()
        return {"message": "Product deleted"}

# CRUD Endpoints for ProductDescription (Lookup Table)
@app.post("/product-descriptions/")
def create_product_description(description: ProductDescription):
    with Session() as session:
        result = session.execute(
            product_description_table.insert().values({
                "Id": description.id,
                "Product Description": description.product_description  # Dictionary syntax for column with space
            })
        )
        session.commit()
        new_description = session.execute(
            product_description_table.select().where(product_description_table.c.Id == description.id)
        ).fetchone()
        return {col.name: value for col, value in zip(product_description_table.columns, new_description)}

@app.get("/product-descriptions/")
def get_product_descriptions():
    with Session() as session:
        try:
            logger.debug("Executing query for product descriptions")
            descriptions = session.execute(product_description_table.select()).fetchall()
            logger.debug(f"Fetched {len(descriptions)} rows from ProductDescription table")
            result = [{col.name: value for col, value in zip(product_description_table.columns, desc)} for desc in descriptions]
            logger.debug(f"Processed result: {result[:5]}")  # Log first 5 items for debugging
            return result
        except Exception as e:
            logger.error(f"Error in get_product_descriptions: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.put("/product-descriptions/{id}")
def update_product_description(id: int, description: ProductDescription):
    with Session() as session:
        existing_desc = session.execute(
            product_description_table.select().where(product_description_table.c.Id == id)
        ).fetchone()
        if existing_desc is None:
            raise HTTPException(status_code=404, detail="Description not found")
        session.execute(
            product_description_table.update().where(product_description_table.c.Id == id).values({
                "Product Description": description.product_description  # Dictionary syntax for column with space
            })
        )
        session.commit()
        updated_desc = session.execute(
            product_description_table.select().where(product_description_table.c.Id == id)
        ).fetchone()
        return {col.name: value for col, value in zip(product_description_table.columns, updated_desc)}

@app.delete("/product-descriptions/{id}")
def delete_product_description(id: int):
    with Session() as session:
        # Check for dependent records in MRVMasterProducts
        dependent_count = session.execute(
            mrv_master_products_table.select().where(mrv_master_products_table.c.ProdDescKey == id)
        ).fetchall()
        if dependent_count:
            raise HTTPException(status_code=400, detail="Cannot delete description with dependent products")
        existing_desc = session.execute(
            product_description_table.select().where(product_description_table.c.Id == id)
        ).fetchone()
        if existing_desc is None:
            raise HTTPException(status_code=404, detail="Description not found")
        session.execute(
            product_description_table.delete().where(product_description_table.c.Id == id)
        )
        session.commit()
        return {"message": "Description deleted"}

# Existing Lookup Table Endpoints
@app.get("/uom/")
def get_uom():
    with Session() as session:
        uoms = session.execute(uom_table.select()).fetchall()
        return [{col.name: value for col, value in zip(uom_table.columns, uom)} for uom in uoms]

@app.get("/product-categories/")
def get_product_categories():
    with Session() as session:
        categories = session.execute(product_category_table.select()).fetchall()
        return [{col.name: value for col, value in zip(product_category_table.columns, category)} for category in categories]

@app.get("/product-types/")
def get_product_types():
    with Session() as session:
        types = session.execute(product_types_table.select()).fetchall()
        return [{col.name: value for col, value in zip(product_types_table.columns, type)} for type in types]