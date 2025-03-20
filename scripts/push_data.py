import json
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime

# Configuración de MongoDB Atlas
MONGO_URI = "mongodb+srv://hola:hola@cluster0.ysuaj.mongodb.net/<tu-db>"
client = MongoClient(MONGO_URI)
db = client["restaurant_db"]

# Función para convertir fecha a datetime
def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%b %d %Y")  # Convierte formato 'Dec 26 2023' a datetime
    except ValueError:
        return None

def insert_restaurants():
    with open("../datasets/restaurants_clean.json", "r", encoding="utf-8") as f:
        restaurants = json.load(f)

    for r in restaurants:
        r["_id"] = ObjectId(r["_id"]["$oid"])  # Convertimos _id a ObjectId

    db.transformed_restaurants.insert_many(restaurants)
    print("Datos de restaurantes insertados correctamente.")

def insert_inspections():
    with open("../datasets/inspections_clean.json", "r", encoding="utf-8") as f:
        inspections = json.load(f)

    for i in inspections:
        i["_id"] = ObjectId(i["_id"]["$oid"])  # Convertimos _id a ObjectId
        i["restaurant_id"] = ObjectId(i["restaurant_id"]["$oid"])  # Convertimos restaurant_id a ObjectId
        i["date"] = parse_date(i["date"])  # Convertimos date a datetime

    db.transformed_inspections.insert_many(inspections)
    print("Datos de inspecciones insertados correctamente.")

if __name__ == "__main__":
    db.transformed_restaurants.delete_many({})
    db.transformed_inspections.delete_many({})
    insert_restaurants()
    insert_inspections()
    print("Importación completa.")
