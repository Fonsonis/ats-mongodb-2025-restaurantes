import json
from bson import ObjectId

def load_restaurants(input_file):
    with open(input_file, "r", encoding="utf-8") as f:
        return {r["_id"]["$oid"]: {
            "_id": str(ObjectId(r["_id"]["$oid"])),
            "name": r["name"],
            "address": {
                "street": r["address"],
                "city": r["address line 2"],
                "postcode": f"{r['outcode']} {r['postcode']}"
            },
            "rating": r["rating"],
            "type_of_food": r["type_of_food"],
            "url": r["URL"]
        } for r in json.load(f)}

def transform_restaurants(input_file, output_file):
    restaurants = load_restaurants(input_file)
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(
            [{**r, "_id": {"$oid": r["_id"]}} for r in restaurants.values()], 
            f, 
            indent=4
        )

def transform_inspections(input_file, output_file, restaurants_file):
    restaurants = load_restaurants(restaurants_file)
    
    with open(input_file, "r", encoding="utf-8") as f:
        inspections = json.load(f)
    
    transformed_inspections = []
    for inspection in inspections:
        restaurant_id = inspection["restaurant_id"]
        if isinstance(restaurant_id, dict) and "$oid" in restaurant_id:
            restaurant_id = str(ObjectId(restaurant_id["$oid"]))
        else:
            restaurant_id = str(ObjectId(restaurant_id))
        
        transformed_inspections.append({
            "_id": {"$oid": str(ObjectId(inspection["_id"]["$oid"]))},
            "restaurant_id": {"$oid": restaurant_id},
            "certificate_number": inspection["certificate_number"],
            "date": inspection["date"],
            "result": inspection["result"],
            "sector": inspection["sector"],
            "address": restaurants.get(str(restaurant_id), {}).get("address", {})
        })
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(transformed_inspections, f, indent=4)

if __name__ == "__main__":
    transform_restaurants("../datasets/restaurants_array.json", "../datasets/restaurants_clean.json")
    transform_inspections("../datasets/inspections_array.json", "../datasets/inspections_clean.json", "../datasets/restaurants_array.json")
    print("Transformación completada. Datos guardados en datasets/restaurants_clean.json e datasets/inspections_clean.json.")
