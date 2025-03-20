db.transformed_restaurants.find({type_of_food: "Thai"})

db.transformed_inspections.find({result: {"$regex": "Violation", "$options": "i"}}).sort({date: 1})

db.transformed_restaurants.find({rating: {$gt: 4}})

db.transformed_restaurants.aggregate([
    {
        $group: {
            _id: "$type_of_food",
            calificacioPromig: { $avg: "$rating" }
        }
    }
])

db.transformed_inspections.aggregate([
    {
        $group: {
            _id: "$result",
            Quantitat: { $sum: 1 }
        }
    },
    {
        $group: {
            _id: null,
            totalIInspeccions: { $sum: "$Quantitat" },
            Resultats: { 
                $push: { 
                    tipusResultat: "$_id", 
                    Quantitat: "$Quantitat" 
                } 
            }
        }
    },
    {
        $unwind: "$Resultats"
    },
    {
        $project: {
            _id: 0,
            Resultat: "$Resultats.tipusResultat",
            Quantitat: "$Resultats.Quantitat",
            Percentatge: { 
                $multiply: [ 
                    { $divide: ["$Resultats.Quantitat", "$totalInspeccions"] }, 
                    100 
                ] 
            }
        }
    }
])

db.transformed_restaurants.aggregate([{"$lookup": {from: "transformed_inspections", "localField": "_id", "foreignField": "restaurant_id", "as": "Inspeccions"}}])

