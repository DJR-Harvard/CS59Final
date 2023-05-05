require("dotenv").config();
const fs = require("fs");
const { MongoClient, ServerApiVersion } = require("mongodb");

// MongoDB Atlas connection string (using the password from the environment variable)
const uri = `mongodb+srv://dar6816:${process.env.MONGODB_PASSWORD}@cluster0.trpvaut.mongodb.net/companies?retryWrites=true&w=majority`;

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});

const pipeline = [
  {
    '$group': {
      '_id': '$sic',
      'count': {
        '$sum': 1
      },
      'sicDescription': {
        '$first': '$sicDescription'
      }
    }
  }, {
    '$project': {
      '_id': 0,
      'sic': '$_id',
      'count': 1,
      'sicDescription': 1
    }
  }, {
    '$sort': {
      'count': -1
    }
  }
];

async function run() {
  try {
    // Connect the client to the server
    await client.connect();
    console.log("Connected to MongoDB Atlas");

    // Specify the database and collections
    const db = client.db("companies");
    const companiesCollection = db.collection("companyData");

    // Run the aggregation pipeline
    const results = await companiesCollection.aggregate(pipeline).toArray();

    // Write the results to the file as a formatted JSON string
    fs.writeFileSync('sic-results.json', JSON.stringify(results, null, 2));

    console.log('Results written to sic-results.json');
  } catch (error) {
    console.error(error.message);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}

run();
