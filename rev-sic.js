require('dotenv').config();
const fs = require('fs');
const readline = require('readline');
const { MongoClient, ServerApiVersion } = require('mongodb');

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

// Create readline interface for user input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

// Prompt the user for input
rl.question('Enter the year for which you want to find revenue by SIC description: ', async (inputYear) => {
  const targetYear = parseInt(inputYear);

  // Define the aggregation pipeline
  const pipeline = [
    {
      $lookup: {
        from: "companyData",
        localField: "cik",
        foreignField: "cik",
        as: "company"
      }
    },
    {
      $unwind: "$company"
    },
    {
      $unwind: "$facts.us-gaap.Revenues.units.USD"
    },
    {
      $addFields: {
        revenueYear: { $year: { $toDate: "$facts.us-gaap.Revenues.units.USD.start" } }
      }
    },
    {
      $match: {
        revenueYear: targetYear
      }
    },
    {
      $group: {
        _id: "$company.sicDescription",
        totalRevenue: { $sum: "$facts.us-gaap.Revenues.units.USD.val" }
      }
    },
    {
      $sort: {
        totalRevenue: -1
      }
    }
  ];

  try {
    // Connect the client to the server
    await client.connect();
    console.log('Connected to MongoDB Atlas');

    // Specify the database and collections
    const db = client.db('companies');
    const accountingDataCollection = db.collection('accountingData');

    // Execute the aggregation pipeline
    const results = await accountingDataCollection.aggregate(pipeline).toArray();

    // Save the results to a JSON file
    const outputFileName = `revenue-by-sicDescription-${targetYear}.json`;
    fs.writeFileSync(outputFileName, JSON.stringify(results, null, 2));

    console.log(`Results saved to ${outputFileName}`);
  } catch (error) {
    console.error(`An error occurred: ${error.message}`);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
    rl.close();
  }
});
