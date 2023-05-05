require('dotenv').config();
const fs = require('fs');
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

// Get the current month and year
const currentDate = new Date();
const currentMonth = currentDate.getMonth() + 1; // Months are 0-indexed in JavaScript
const currentYear = currentDate.getFullYear();

// Define the aggregation pipeline
const pipeline = [
  {
    '$addFields': {
      'recentFilingMonthYear': {
        '$map': {
          'input': '$recent.filingDate',
          'as': 'dateStr',
          'in': {
            'month': {
              '$month': {
                '$dateFromString': {
                  'dateString': '$$dateStr'
                }
              }
            },
            'year': {
              '$year': {
                '$dateFromString': {
                  'dateString': '$$dateStr'
                }
              }
            }
          }
        }
      }
    }
  },
  {
    '$match': {
      'recentFilingMonthYear': {
        '$elemMatch': {
          'month': currentMonth,
          'year': currentYear
        }
      }
    }
  },
  {
    '$lookup': {
      'from': 'companyData',
      'localField': 'companyId',
      'foreignField': '_id',
      'as': 'companyData'
    }
  },
  {
    '$unwind': '$companyData'
  },
  {
    '$project': {
      '_id': 0,
      'companyData': '$companyData',
      'filingsData': '$$ROOT'
    }
  }
];

(async () => {
  try {
    // Connect the client to the server
    await client.connect();
    console.log('Connected to MongoDB Atlas');

    // Specify the database and collections
    const db = client.db('companies');
    const filingsCollection = db.collection('filingsData');

    // Execute the aggregation pipeline
    const results = await filingsCollection.aggregate(pipeline).toArray();

    // Save the results to a JSON file
    fs.writeFileSync('current-month-filings.json', JSON.stringify(results, null, 2));

    console.log('Results saved to current-month-filings.json');
  } catch (error) {
    console.error(`An error occurred: ${error.message}`);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
})();
