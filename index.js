require("dotenv").config();
const axios = require("axios");
const fs = require("fs");
const { MongoClient, ServerApiVersion } = require("mongodb");

// Define the SEC EDGAR API endpoint and parameters
const secApiUrl = "https://data.sec.gov/submissions/CIK{cik}.json";

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

// Function to retrieve and insert company data for a given CIK
async function processCompany(cik) {
  try {
    // Connect the client to the server
    await client.connect();
    console.log("Connected to MongoDB Atlas");

    // Specify the database and collections
    const db = client.db("companies");
    const companiesCollection = db.collection("companyData");
    const filingsCollection = db.collection("filingsData");

    // Pad the CIK with leading zeros to make it 10 digits
    const paddedCik = String(cik).padStart(10, "0");

    // Make a request to the SEC EDGAR API
    const response = await axios.get(secApiUrl.replace("{cik}", paddedCik));

    // Parse the response to extract company fact information
    const companyFacts = response.data;

    // Extract the filings data and remove it from the companyFacts object
    const filingsData = companyFacts.filings;
    delete companyFacts.filings;

    // Insert the company fact information into the MongoDB Atlas database
    const companyResult = await companiesCollection.insertOne(companyFacts);
    const companyId = companyResult.insertedId;

    // Add a reference field (companyId) to the filings data
    filingsData.companyId = companyId;

    // Insert the filings data into the filings collection
    const filingsResult = await filingsCollection.insertOne(filingsData);

    console.log(
      `Company facts and filings for CIK ${paddedCik} inserted successfully.`
    );
  } catch (error) {
    // Check if the error is a 404 Not Found error
    if (error.response && error.response.status === 404) {
      console.log(`No data found for CIK ${cik}. Skipping...`);
    } else {
      // Throw the error to be handled by the caller
      throw new Error(`An error occurred for CIK ${cik}: ${error.message}`);
    }
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}

// Read and parse the JSON file
const companyTickers = JSON.parse(
  fs.readFileSync("company-tickers.json", "utf-8")
);

// Iterate over the companies and process each one
(async () => {
  for (const key in companyTickers) {
    if (companyTickers.hasOwnProperty(key)) {
      const cik = companyTickers[key].cik_str;
      try {
        await processCompany(cik);
      } catch (error) {
        console.error(error.message);
        process.exit(1); // Exit the program with a non-zero status code
      }
    }
  }
})();
