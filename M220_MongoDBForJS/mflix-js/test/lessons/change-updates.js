const MongoClient = require("mongodb").MongoClient
require("dotenv").config()
;(async function() {
  const client = await MongoClient.connect(
    process.env.MFLIX_DB_URI,
    { wtimeout: 2500, poolSize: 50, useNewUrlParser: true },
  )

  try {
    const pipeline = [
      {
        $project: { documentKey: false },
      },
    ]

    console.log("Connected correctly to server")
    const db = await client.db("superheroesdb")
    const collection = db.collection("superheroes")

    const changeStream = collection.watch(pipeline)
    // start listen to changes
    changeStream.on("change", change => {
      console.log(change)
    })

    // So the change stream cursor we saw previously, is just gonna spit out anything.
    // it gets, with no filter. Any change to the data in the collection will appear in this output.
    // But really, this is noise. We only want to know when it's close to zero (or below 20 in this case).
    // Thankfully , changestreams in MongoDB
    // let us specify an aggregation pipeline to control which change events are returned by the driver.

    // Here we are calling our changeDocsUpdate function which we have defined below:

    changeDocsUpdate(collection)
    updateAmount(collection)
  } catch (e) {
    console.log(e)
  }

  // Lets create a change stream that will alert us when the amount of stock for any given product drops below 20.

  async function changeDocsUpdate(collection) {
    // here we are defining a pipeline to use to filter out change stream events based on an aggregation operator.
    // We only going to return change events where the amount value is less than 20. (point)
    const low_quantity_pipeline = [
      { $match: { "fullDocument.amount": { $lt: 20 } } },
    ]

    // and here we are opening a change stream and passing the pipeline we created.

    const changeStream = collection.watch(low_quantity_pipeline)

    // This code will log the inserts and the change events from the change stream.

    let updatesDetected = 0
    changeStream.on("change", change => {
      console.log(updatesDetected, change)
      updatesDetected++
      if (updatesDetected === 10) {
        changeStream.close()
        collection.drop()
        process.exit(0)
      }
    })
  }

  // This function is setting the amount value to a value lower values than 20 to be logged by our change stream.
  // We have added a small timeout so that we can see the changes being logged

  async function updateAmount(collection) {
    for (let i = 0; i < 10; i++) {
      setTimeout(async () => {
        await collection.updateOne(
          { title: "Super Man 4" },
          {
            $set: { amount: `${i}` },
          },
          {
            upsert: true,
          },
        )
      }, i * 1000)
    }
  }
})()

// Lets run this and see what changes are logged.

// It works as expected. We can now be notified when our stock drops below 20 for Super Man 4.

// Summary
// Change streams can be opened against a collection
// Tracks data changes in real time
// Aggregation pipelines can be used to transform change event documents.
// So change streams are a great way to track changes to the data in a collection. And if you're using Mongo 4.0, you can open a change stream against a whole database, and even a whole cluster.

// We also have the flexibility to pass an aggregation pipeline to the change stream, to transform or filter out some of the change event documents.
