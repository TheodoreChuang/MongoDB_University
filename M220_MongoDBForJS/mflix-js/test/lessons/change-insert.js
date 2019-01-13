const MongoClient = require("mongodb").MongoClient
require("dotenv").config()
;(async function() {
  const client = await MongoClient.connect(
    process.env.MFLIX_DB_URI,
    { wtimeout: 2500, poolSize: 50, useNewUrlParser: true },
  )

  // In this lesson, we're going to use change streams to track real-time changes to the data that our application's using.

  // Change Streams
  // Report changes at the collection level
  // Accept pipelines to transform change events
  // As of MongoDB 3.6, change streams report changes at the collection level, so we open a change stream against a specific collection.

  // But by default it will return any change to the data in that collection regardless of what it is,
  // so we can also pass a pipeline to transform the change events we get back from the stream.

  try {
    const pipeline = [
      {
        $project: { documentKey: false },
      },
    ]

    // So here I'm just initializing my MongoClient object,
    console.log("Connected correctly to server")
    // and here we are specifying db and collections
    const db = await client.db("superheroesdb")
    const collection = db.collection("superheroes")

    // So here I'm opening a change stream against the super heroes (point) collection, using the watch() method. watch() (point)
    // returns a cursor object, so we can iterate through it to return whatever document is next in the cursor.
    // We've wrapped this in a try-catch block so if something happens to the connection used for the change stream, we'll know immediately.

    const changeStream = collection.watch(pipeline)
    // start listen to changes
    changeStream.on("change", change => {
      console.log(change)
    })

    insertDocs(collection)
  } catch (e) {
    console.log(e)
  }

  // Let's say that we have a shop that sells graded super hero collectables. We have some Super Man figures which are graded
  // and priced according to grade. Let's insert our documents:

  async function insertDocs(collection) {
    let amounts = [20, 30, 40, 50, 65, 75, 80, 90, 95, 100]

    // Creating documents to insert based on the amounts array above.
    let docs = amounts.map((amount, idx) => ({
      title: `Super Man ${idx + 1}`,
      amount,
    }))

    console.log(docs)

    /// And then inserting them:

    for (var idx = 0; idx < docs.length; idx++) {
      let doc = docs[idx]
      setTimeout(function() {
        let insertResult = collection.insertOne(doc, function(err) {
          console.log(err)
        })
        console.log("insertResult: ", insertResult)
      }, 1000 * idx)
    }
  }

  // As we have opened a change stream on the superheroes collection, we should see the changes being logged as the documents are inserted.
  // Let's run this and see how it turns out. (run code)

  // Great! We can see all of the documents being inserted, our change stream works as expected! But it's not very useful at the moment.

  // So the change stream cursor is just gonna spit out anything it gets, with no filter.Any change to the data in the super heroes collection will appear in this output.

  // But really, this is noise.We don't care when a documemt is inserted, we only want to know when the amount we have left is close to zero.
})()
