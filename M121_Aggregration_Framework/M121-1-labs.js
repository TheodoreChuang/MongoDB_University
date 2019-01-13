// M121 - Chapter 1: Basic Aggregation - $match and $project

// mongo "mongodb://cluster0-shard-00-00-jxeqq.mongodb.net:27017,cluster0-shard-00-01-jxeqq.mongodb.net:27017,cluster0-shard-00-02-jxeqq.mongodb.net:27017/aggregations?replicaSet=Cluster0-shard-0" --authenticationDatabase admin --ssl -u m121 -p aggregations --norc

// ***** Lab 1 - $match *****

// imdb.rating is at least 7
// genres does not contain "Crime" or "Horror"
// rated is either "PG" or "G"
// languages contains "English" and "Japanese"

var pipeline = [
  {
    $match: {
      "imdb.rating": { $gte: 7 },
      genres: { $nin: ["Crime", "Horror"] },
      rated: { $in: ["PG", "G"] },
      languages: { $all: ["English", "Japanese"] }
    }
  }
];

db.movies.aggregate(pipeline).itcount(); //23

// ***** Lab 2 - $project *****

// Using the same $match stage from the previous lab, add a $project stage to only display the the title and film rating (title and rated fields).

var pipeline = [
  {
    $match: {
      "imdb.rating": { $gte: 7 },
      genres: { $nin: ["Crime", "Horror"] },
      rated: { $in: ["PG", "G"] },
      languages: { $all: ["English", "Japanese"] }
    }
  },
  {
    $project: { _id: 0, title: 1, rated: 1 }
  }
];

db.movies.aggregate(pipeline).itcount();

// ***** Lab 3 - Computing Fields *****

// Find a count of the number of movies that have a title composed of one word.
// { $split: [ <string expression>, <delimiter> ] }
// { $size: <expression> }

db.movies
  .aggregate([
    // We begin with a $match stage, ensuring that we only allow movies where the title is a string
    {
      $match: {
        title: {
          $type: "string"
        }
      }
    },
    // Next is our $project stage, splitting the title on spaces. This creates an array of strings
    {
      $project: {
        title: { $split: ["$title", " "] },
        _id: 0
      }
    },
    // We use another $match stage to filter down to documents that only have one element in the newly computed title field, and use itcount() to get a count
    {
      $match: {
        title: { $size: 1 }
      }
    }
  ])
  .itcount();

// ***** Lab 4 - Optional *****

db.movies.aggregate([
  // With our first $match stage, we filter out documents that are not an array or have an empty array for the fields we are interested in.
  {
    $match: {
      cast: { $elemMatch: { $exists: true } },
      directors: { $elemMatch: { $exists: true } },
      writers: { $elemMatch: { $exists: true } }
    }
  },
  // Next is a $project stage, removing the _id field and retaining both the directors and cast fields. We replace the existing writers field with a new computed value, cleaning up the strings within writers
  {
    $project: {
      _id: 0,
      cast: 1,
      directors: 1,
      writers: {
        $map: {
          input: "$writers",
          as: "writer",
          in: {
            $arrayElemAt: [
              {
                $split: ["$$writer", " ("]
              },
              0
            ]
          }
        }
      }
    }
  },
  // We use another $project stage to computer a new field called labor_of_love that ensures the intersection of cast, writers, and our newly cleaned directors is greater than 0. This definitely means that at least one element in each array is identical! $gt will return true or false.
  {
    $project: {
      labor_of_love: {
        $gt: [
          { $size: { $setIntersection: ["$cast", "$directors", "$writers"] } },
          0
        ]
      }
    }
  },
  // Lastly, we follow with a $match stage, only allowing documents through where labor_of_love is true. In our example we use a $match stage, but itcount() works too.
  {
    $match: { labor_of_love: true }
  },
  {
    $count: "labors of love"
  }
]);
// 1597
