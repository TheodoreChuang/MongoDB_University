// M121 - Week 2 - Utility Stages

// Lab 1: Using Cursor-like Stages

// For movies released in the USA with a tomatoes.viewer.rating greater than or equal to 3, calculate a new field called num_favs that represets how many favorites appear in the cast field of the movie.
// Sort your results by num_favs, tomatoes.viewer.rating, and title, all in descending order.
// What is the title of the 25th film in the aggregation result?

// We store our favorites in a variable for easy reference within the pipeline
var favorites = [
  "Sandra Bullock",
  "Tom Hanks",
  "Julia Roberts",
  "Kevin Spacey",
  "George Clooney"
];

db.movies.aggregate([
  // We start by matching films that include at least one of our favorites in their cast
  {
    $match: {
      "tomatoes.viewer.rating": { $gte: 3 },
      countries: "USA",
      cast: {
        $in: favorites
      }
    }
  },
  // Then, we will be projecting the num_favs value by calculating the $size of the array intersection, between the given set of favorites and the film cast:
  {
    $project: {
      _id: 0,
      title: 1,
      "tomatoes.viewer.rating": 1,
      num_favs: {
        $size: {
          $setIntersection: ["$cast", favorites]
        }
      }
    }
  },
  // After that, we call the $sort stage and $skip + $limit in the result to the element requested:
  {
    $sort: { num_favs: -1, "tomatoes.viewer.rating": -1, title: -1 }
  },
  {
    $skip: 24
  },
  {
    $limit: 1
  }
]);

// Lab 2 - Bringing it all together
// Problem:

// Calculate an average rating for each movie in our collection where English is an available language, the minimum imdb.rating is at least 1, the minimum imdb.votes is at least 1, and it was released in 1990 or after. You'll be required to rescale (or normalize) imdb.votes. The formula to rescale imdb.votes and calculate normalized_rating is included as a handout.

// What film has the lowest normalized_rating?

db.movies.aggregate([
  // We start by applying the $match filtering:
  {
    $match: {
      year: { $gte: 1990 },
      languages: { $in: ["English"] },
      "imdb.votes": { $gte: 1 },
      "imdb.rating": { $gte: 1 }
    }
  },
  // And within the $project stage we apply the scaling and normalizating calculations:
  {
    $project: {
      _id: 0,
      title: 1,
      "imdb.rating": 1,
      "imdb.votes": 1,
      normalized_rating: {
        $avg: [
          "$imdb.rating",
          {
            $add: [
              1,
              {
                $multiply: [
                  9,
                  {
                    $divide: [
                      { $subtract: ["$imdb.votes", 5] },
                      { $subtract: [1521105, 5] }
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }
    }
  },
  { $sort: { normalized_rating: 1 } },
  { $limit: 1 }
]);

// in a new computed field normalized_rating.
// The first element of the result, after sorting by normalized_rating is The Christmas Tree, the expected correct answer.

// Use with Lab 2 - Bringing it all together

// general scaling
min + (max - min) * ((x - x_min) / (x_max - x_min));

// we will use 1 as the minimum value and 10 as the maximum value for scaling,
// so all scaled votes will fall into the range [1,10]

scaled_votes = 1 + 9 * ((x - x_min) / (x_max - x_min));

// NOTE: We CANNOT simply do 10 * ((x - x_min))..., results will be wrong
// Order of operations is important!

// use these values for scaling imdb.votes
x_max = 1521105;
x_min = 5;
min = 1;
max = 10;
x = imdb.votes;

// within a pipeline, it should look something like the following
/*
  {
    $add: [
      1,
      {
        $multiply: [
          9,
          {
            $divide: [
              { $subtract: [<x>, <x_min>] },
              { $subtract: [<x_max>, <x_min>] }
            ]
          }
        ]
      }
    ]
  }
*/

// given we have the numbers, this is how to calculated normalized_rating
// yes, you can use $avg in $project and $addFields!
normalized_rating = average(scaled_votes, imdb.rating);
