"""Finding movie recommendations to a given movie through Cosine Similarity
using Spark Dataframes
"""

from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
import sys



def computeCosineSimilarity(df):
    """Function to compute the Cosine Similarity score of ratings for each
    movie pair.
    """
    # Create the new columns xx, yy, and xy for cosine similarity computation
    df_score = (
        df.withColumn("xx", F.col("rating1") * F.col("rating1"))
        .withColumn("yy", F.col("rating2") * F.col("rating2"))
        .withColumn("xy", F.col("rating1") * F.col("rating2"))
    )

    # Aggregate the results by unique movie pairs and calculate numerator, ...
    # ...denominator, and number of shared viewers (i.e. numPairs)
    df_score = df_score.groupby(["movieId1", "movieId2"]).agg(
        F.sum(F.col("xy")).alias("numerator"),
        (
            F.sqrt(F.sum(F.col("xx"))) * F.sqrt(F.sum(F.col("yy")))
        ).alias("denominator"),
        F.count("*").alias("numPairs"),
    )

    # Create a user-defined function to calculate the cosine similarity score
    get_score = F.udf(lambda n, d: 0 if d == 0 else (n / d), T.FloatType())

    # Calculate the Cosine Similarity score for each movie pair using ...
    # ...numerator and denominator, and include relevant columns only ...
    # ...in the output dataframe
    df_score = df_score.withColumn(
        "score", get_score(F.col("numerator"), F.col("denominator"))
    ).select(["movieId1", "movieId2", "score", "numPairs"])

    return df_score


def getMovieName(movieId):
    """Function to get the movie name for a given movie ID"""
    return movieNamesDF.filter(F.col("movieId") == movieId) \
                       .collect()[0] \
                       .movieName


def printRecommendations(results, targetMovieId):
    """Function to print information about the top 10 movie recommendations
    for a given target movie.
    """
    for result in results:
        # Determine the recommended movie ID
        if result.movieId1 == targetMovieId:
            recommendedMovieId = result.movieId2
        if result.movieId2 == targetMovieId:
            recommendedMovieId = result.movieId1

        # Get the movie name, similarity score, and strenth for each ...
        # ...recommendation
        recommendedMovieName = getMovieName(recommendedMovieId)
        similarityScore = result.score
        strength = result.numPairs

        # Print recommendation details
        print("-" * 80)

        print(
            "{} viewers also watched:\n{}\nSimilarity Score: {}\n".format(
                strength, recommendedMovieName, similarityScore
            )
        )

    return None


def main():
    """Function to execute the program.
    """
    # Create spark object
    # .master('local[*]'): use every CPU core on the local machine to ...
    # ...execute the job
    spark = (
        SparkSession.builder.appName("MovieSimilarities")
        .master("local[*]")
        .getOrCreate()
    )
    # Disable logging of info messages to the console
    spark.sparkContext.setLogLevel("ERROR")

    # Define the schema for the movie names dataset
    movieNamesSchema = T.StructType(
        [
            T.StructField("movieId", T.IntegerType()),
            T.StructField("movieName", T.StringType()),
        ]
    )

    # Read the movie names data into a global spark dataframe
    global movieNamesDF
    movieNamesDF = spark.read.csv(
        "./data/ml-100k/u.item",
        header=False,
        sep="|",
        encoding="ISO-8859-1",
        schema=movieNamesSchema,
    )

    # Define the schema for the movie ratings dataset
    movieSchema = T.StructType(
        [
            T.StructField("userId", T.IntegerType()),
            T.StructField("movieId", T.IntegerType()),
            T.StructField("rating", T.IntegerType()),
            T.StructField("timestamp", T.LongType()),
        ]
    )

    # Read the movie ratings data into a spark dataframe
    movieDF = spark.read.csv(
        "./data/ml-100k/u.data", header=False, sep="\t", schema=movieSchema
    )

    # Discard the rows where ratings are lower than 3,
    # and sort by userId and movieId in ascending order
    movieDF_filtered = movieDF.filter(movieDF.rating >= 3) \
                              .sort(["userId", "movieId"])

    # Include relevant columns only in ratingDF
    ratingDF = movieDF_filtered.select(["userId", "movieId", "rating"])
    # Create two copies of the ratingDF and rename their columns properly ...
    # ...for later join
    ratingDF_1 = ratingDF.withColumnRenamed("movieId", "movieId1") \
                         .withColumnRenamed("rating", "rating1")
    ratingDF_2 = ratingDF.withColumnRenamed("movieId", "movieId2") \
                         .withColumnRenamed("rating", "rating2")

    # Join the two idential dataframes, and filter to get unique pairs of ...
    # ...movies watched by the same users
    joinedDF = ratingDF_1.join(ratingDF_2, how="inner", on="userId") \
                         .where("movieId1 < movieId2")

    # Compute Consine Similarity score and number of shared viewers for ...
    # ...each unique pair of movies
    moviePairsScoredDF = computeCosineSimilarity(joinedDF).cache()
    # .cache(): cache dataframe to MEMORY
    # .persist(): cache dataframe to DISK

    # Check if a target movie ID is provided as a command-line argument
    if len(sys.argv) > 1:
        # Get the target movie ID for which we want to find movie ...
        # ...recommendations
        movieID = int(sys.argv[1])
        # Set the threshold for similarity score
        scoreThreshold = 0.97
        # Set the threshold for numPairs
        coOccurrenceThreshold = 50.0

        # Filter the dataframe to get the movie pairs for the target movie ...
        # ...that exceed the specified thresholds above
        moviePairsScoredDF_filtered = moviePairsScoredDF.filter(
            ((F.col("movieId1") == movieID) | (F.col("movieId2") == movieID))
            & (F.col("score") > scoreThreshold)
            & (F.col("numPairs") > coOccurrenceThreshold)
        )

        # Get the top 10 movie recommendations based on similarity score
        top10_scores = moviePairsScoredDF_filtered.sort(F.desc("score")) \
                                                  .head(10)
        # Get the top 10 movie recommendations based on number of shared viewers
        top10_viewers = moviePairsScoredDF_filtered.sort(F.desc("numPairs")) \
                                                   .head(10)

        # Get the target movie name
        targetMovieName = getMovieName(movieID)

        # Print information about the top 10 movie recommendations based on ...
        # ...Cosine Similarity score
        print(
            f"Top 10 movie recommendations for {targetMovieName} based on "
            "Cosine Similarity score of ratings:\n"
        )
        printRecommendations(top10_scores, movieID)

        print("=" * 80)

        # Print information about the top 10 movie recommendations based on ...
        # ...the number of shared viewers
        print(
            f"Top 10 movie recommendations for {targetMovieName} based on "
            "the number of shared viewers:\n"
        )
        printRecommendations(top10_viewers, movieID)

    else:
        # Error handling logging info
        print(
            "Movie ID not provided!\nPlease enter a valid movie ID "
            "and run the script again."
        )



if __name__ == "__main__":
    main()


"""After executing the Spark driver script, you can access the Spark UI via
`localhost:4040`, which can be helpful for debugging purposes.
"""
