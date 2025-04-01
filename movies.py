from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    spark = SparkSession.builder.appName("movies_data").getOrCreate()

    print("Reading movies3.csv ...")
    path_movies = "movies3.csv"
    df_movies = spark.read.csv(path_movies, header=True, inferSchema=True)

    # Renombrar columnas para facilitar su uso
    df_movies = df_movies.withColumnRenamed("id", "movie_id") \
                         .withColumnRenamed("name", "title") \
                         .withColumnRenamed("date", "release_year") \
                         .withColumnRenamed("minute", "duration") \
                         .withColumnRenamed("rating", "rating")

    df_movies.createOrReplaceTempView("movies")

    def save_to_jsonl(df, folder_name):
        path = os.path.join("results", folder_name, "data.jsonl")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as file:
            for row in df.toJSON().collect():
                file.write(row + "\n")

    # 1. Películas con mayor duración
    query = """SELECT title, duration FROM movies ORDER BY duration DESC LIMIT 10"""
    df_longest_movies = spark.sql(query)
    df_longest_movies.show()
    save_to_jsonl(df_longest_movies, "longest_movies")

    # 2. Películas con mejor calificación
    query = """SELECT title, rating FROM movies WHERE rating IS NOT NULL ORDER BY rating DESC LIMIT 10"""
    df_best_rated_movies = spark.sql(query)
    df_best_rated_movies.show()
    save_to_jsonl(df_best_rated_movies, "best_rated_movies")

    # 3. Cantidad de películas por año
    query = """SELECT release_year, COUNT(*) as total_movies FROM movies WHERE release_year IS NOT NULL GROUP BY release_year ORDER BY total_movies DESC"""
    df_movies_per_year = spark.sql(query)
    df_movies_per_year.show()
    save_to_jsonl(df_movies_per_year, "movies_per_year")

    # 4. Películas sin descripción
    query = """SELECT title FROM movies WHERE description IS NULL"""
    df_movies_without_desc = spark.sql(query)
    df_movies_without_desc.show()
    save_to_jsonl(df_movies_without_desc, "movies_without_desc")

    spark.stop()
