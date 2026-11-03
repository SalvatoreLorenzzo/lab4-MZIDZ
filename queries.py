QUERIES = {
    1: """
        SELECT DISTINCT titleId AS tconst, title, language
        FROM akas
        WHERE region = 'UA' AND title IS NOT NULL
        ORDER BY tconst, title, language
    """,
    2: """
        SELECT nconst, primaryName, birthYear
        FROM names
        WHERE birthYear BETWEEN 1801 AND 1900
        ORDER BY birthYear, nconst
    """,
    3: """
        SELECT b.tconst, o.title, b.runtimeMinutes
        FROM basics b JOIN originals o ON b.tconst = o.tconst
        WHERE b.titleType = 'movie' AND b.runtimeMinutes > 120
        ORDER BY b.runtimeMinutes DESC, b.tconst
    """,
    4: """
        SELECT p.tconst, p.ordering, n.nconst,
               n.primaryName, o.title, p.characters
        FROM principals p
        JOIN names n ON p.nconst = n.nconst
        JOIN originals o ON p.tconst = o.tconst
        WHERE p.category IN ('actor', 'actress')
        ORDER BY p.tconst, p.ordering, n.nconst
    """,
    5: """
        SELECT a.region, COUNT(DISTINCT b.tconst) AS adultTitles
        FROM basics b JOIN akas a ON b.tconst = a.titleId
        WHERE b.isAdult = 1 AND a.region IS NOT NULL
        GROUP BY a.region
        ORDER BY adultTitles DESC, a.region
        LIMIT 100
    """,
    6: """
        WITH counts AS (
            SELECT parentTconst, COUNT(DISTINCT tconst) AS episodes
            FROM episodes WHERE parentTconst IS NOT NULL
            GROUP BY parentTconst
        )
        SELECT b.tconst, o.title, c.episodes
        FROM counts c JOIN basics b ON c.parentTconst = b.tconst
        JOIN originals o ON b.tconst = o.tconst
        WHERE b.titleType IN ('tvSeries', 'tvMiniSeries')
        ORDER BY c.episodes DESC, b.tconst
        LIMIT 50
    """,
    7: """
        WITH ranked AS (
            SELECT tconst, title, startYear, averageRating, numVotes,
                   CAST(FLOOR(startYear / 10) * 10 AS INT) AS decade,
                   ROW_NUMBER() OVER (
                       PARTITION BY FLOOR(startYear / 10)
                       ORDER BY averageRating DESC, numVotes DESC, tconst
                   ) AS rank
            FROM movie_ratings WHERE startYear IS NOT NULL
        )
        SELECT * FROM ranked WHERE rank <= 10
        ORDER BY decade DESC, rank
    """,
    8: """
        WITH expanded AS (
            SELECT tconst, title, averageRating, numVotes,
                   EXPLODE(SPLIT(genres, ',')) AS genre
            FROM movie_ratings WHERE genres IS NOT NULL
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY genre
                ORDER BY averageRating DESC, numVotes DESC, tconst
            ) AS rank FROM expanded
        )
        SELECT * FROM ranked WHERE rank <= 10
        ORDER BY genre, rank
    """,
}


def prepare_views(spark):
    spark.sql("""
        SELECT titleId AS tconst, MIN(title) AS title
        FROM akas WHERE isOriginalTitle = 1 AND title IS NOT NULL
        GROUP BY titleId
    """).createOrReplaceTempView('originals')
    spark.sql("""
        SELECT b.tconst, o.title, b.startYear, b.genres,
               r.averageRating, r.numVotes
        FROM basics b JOIN ratings r ON b.tconst = r.tconst
        JOIN originals o ON b.tconst = o.tconst
        WHERE b.titleType = 'movie' AND b.isAdult = 0
          AND r.averageRating IS NOT NULL AND r.numVotes > 0
    """).createOrReplaceTempView('movie_ratings')
