import gzip
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from main import SCHEMAS, read_data
from queries import QUERIES, prepare_views


spark = (SparkSession.builder.master('local[2]').appName('ETL tests')
         .config('spark.ui.enabled', 'false')
         .config('spark.driver.host', '127.0.0.1')
         .config('spark.driver.bindAddress', '127.0.0.1')
         .config('spark.sql.shuffle.partitions', '2').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    akas = [(t, 1, t, None, None, None, None, 1)
            for t in ('a', 'b', 'c', 's')]
    akas += [('a', 2, 'a', None, 'en', None, None, 1),
             ('a', 3, 'UA name', 'UA', 'ru', None, None, 0),
             ('b', 2, 'UA title', 'UA', 'uk', None, None, 0),
             ('c', 2, 'US one', 'US', 'en', None, None, 0),
             ('c', 3, 'US two', 'US', 'en', None, None, 0),
             ('c', 4, 'UA adult', 'UA', 'uk', None, None, 0)]
    fixtures = {
        'akas': akas,
        'basics': [('a', 'movie', 'a', 'a', 0, 1900, None, 121, 'Drama,Comedy'),
                   ('b', 'movie', 'b', 'b', 0, 1901, None, 120, 'Drama'),
                   ('c', 'movie', 'c', 'c', 1, 1901, None, 160, 'Adult'),
                   ('s', 'tvSeries', 's', 's', 0, 2000, None, 140, 'Drama')],
        'names': [('n1', 'Old', 1800, None, None, None),
                  ('n2', 'First', 1801, None, None, None),
                  ('n3', 'Last', 1900, None, None, None),
                  ('n4', 'Next', 1901, None, None, None)],
        'principals': [('a', 1, 'n2', 'actor', None, '["Role"]'),
                       ('a', 2, 'n3', 'director', None, None)],
        'episodes': [('e1', 's', 1, 1), ('e2', 's', 2, 1)],
        'ratings': [('a', 10.0, 5), ('b', 10.0, 10), ('c', 10.0, 100)],
    }
    for alias, rows in fixtures.items():
        spark.createDataFrame(rows, SCHEMAS[alias][1]).createOrReplaceTempView(alias)
    prepare_views(spark)
    results = {i: spark.sql(query).collect() for i, query in QUERIES.items()}
    assert {r.language for r in results[1]} == {'ru', 'uk'}
    assert {r.nconst for r in results[2]} == {'n2', 'n3'}
    assert {r.tconst for r in results[3]} == {'a', 'c'}
    assert len(results[4]) == 1 and results[4][0].characters == '["Role"]'
    assert {r.region: r.adultTitles for r in results[5]} == {'UA': 1, 'US': 1}
    assert len(results[6]) == 1 and results[6][0].episodes == 2
    assert [r.tconst for r in results[7]] == ['b', 'a']
    assert {r.decade for r in results[7]} == {1900}
    assert [(r.genre, r.tconst, r.rank) for r in results[8]] == [
        ('Comedy', 'a', 1), ('Drama', 'b', 1), ('Drama', 'a', 2)]
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'sample.tsv.gz'
        with gzip.open(path, 'wt', encoding='utf-8') as stream:
            stream.write('nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles\n')
            stream.write('n5\t"Quoted name"\t\\N\t\\N\tactor\t\\N\n')
        row = read_data(spark, path, SCHEMAS['names'][1]).first()
        assert row.birthYear is None and row.primaryName == '"Quoted name"'
    print('PASS: all eight queries, boundary values, duplicates, seasons, ranks and TSV nulls')
finally:
    spark.stop()
