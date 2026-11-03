import argparse
import json
from pathlib import Path

from queries import QUERIES, prepare_views


SCHEMAS = {
    'akas': ('title.akas',
        'titleId STRING, ordering INT, title STRING, region STRING, '
        'language STRING, types STRING, attributes STRING, isOriginalTitle INT'),
    'basics': ('title.basics',
        'tconst STRING, titleType STRING, primaryTitle STRING, originalTitle STRING, '
        'isAdult INT, startYear INT, endYear INT, runtimeMinutes INT, genres STRING'),
    'names': ('name.basics',
        'nconst STRING, primaryName STRING, birthYear INT, deathYear INT, '
        'primaryProfession STRING, knownForTitles STRING'),
    'crew': ('title.crew', 'tconst STRING, directors STRING, writers STRING'),
    'episodes': ('title.episode',
        'tconst STRING, parentTconst STRING, seasonNumber INT, episodeNumber INT'),
    'principals': ('title.principals',
        'tconst STRING, ordering INT, nconst STRING, category STRING, '
        'job STRING, characters STRING'),
    'ratings': ('title.ratings',
        'tconst STRING, averageRating DOUBLE, numVotes LONG'),
}


def read_data(spark, path, schema):
    return spark.read.csv(str(path), schema=schema, header=True, sep='\t',
                          nullValue='\\N', quote='', mode='FAILFAST')


def write_data(frame, path):
    frame.write.mode('overwrite').option('header', True).option('sep', '\t') \
        .option('nullValue', '\\N').option('quote', '').csv(str(path))


def show_saved(output, tasks):
    summary = json.loads((output / 'summary.json').read_text(encoding='utf-8'))
    previews = json.loads((output / 'preview.json').read_text(encoding='utf-8'))
    print(f"PySpark {summary['spark']} | IMDb ETL | full archives")
    for number in tasks:
        print(f"\nTask {number}: {summary['tasks'][str(number)]['rows']:,} rows")
        for row in previews[str(number)][:3]:
            print(' | '.join(f'{key}={value}' for key, value in row.items()))
    print('\nAll result rows saved as TSV in results/task1 ... task8')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', type=Path, default=Path('imdb-data'))
    parser.add_argument('--output', type=Path, default=Path('results'))
    parser.add_argument('--cache', type=Path, default=Path('cache'))
    parser.add_argument('--tasks', default='1,2,3,4,5,6,7,8')
    parser.add_argument('--view')
    args = parser.parse_args()
    tasks = [int(x) for x in (args.view or args.tasks).split(',')]
    if not tasks or any(x not in QUERIES for x in tasks):
        parser.error('tasks must be selected from 1 through 8')
    if args.view:
        show_saved(args.output, tasks)
        return

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[4]').appName('IMDb ETL')
             .config('spark.driver.memory', '4g')
             .config('spark.driver.host', '127.0.0.1')
             .config('spark.driver.bindAddress', '127.0.0.1')
             .config('spark.sql.shuffle.partitions', '32')
             .config('spark.sql.adaptive.enabled', 'true')
             .config('spark.ui.showConsoleProgress', 'false')
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    args.output.mkdir(parents=True, exist_ok=True)
    args.cache.mkdir(parents=True, exist_ok=True)
    summary_path = args.output / 'summary.json'
    preview_path = args.output / 'preview.json'
    summary = json.loads(summary_path.read_text(encoding='utf-8')) \
        if summary_path.exists() else {'spark': spark.version, 'inputs': {}, 'tasks': {}}
    previews = json.loads(preview_path.read_text(encoding='utf-8')) \
        if preview_path.exists() else {}
    try:
        spark.sql("SELECT * FROM VALUES (1, 'ETL'), (2, 'Spark') AS t(id, name)").show()
        for alias, (filename, schema) in SCHEMAS.items():
            source = args.data_dir / f'{filename}.tsv.gz'
            stat = source.stat()
            fingerprint = {'bytes': stat.st_size, 'mtime_ns': stat.st_mtime_ns}
            cached = args.cache / alias
            manifest = args.cache / f'{alias}.json'
            if not manifest.exists() or not (cached / '_SUCCESS').exists() \
                    or json.loads(manifest.read_text()) != fingerprint:
                print(f'Extract: {source.name}', flush=True)
                read_data(spark, source, schema).repartition(16) \
                    .write.mode('overwrite').parquet(str(cached))
                manifest.write_text(json.dumps(fingerprint), encoding='utf-8')
            frame = spark.read.parquet(str(cached))
            frame.createOrReplaceTempView(alias)
            summary['inputs'][alias] = {'file': source.name, 'rows': frame.count()}
            print(f"{alias}: {summary['inputs'][alias]['rows']:,} rows", flush=True)
        prepare_views(spark)
        for number in tasks:
            print(f'\nTask {number}', flush=True)
            result = spark.sql(QUERIES[number])
            path = args.output / f'task{number}'
            write_data(result, path)
            saved = read_data(spark, path, result.schema)
            rows = saved.count()
            order = result.columns[:1]
            if number == 2:
                order = ['birthYear', 'nconst']
            elif number in (3, 5, 6):
                order = [saved.columns[-1]]
            elif number == 7:
                order = ['decade', 'rank']
            elif number == 8:
                order = ['genre', 'rank']
            shown = saved.orderBy(*order, ascending=number not in (3, 5, 6))
            if number == 7:
                shown = saved.orderBy(saved.decade.desc(), saved.rank.asc())
            sample = [row.asDict() for row in shown.limit(5).collect()]
            if number in (5, 6):
                assert rows <= (100 if number == 5 else 50)
            if number in (7, 8):
                assert saved.filter('rank < 1 OR rank > 10').count() == 0
            summary['tasks'][str(number)] = {'rows': rows, 'columns': result.columns}
            previews[str(number)] = sample
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2),
                                    encoding='utf-8')
            preview_path.write_text(json.dumps(previews, ensure_ascii=False, indent=2),
                                    encoding='utf-8')
            print(f'Rows saved: {rows:,}', flush=True)
            shown.show(5, truncate=48)
        print('ETL completed successfully.', flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
