"""Publish an accepted Spark package; no raw events or public service dependency."""
import argparse
import json
from pathlib import Path

from snow_statistics.publication import connect, publication_database, publish, validate

parser = argparse.ArgumentParser()
parser.add_argument("package", type=Path)
parser.add_argument("--lock-directory", type=Path, default=Path("runtime/publication"))
parser.add_argument("--init-schema", action="store_true")
parser.add_argument("--fail-after-load", action="store_true")
args = parser.parse_args()
package = json.loads(args.package.read_text(encoding="utf-8"))
manifest = validate(package)[0]
database = publication_database(manifest["source"])
with connect() as db:
    if args.init_schema:
        with db.cursor() as cursor:
            for statement in (Path(__file__).resolve().parents[1] / "warehouse/doris/publication.sql").read_text().split(";"):
                if statement.strip():
                    cursor.execute(statement.replace("snow.", database + ".").replace("EXISTS snow", "EXISTS " + database))
    print(json.dumps(publish(db, package, args.lock_directory, args.fail_after_load), indent=2))
