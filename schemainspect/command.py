import argparse
import sys

from sqlbag import S

from .get import get_inspector
from .misc import quoted_identifier
from .tableformat import t


def parse_args(args):
    parser = argparse.ArgumentParser(description="Inspect a schema")
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    parser_deps = subparsers.add_parser("deps", help="Show inspected dependencies")
    parser_deps.add_argument("db_url", help="URL")
    return parser.parse_args(args)


def do_deps(db_url):
    with S(db_url) as s:
        i = get_inspector(s)
        deps = i.deps

    def process_row(dep):
        depends_on = quoted_identifier(dep.name, dep.schema, dep.identity_arguments)
        thing = quoted_identifier(
            dep.name_dependent_on,
            dep.schema_dependent_on,
            dep.identity_arguments_dependent_on,
        )

        return dict(
            thing="{}: {}".format(dep.kind_dependent_on, thing),
            depends_on="{}: {}".format(dep.kind, depends_on),
        )

    deps = [process_row(_) for _ in deps]

    rows = t(deps)

    if rows:
        print(rows)
    else:
        print("No dependencies found.")


def run(args):
    if args.command == "deps":
        do_deps(args.db_url)


def do_command():  # pragma: no cover
    args = parse_args(sys.argv[1:])
    status = run(args)
    sys.exit(status)
