import click

import json

from compgraph.algorithms import word_count_graph


def run_word_count(input_filepath: str, output_filepath: str) -> None:
    graph = word_count_graph(input_filepath, read_from_file=True)
    result = graph.run()
    with open(output_filepath, 'w') as out:
        json.dump(list(result), out)


@click.command()
@click.argument('input_filepath', nargs=1)
@click.argument('output_filepath', nargs=1)
def main(input_filepath: str, output_filepath: str) -> None:
    run_word_count(input_filepath, output_filepath)


if __name__ == '__main__':
    main()
