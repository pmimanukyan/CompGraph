import json

from . import Graph, operations


def word_count_graph(input_stream_name: str, text_column: str = 'text',
                     count_column: str = 'count',
                     read_from_file: bool = False) -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    if read_from_file:
        graph = Graph.graph_from_file(input_stream_name, json.loads) \
            .map(operations.FilterPunctuation(text_column)) \
            .map(operations.LowerCase(text_column)) \
            .map(operations.Split(text_column)) \
            .sort([text_column]) \
            .reduce(operations.Count(count_column), [text_column]) \
            .sort([count_column, text_column])
    else:
        graph = Graph.graph_from_iter(input_stream_name) \
            .map(operations.FilterPunctuation(text_column)) \
            .map(operations.LowerCase(text_column)) \
            .map(operations.Split(text_column)) \
            .sort([text_column]) \
            .reduce(operations.Count(count_column), [text_column]) \
            .sort([count_column, text_column])
    return graph


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id',
                         text_column: str = 'text',
                         result_column: str = 'tf_idf',
                         read_from_file: bool = False) -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    if read_from_file:
        split_word = Graph.graph_from_file(input_stream_name, json.loads) \
            .map(operations.LowerCase(text_column)) \
            .map(operations.FilterPunctuation(text_column)) \
            .map(operations.Split(text_column))

        count_docs = Graph.graph_from_file(input_stream_name, json.loads) \
            .map(operations.Project([doc_column])) \
            .reduce(operations.CountUnique(doc_column, 'n_docs'), [])
    else:
        split_word = Graph.graph_from_iter(input_stream_name) \
            .map(operations.LowerCase(text_column)) \
            .map(operations.FilterPunctuation(text_column)) \
            .map(operations.Split(text_column))

        count_docs = Graph.graph_from_iter(input_stream_name) \
            .map(operations.Project([doc_column])) \
            .reduce(operations.CountUnique(doc_column, 'n_docs'), [])

    uniques = split_word.sort([text_column]) \
        .reduce(operations.CountUnique(doc_column, 'uniques'), [text_column]) \
        .sort([text_column])
    tf = split_word.sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column, 'tf'), [doc_column]) \
        .sort([text_column])

    graph = count_docs.join(operations.InnerJoiner(), tf, []) \
        .join(operations.InnerJoiner(), uniques, [text_column]) \
        .map(operations.Divide(nominator='n_docs', denominator='uniques',
                               result='fraction')) \
        .map(operations.Log('fraction', 'log')) \
        .map(operations.Product(['tf', 'log'], result_column=result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([text_column]) \
        .reduce(operations.TopN(column=result_column, n=3), [text_column])
    return graph


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id',
              text_column: str = 'text',
              result_column: str = 'pmi', read_from_file: bool = False) -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""

    if read_from_file:
        words = Graph.graph_from_file(input_stream_name, json.loads) \
            .map(operations.FilterPunctuation(text_column)) \
            .map(operations.LowerCase(text_column)) \
            .map(operations.Split(text_column)) \
            .map(operations.Filter(lambda row: len(row[text_column]) > 4)) \
            .sort([doc_column, text_column])
    else:
        words = Graph.graph_from_iter(input_stream_name) \
            .map(operations.FilterPunctuation(text_column)) \
            .map(operations.LowerCase(text_column)) \
            .map(operations.Split(text_column)) \
            .map(operations.Filter(lambda row: len(row[text_column]) > 4)) \
            .sort([doc_column, text_column])

    filtered = words.sort([doc_column, text_column]) \
        .reduce(operations.Count('count'), [doc_column, text_column]) \
        .map(operations.Filter(lambda row: row['count'] >= 2)) \
        .map(operations.Project([doc_column, text_column])) \
        .join(operations.InnerJoiner(), words, [doc_column, text_column])

    tf = filtered.reduce(operations.TermFrequency(text_column, 'tf'),
                         [doc_column]) \
        .sort([text_column])
    tf_all = filtered.reduce(operations.TermFrequency(text_column, 'tf_all'),
                             []) \
        .sort([text_column])

    graph = tf.join(operations.InnerJoiner(), tf_all, [text_column]) \
        .map(operations.Divide(nominator='tf', denominator='tf_all',
                               result='fraction')) \
        .map(operations.Log(arg='fraction', result=result_column)) \
        .map(operations.Project([doc_column, text_column, result_column])) \
        .sort([doc_column, result_column, text_column]) \
        .reduce(operations.TopN(result_column, 10), [doc_column])

    return graph


def yandex_maps_graph(input_stream_name_time: str,
                      input_stream_name_length: str,
                      enter_time_column: str = 'enter_time',
                      leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id',
                      start_coord_column: str = 'start',
                      end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday',
                      hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed', read_from_file: bool = False) -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""
    time_format = '%Y%m%dT%H%M%S.%f'

    if read_from_file:
        time = Graph.graph_from_file(input_stream_name_time, json.loads) \
            .map(operations.WeekHour(enter_time_column, time_format,
                                     weekday_result_column, hour_result_column)) \
            .sort([edge_id_column])

        distance = Graph.graph_from_file(input_stream_name_length, json.loads) \
            .map(operations.HaversineDistance(start_coord_column, end_coord_column, 'haversine_distance')) \
            .sort([edge_id_column])
    else:
        time = Graph.graph_from_iter(input_stream_name_time) \
            .map(operations.WeekHour(enter_time_column, time_format,
                                     weekday_result_column, hour_result_column)) \
            .sort([edge_id_column])

        distance = Graph.graph_from_iter(input_stream_name_length) \
            .map(operations.HaversineDistance(start_coord_column, end_coord_column, 'haversine_distance')) \
            .sort([edge_id_column])

    graph = time.join(operations.InnerJoiner(), distance, [edge_id_column]) \
        .sort([weekday_result_column, hour_result_column]) \
        .reduce(
        operations.Speed('haversine_distance', enter_time_column, leave_time_column,
                         time_format, speed_result_column),
        [weekday_result_column, hour_result_column])

    return graph
