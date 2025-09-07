from typing import Any 


class PipelineBuilderMongoaggregations:
    
    SUM = "$sum"
    AVG = "$avg"
    MIN = "$min"
    MAX = "$max"
    FIRST = "$first"
    LAST = "$last"
    
    def __init__(self):
        self._aggregations = {}

    def sum(self, field: str, alias: str ) -> 'PipelineBuilderMongoaggregations':
        self._aggregations[alias] = {PipelineBuilderMongoaggregations.SUM: f"${field}"}
        return self
    
    def avg(self, field: str, alias: str ) -> 'PipelineBuilderMongoaggregations':
        self._aggregations[alias] = {PipelineBuilderMongoaggregations.AVG: f"${field}"}
        return self
    
    def min(self, field: str, alias: str ) -> 'PipelineBuilderMongoaggregations':
        self._aggregations[alias] = {PipelineBuilderMongoaggregations.MIN: f"${field}"}
        return self 
    
    def max(self, field: str, alias: str) -> 'PipelineBuilderMongoaggregations':
        self._aggregations[alias] = {PipelineBuilderMongoaggregations.MAX: f"${field}"}
        return self
    
    def first(self, field: str, alias: str) -> 'PipelineBuilderMongoaggregations':
        self._aggregations[alias] = {PipelineBuilderMongoaggregations.FIRST: f"${field}"}
        return self

    def last(self, field: str, alias: str) -> 'PipelineBuilderMongoaggregations':
        self._aggregations[alias] = {PipelineBuilderMongoaggregations.LAST: f"${field}"}
        return self

    def count(self, alias: str) -> 'PipelineBuilderMongoaggregations':
        self._aggregations[alias] = {"$sum": 1}
        return self

    def build(self) -> dict:
        return self._aggregations



class PipelineBuilderMongo:
    
    def __init__(self):
        self._stages = []

    def match(self, criteria: dict) -> 'PipelineBuilderMongo':
        self._stages.append({"$match": criteria})
        return self 

    def group(self, _id, aggregations: PipelineBuilderMongoaggregations) -> 'PipelineBuilderMongo':
        stage = {"$group": {"_id": _id}}
        stage["$group"].update(aggregations.build()) 
        self._stages.append(stage)
        return self

    def sort(self, sort_criteria: dict) -> 'PipelineBuilderMongo':
        self._stages.append({"$sort": sort_criteria})
        return self

    def project(self, projection: dict) -> 'PipelineBuilderMongo':
        self._stages.append({"$project": projection})
        return self

    def limit(self, count: int) -> 'PipelineBuilderMongo':
        self._stages.append({"$limit": count})
        return self

    def skip(self, count: int) -> 'PipelineBuilderMongo':
        self._stages.append({"$skip": count})
        return self

    def build(self) -> list[dict]:
        return self._stages
    
    



class ESQueryBuilder:
    def __init__(self):
        self._stages = []

    def match(self, text: str, field: str) -> 'ESQueryBuilder':
        self._stages.append({"match": {field: text}})
        return self

    def term(self, text: str, field: str) -> 'ESQueryBuilder':
        self._stages.append({"term": {field: text}})
        return self

    def multi_match(self, text: str, fields: list[str]) -> 'ESQueryBuilder':
        self._stages.append({"multi_match": {"query": text, "fields": fields}})
        return self

    def exists(self, field: str) -> 'ESQueryBuilder':
        self._stages.append({"exists": {"field": field}})
        return self

    def terms(self, texts: list[str], field: str) -> 'ESQueryBuilder':
        self._stages.append({"terms": {field: texts}})
        return self

    def range(self, gte: int | None = None, lte: int | None = None, field: str | None = None) -> 'ESQueryBuilder':
        range_query = {"range": {field: {}}}
        if gte is not None:
            range_query["range"][field]["gte"] = gte
        if lte is not None:
            range_query["range"][field]["lte"] = lte
        self._stages.append(range_query)
        return self

    def prefix(self, text: str, field: str) -> 'ESQueryBuilder':
        self._stages.append({"prefix": {field: text}})
        return self

    def fuzzy(self, text: str, field: str) -> 'ESQueryBuilder':
        self._stages.append({"fuzzy": {field: text}})
        return self

    def ids(self, ids: list[str]) -> 'ESQueryBuilder':
        self._stages.append({"ids": {"values": ids}})
        return self

    def build(self) -> list[dict]:
        return self._stages


class PipelineBuilderES:
    def __init__(self):
        self._stages = {"bool": {"must": [], "must_not": [], "should": [], "filter": []}}

    def must(self, query: ESQueryBuilder) -> 'PipelineBuilderES':
        self._stages["bool"]["must"].extend(query.build())
        return self

    def must_not(self, query: ESQueryBuilder) -> 'PipelineBuilderES':
        self._stages["bool"]["must_not"].extend(query.build())
        return self

    def filter(self, query: ESQueryBuilder) -> 'PipelineBuilderES':
        self._stages["bool"]["filter"].extend(query.build())
        return self

    def should(self, query: ESQueryBuilder) -> 'PipelineBuilderES':
        self._stages["bool"]["should"].extend(query.build())
        return self
    
    def build(self) -> dict:
        return self._stages


def a