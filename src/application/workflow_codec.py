"""Explicit-schema workflow JSON codec and SQL row expressions; no inference/I/O."""
from datetime import datetime
from decimal import Decimal
import json

from src.application.workflow_contract import ContractError, _shape
from src.application.workflow_schema import TABLES


def _decode(value, fields):
    if type(value) is not dict or set(value) != {field.name for field in fields}:
        raise ContractError('INVALID_WORKFLOW_STORAGE')
    result = {}
    for field in fields:
        item = value[field.name]
        def scalar(item):
            if item is None and field.mode == 'NULLABLE':
                return None
            if field.field_type == 'RECORD':
                return _decode(item, field.fields)
            if field.field_type == 'TIMESTAMP' and type(item) is str:
                return datetime.fromisoformat(item.replace(' UTC','+00:00').replace('Z','+00:00'))
            if field.field_type == 'INTEGER' and type(item) is str and item.lstrip('-').isdecimal():
                return int(item)
            if field.field_type == 'NUMERIC' and type(item) in (str,int,Decimal):
                return Decimal(str(item))
            if field.field_type == 'FLOAT' and type(item) is Decimal:
                return float(item)
            if field.field_type == 'JSON':
                return json.loads(json.dumps(item, default=lambda value: float(value)))
            return item
        if field.mode == 'REPEATED':
            if type(item) is not list:
                raise ContractError('INVALID_WORKFLOW_STORAGE')
            result[field.name] = [scalar(child) for child in item]
        else:
            result[field.name] = scalar(item)
    return result


def decode_row(text: str, table: str) -> dict:
    try:
        if type(text) is not str or len(text.encode('utf-8')) > 512 * 1024:
            raise ValueError()
        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError()
                result[key] = value
            return result
        def constant(value):
            raise ValueError()
        row = _decode(json.loads(text, object_pairs_hook=pairs, parse_constant=constant,
            parse_float=Decimal), TABLES[table].schema)
        _shape(row, TABLES[table].schema)
        return row
    except (ValueError, TypeError, KeyError, OverflowError):
        raise ContractError('INVALID_WORKFLOW_STORAGE') from None


def encode_row(row: dict, table: str) -> str:
    _shape(row, TABLES[table].schema)
    def scalar(value):
        if isinstance(value, datetime):
            return value.isoformat()
        if type(value) is Decimal:
            return str(value)
        raise TypeError()
    return json.dumps(row, default=scalar, allow_nan=False, separators=(',',':'))


def sql_row(parameter: str, table: str) -> str:
    """Construct a STRUCT with each destination field explicitly typed."""
    if parameter not in {'candidate','audit','workspace','owner','attempt','invocation'}:
        raise ValueError('Fixed SQL parameter name required')
    def fields_expr(fields, source):
        parts=[]
        for field in fields:
            path="'$." + field.name + "'"
            value=f'JSON_VALUE({source}, {path})'
            if field.field_type == 'RECORD':
                if field.mode == 'REPEATED':
                    value=f'ARRAY(SELECT AS STRUCT {fields_expr(field.fields,"item")} FROM UNNEST(JSON_QUERY_ARRAY({source}, {path})) item)'
                else:
                    value=f'STRUCT({fields_expr(field.fields, f"JSON_QUERY({source}, {path})")})'
            elif field.mode == 'REPEATED':
                value=f'JSON_VALUE_ARRAY({source}, {path})'
            elif field.field_type == 'JSON':
                value=f'JSON_QUERY({source}, {path})'
            elif field.field_type != 'STRING':
                kind={'INTEGER':'INT64','BOOLEAN':'BOOL','TIMESTAMP':'TIMESTAMP','NUMERIC':'NUMERIC','FLOAT':'FLOAT64'}[field.field_type]
                value=f'CAST({value} AS {kind})'
            parts.append(f'{value} AS {field.name}')
        return ', '.join(parts)
    return f'STRUCT({fields_expr(TABLES[table].schema, f"PARSE_JSON(@{parameter})")})'
