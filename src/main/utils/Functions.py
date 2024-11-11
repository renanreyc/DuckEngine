import os
import re
import json
import boto3
import pyarrow as pa
from typing import Dict
from python.br.com.neogrid.dataload.CustomLogger import CustomLogger
from python.br.com.neogrid.dataload.utils.Parameters import Parameters
from python.br.com.neogrid.dataload.model.Configuration import Configuration, Load, Output, Transform, Delete


def read_json(config: str, parameters: Parameters, custom_logger: CustomLogger) -> Configuration:
    if config.startswith("obs://"):
        obs_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("OBS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("OBS_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("OBS_ENDPOINT_URL"),
            region_name=os.getenv("OBS_REGION")
        )
        obs_pattern = r"^obs://([^/]+)/(.*)"
        match = re.match(obs_pattern, config)
        if match:
            bucket, path = match.groups()
            custom_logger.log_custom_with_message(
                "Fazendo a leitura do json no OBS", custom_logger.return_type_level("INFO"))
            file_contents = read_object_obs(bucket, path, obs_client)
        else:
            custom_logger.log_custom_with_message(
                "Formato OBS inválido.", custom_logger.return_type_level("ERROR"))
            raise ValueError("Formato OBS inválido.")
    else:
        file_contents = read_object_local(config)

    data = json.loads(file_contents)

    load_objs = [Load(**item) for item in data.get('load', [])]
    transform_objs = [Transform(**item) for item in data.get('transform', [])]
    delete_objs = [Delete(**item) for item in data.get('delete', [])]
    output_objs = [Output(**item) for item in data.get('output', [])]

    return Configuration(load=load_objs, transform=transform_objs, output=output_objs, delete=delete_objs)


def replacements_parameters(param: Parameters, query: str) -> str:
    replacements = dict(replacement.split('=', 1) for replacement in param.parameters)
    for key, value in replacements.items():
        query = query.replace(f"${{{key}}}", value)
    return query


def read_object_local(path: str) -> str:
    if not path:
        raise ValueError("Caminho do arquivo inválido")
    try:
        with open(path, 'r') as file:
            return file.read()
    except Exception as ex:
        raise RuntimeError(f"Falha ao ler o arquivo {path}") from ex


def read_object_obs(bucket: str, object_key: str, obs_client) -> str:
    response = obs_client.get_object(Bucket=bucket, Key=object_key)
    content = read_input_stream_to_string(response['Body'])
    response['Body'].close()
    return content


def read_input_stream_to_string(input_stream) -> str:
    return input_stream.read().decode('utf-8')


def read_options_input(input: Load) -> Dict[str, str]:
    return input.options.get().values


def get_schema_from_table(schema_info) -> pa.Schema:
    fields = []
    for column in schema_info:
        col_name = column[0]
        col_type = column[1]

        if col_type in ['INTEGER', 'INT', 'BIGINT']:
            fields.append((col_name, pa.int32()))
        elif col_type in ['VARCHAR', 'TEXT', 'STRING']:
            fields.append((col_name, pa.string()))
        elif col_type == 'FLOAT':
            fields.append((col_name, pa.float32()))
        elif col_type == 'DOUBLE':
            fields.append((col_name, pa.float64()))
        elif col_type == 'BOOLEAN':
            fields.append((col_name, pa.bool_()))

    return pa.schema(fields)
