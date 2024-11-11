import os
import boto3
import pandas as pd
import duckdb as con
import pyarrow as pa
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from deltalake.writer import write_deltalake
from python.br.com.neogrid.dataload.utils.Functions import read_object_obs, replacements_parameters, get_schema_from_table
from python.br.com.neogrid.dataload.utils.Parameters import Parameters
from python.br.com.neogrid.dataload.CustomLogger import CustomLogger
from python.br.com.neogrid.dataload.model.Configuration import Load, Output, Transform, Delete


class Obs:

    @staticmethod
    def load_from_obs(load: Load, parameters: Parameters, custom_logger: CustomLogger) -> None:

        con.connect(config={"allow_unsigned_extensions": "true"})
        con.load_extension('/app/main/libs/httpfs/httpfs.duckdb_extension')
        con.execute(f"""CREATE or replace SECRET obs_dev (TYPE S3, KEY_ID '{os.getenv("OBS_ACCESS_KEY_ID")}',
            SECRET '{os.getenv("OBS_SECRET_ACCESS_KEY")}',
            REGION '{os.getenv("OBS_REGION")}',
            ENDPOINT '{os.getenv("OBS_ENDPOINT")}')""")

        if load.method and load.method.lower() == "read":
            pathfiles = f"s3://{load.bucket}/{load.pathFiles}"
            load_format = load.format.lower() if load.format else None

            message2 = f"\nFazendo leitura do arquivo / path: {pathfiles}"
            custom_logger.log_custom_with_message(
                message2, custom_logger.return_type_level("INFO"))

            if load_format == "csv":
                table_arrow = con.execute(
                    f"SELECT * FROM read_csv_auto('{pathfiles}')").arrow()
            elif load_format == "parquet":
                table_arrow = con.execute(
                    f"SELECT * FROM read_parquet('{pathfiles}')").arrow()
            elif load_format == "json":
                table_arrow = con.execute(
                    f"SELECT * FROM read_json_auto('{pathfiles}')").arrow()
            elif load_format == "delta":
                storage_options = {
                    "access_key_id": os.getenv("OBS_ACCESS_KEY_ID"),
                    "secret_access_key": os.getenv("OBS_SECRET_ACCESS_KEY"),
                    "endpoint_url": os.getenv("OBS_ENDPOINT_URL"),
                    "region": os.getenv("OBS_REGION"),
                    "AWS_S3_ALLOW_UNSAFE_RENAME": 'true'
                }
                table_arrow = DeltaTable(
                    pathfiles, storage_options=storage_options).to_pyarrow_table()

            else:
                message = "Formato de arquivo não suportado"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)

            con.register(load.tempView, table_arrow)

        elif load.method and load.method.lower() == "query":
            load_format = load.format.lower() if load.format else None
            query_load = load.queryLoad if load.queryLoad else None

            if query_load == None:
                message = "QueryLoad não declarada para uso no método 'query'."
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)
                    
            result = replacements_parameters(
                parameters, query_load) if parameters.parameters else query_load
            
            message2 = f"\nFazendo leitura da query / query: {result}"
            custom_logger.log_custom_with_message(
                message2, custom_logger.return_type_level("INFO"))
            
            if load_format == "delta":
                 table_arrow = con.execute(
                    f"{result}").arrow()
            else:
                message = "Formato do tipo do arquivo para query não suportado"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)
                 
            con.register(load.tempView, table_arrow)

        elif load.method and load.method.lower() == "create_table":
            message = f"Fazendo a leitura do arquivo para query: {load.pathFiles}"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("INFO"))

            obs_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv("OBS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("OBS_SECRET_ACCESS_KEY"),
                endpoint_url=os.getenv("OBS_ENDPOINT_URL"),
                region_name=os.getenv("OBS_REGION")
            )

            query = read_object_obs(load.bucket, load.pathFiles, obs_client)
            result = replacements_parameters(
                parameters, query) if parameters.parameters else query

            if "create table" in result.lower():
                path_delta = f"s3://{load.bucket}/{load.createTableLocation}/{load.table}"
                con.execute(result)
                schema_info = con.execute(f"DESCRIBE {load.table};").fetchall()
                schema = get_schema_from_table(schema_info)

                columns_sql = [field.name for field in schema]
                table = pa.Table.from_pandas(pd.DataFrame(
                    columns=columns_sql), schema=schema, preserve_index=False)

                storage_options = {
                    "access_key_id": os.getenv("OBS_ACCESS_KEY_ID"),
                    "secret_access_key": os.getenv("OBS_SECRET_ACCESS_KEY"),
                    "endpoint_url": os.getenv("OBS_ENDPOINT_URL"),
                    "region": os.getenv("OBS_REGION"),
                    "AWS_S3_ALLOW_UNSAFE_RENAME": 'true'
                }

                write_deltalake(path_delta, table,
                                storage_options=storage_options)
            else:
                con.execute(result)

        else:
            message = "Método de carga não suportado"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("ERROR"))
            raise ValueError(message)

    @staticmethod
    def output_obs(output: Output, custom_logger: CustomLogger) -> None:
        pathfiles = f"s3://{output.bucket}/{output.pathOutputFiles}"

        message = f"\nIniciando a escrita no path: {output.pathOutputFiles}"
        custom_logger.log_custom_with_message(
            message, custom_logger.return_type_level("INFO"))

        output_format = output.format.lower() if output.format else None
        overwrite_clause = " (OVERWRITE)" if output.mode == "overwrite" else ""

        if output_format == "csv":
            if output.partitionedBy:
                partition_clause = f", PARTITION_BY ({', '.join(output.partitionedBy)})"
                sql_command = f"COPY (SELECT * FROM {output.tempViewToWrite}) TO '{pathfiles}' (FORMAT CSV{partition_clause}{overwrite_clause})"
                con.execute(sql_command)
            else:
                sql_command = f"COPY (SELECT * FROM {output.tempViewToWrite}) TO '{pathfiles}' (FORMAT CSV {overwrite_clause})"
                con.execute(sql_command)
        elif output_format == "parquet":
            if output.partitionedBy:
                partition_clause = f", PARTITION_BY ({', '.join(output.partitionedBy)})"
                sql_command = f"COPY (SELECT * FROM {output.tempViewToWrite}) TO '{pathfiles}' (FORMAT PARQUET{partition_clause}{overwrite_clause})"
                con.execute(sql_command)
            else:
                con.execute(
                    f"COPY (SELECT * FROM {output.tempViewToWrite}) TO '{pathfiles}' (FORMAT PARQUET{overwrite_clause})")
        elif output_format == "json":
            if output.partitionedBy:
                partition_clause = f", PARTITION_BY ({', '.join(output.partitionedBy)})"
                sql_command = f"COPY (SELECT * FROM {output.tempViewToWrite}) TO '{pathfiles}' (FORMAT JSON{partition_clause}{overwrite_clause})"
                con.execute(sql_command)
            else:
                con.execute(
                    f"COPY (SELECT * FROM {output.tempViewToWrite}) TO '{pathfiles}' (FORMAT JSON{overwrite_clause})")
        elif output_format == "delta":
            table_arrow = con.table(output.tempViewToWrite).arrow()

            storage_options = {
                "access_key_id": os.getenv("OBS_ACCESS_KEY_ID"),
                "secret_access_key": os.getenv("OBS_SECRET_ACCESS_KEY"),
                "endpoint_url": os.getenv("OBS_ENDPOINT_URL"),
                "region": os.getenv("OBS_REGION"),
                "AWS_S3_ALLOW_UNSAFE_RENAME": 'true'
            }

            if output.partitionedBy:
                write_deltalake(pathfiles, table_arrow, partition_by=output.partitionedBy,
                                mode=output.mode, storage_options=storage_options)
            else:
                write_deltalake(pathfiles, table_arrow, mode=output.mode,
                                storage_options=storage_options)

        else:
            message = "Formato de arquivo não suportado"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("ERROR"))
            raise ValueError(message)

    @staticmethod
    def delete_data_obs(delete: Delete, parameters: Parameters, custom_logger: CustomLogger) -> None:

        con.connect(config={"allow_unsigned_extensions": "true"})
        con.load_extension('/app/main/libs/httpfs/httpfs.duckdb_extension')

        pathfiles = f"s3://{delete.bucket}/{delete.pathFilesToDelete}"
        load_format = delete.format.lower() if delete.format else None

        message2 = f"\niniciando processo de delete - path: {pathfiles}"
        custom_logger.log_custom_with_message(
            message2, custom_logger.return_type_level("INFO"))

        field_filter = delete.fieldToFilter if delete.fieldToFilter else None
        method_get_values = delete.methodGetValues if delete.methodGetValues else None

        if method_get_values == 'list':
            values_list = delete.listValues if delete.listValues else None
            values_filter = ", ".join(f"'{value}'" for value in values_list)

        elif method_get_values == "query":
            query = delete.queryValues if delete.queryValues else None
            result = replacements_parameters(
                parameters, query) if parameters.parameters else query
            
            df = con.execute(result).fetchdf()
            values_list = df.iloc[:, 0].tolist() if not df.empty else None
            values_filter = ", ".join(f"'{value}'" for value in values_list)

        else:
            message = "Metodo Get de Values para delete não suportado"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("ERROR"))
            raise ValueError(message)        

        if load_format == "delta":
            storage_options = {
                "access_key_id": os.getenv("OBS_ACCESS_KEY_ID"),
                "secret_access_key": os.getenv("OBS_SECRET_ACCESS_KEY"),
                "endpoint_url": os.getenv("OBS_ENDPOINT_URL"),
                "region": os.getenv("OBS_REGION"),
                "AWS_S3_ALLOW_UNSAFE_RENAME": 'true'
            }

            try:
                deltaTable = DeltaTable(
                    pathfiles, storage_options=storage_options)
                metadata_delete = deltaTable.delete(f"{field_filter} in ({values_filter})")

            except TableNotFoundError:
                metadata_delete = f"A tabela delta '{pathfiles}' não existe para delete."
            except Exception as e:
                message = f"Error no processo de delete da tabela '{pathfiles}': {e}"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)
            
            custom_logger.log_custom_with_message(
                    f'delete_results: {str(metadata_delete)}', 
                    custom_logger.return_type_level("INFO")
                )  

        else:
            message = "Formato de arquivo não suportado"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("ERROR"))
            raise ValueError(message)


    @staticmethod
    def transform_data_obs(transform: Transform, parameters: Parameters, custom_logger: CustomLogger) -> None:
        obs_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("OBS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("OBS_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("OBS_ENDPOINT_URL"),
            region_name=os.getenv("OBS_REGION")
        )

        query = read_object_obs(
            transform.bucket, transform.pathQuery, obs_client)
        result = replacements_parameters(
            parameters, query) if parameters.parameters else query

        message = f"\nIniciando o processo de transformação: {transform.pathQuery}"
        custom_logger.log_custom_with_message(
            message, custom_logger.return_type_level("INFO"))

        table_arrow = con.execute(result).arrow()

        if transform.tempView:
            con.register(transform.tempView, table_arrow)

        if transform.output:
            output_action = transform.output.lower()
            if output_action == "show":
                df = table_arrow.to_pandas()
                print(df.head(5))
            elif output_action == "printschema":
                print(table_arrow.schema)
            elif output_action is None or output_action == "none":
                pass 
            else:
                message = "Saída não reconhecida"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)
