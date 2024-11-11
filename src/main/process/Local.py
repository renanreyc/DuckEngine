from python.br.com.neogrid.dataload.utils.Functions import read_object_local, replacements_parameters
from python.br.com.neogrid.dataload.utils.Parameters import Parameters
from python.br.com.neogrid.dataload.CustomLogger import CustomLogger
from python.br.com.neogrid.dataload.model.Configuration import Load, Output, Transform
import duckdb as con


class Local:

    @staticmethod
    def load_from_local(load: Load, parameters: Parameters, custom_logger: CustomLogger):
        if load.method and load.method.lower() == "read":
            message = f"Fazendo a leitura do arquivo para data frame: {load.pathFiles}"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("INFO"))

            if load.format:
                load_format = load.format.lower()
                if load_format == "csv":
                    df = con.execute(
                        f"SELECT * FROM read_csv_auto('{load.pathFiles}')").fetchdf()
                elif load_format == "parquet":
                    df = con.execute(
                        f"SELECT * FROM read_parquet('{load.pathFiles}')").fetchdf()
                else:
                    message = "Formato de arquivo não suportado"
                    custom_logger.log_custom_with_message(
                        message, custom_logger.return_type_level("ERROR"))
                    raise ValueError("Formato de arquivo não suportado")

            con.register(load.tempView, df)

        elif load.method and load.method.lower() == "query":
            message = f"Fazendo a leitura do arquivo para data query: {load.pathQuery}"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("INFO"))

            query = read_object_local(load.pathQuery)
            result = replacements_parameters(
                parameters, query) if parameters.parameters else query
            con.execute(result).fetchdf()

        else:
            message = "Método de carga não suportado"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("ERROR"))
            raise ValueError("Método de carga não suportado")

    @staticmethod
    def output_local(output: Output, custom_logger: CustomLogger):
        df = con.table(output.tempViewToWrite)

        message = f"Iniciando a escrita no path: {output.pathOutputFiles}"
        custom_logger.log_custom_with_message(
            message, custom_logger.return_type_level("INFO"))

        output_format = output.format.lower()

        if output_format == "csv":
            if output.partitionedBy:
                partition_clause = f", PARTITION_BY({', '.join(output.partitionedBy)})"
                overwrite_clause = ", OVERWRITE_OR_IGNORE" if output.mode == "overwrite" else ""
                sql_command = f"COPY(SELECT * FROM {output.tempViewToWrite}) TO '{output.pathOutputFiles}' (FORMAT {output_format.upper()}{partition_clause}{overwrite_clause})"
                con.execute(sql_command)
            df.to_csv(output.pathOutputFiles)
        elif output_format == "parquet":
            if output.partitionedBy:
                partition_clause = f", PARTITION_BY({', '.join(output.partitionedBy)})"
                overwrite_clause = ", OVERWRITE_OR_IGNORE" if output.mode == "overwrite" else ""
                sql_command = f"COPY(SELECT * FROM {output.tempViewToWrite}) TO '{output.pathOutputFiles}' (FORMAT {output_format.upper()}{partition_clause}{overwrite_clause})"
                con.execute(sql_command)
            df.to_parquet(output.pathOutputFiles)
        else:
            message = "Formato de arquivo não suportado"
            custom_logger.log_custom_with_message(
                message, custom_logger.return_type_level("ERROR"))
            raise ValueError("Formato de arquivo não suportado")

    @staticmethod
    def transform_data(transform: Transform, parameters: Parameters, custom_logger: CustomLogger):
        query = read_object_local(transform.pathQuery)
        result = replacements_parameters(
            parameters, query) if parameters.parameters else query

        message = "Iniciando o processo de transformação"
        custom_logger.log_custom_with_message(
            message, custom_logger.return_type_level("INFO"))

        df = con.execute(result).fetchdf()

        con.register(transform.tempView, df)

        if transform.output:
            output_action = transform.output.lower()
            if output_action == "show":
                print(df)
            elif output_action == "printschema":
                print(df.dtypes)
            else:
                message = "Saída não reconhecida"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError("Saída não reconhecida")
