from python.br.com.neogrid.dataload.utils.Parameters import Parameters
from python.br.com.neogrid.dataload.CustomLogger import CustomLogger
from python.br.com.neogrid.dataload.model.Configuration import Configuration
from python.br.com.neogrid.dataload.process.Local import Local
from python.br.com.neogrid.dataload.process.Obs import Obs
import duckdb as con


class Process:

    @staticmethod
    def data_load(jsonroot: Configuration,  parameters: Parameters, custom_logger: CustomLogger) -> None:
        for load in jsonroot.load:
            source = load.source.lower() if load.source else None
            if source == "local":
                Local.load_from_local(load, parameters, custom_logger)
            elif source == "obs":
                Obs.load_from_obs(load, parameters, custom_logger)
            else:
                message = "Fonte não encontrada no load"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError("Fonte não encontrada no load")

    @staticmethod
    def transform_data(jsonroot: Configuration, parameters: Parameters, custom_logger: CustomLogger) -> None:
        for transform in jsonroot.transform:
            source_query = transform.sourceQuery.lower() if transform.sourceQuery else None
            if source_query == "local":
                Local.transform_data(transform, parameters, custom_logger)
            elif source_query == "obs":
                Obs.transform_data_obs(transform, parameters, custom_logger)
            else:
                message = "Fonte não encontrada na transformação"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)
            
    @staticmethod
    def delete_data(jsonroot: Configuration, parameters: Parameters, custom_logger: CustomLogger) -> None:
        for delete in jsonroot.delete:
            source = delete.source.lower() if delete.source else None
            if source == "obs":
                Obs.delete_data_obs(delete, parameters, custom_logger)
            else:
                message = "source não suportada no delete"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)

    @staticmethod
    def data_output(jsonroot: Configuration, custom_logger: CustomLogger) -> None:
        for output in jsonroot.output:
            source = output.source.lower() if output.source else None
            if source == "local":
                Local.output_local(output, custom_logger)
            elif source == "obs":
                Obs.output_obs(output, custom_logger)
            else:
                message = "Fonte não encontrada no output"
                custom_logger.log_custom_with_message(
                    message, custom_logger.return_type_level("ERROR"))
                raise ValueError(message)

            # talvez precisamos limitar a memoria ler documentação.
            con.execute("PRAGMA memory_limit='10GB'")
