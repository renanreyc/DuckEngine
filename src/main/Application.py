import sys
import logging
from dotenv import load_dotenv
from python.br.com.neogrid.dataload.utils.Functions import read_json
from python.br.com.neogrid.dataload.utils.Parameters import parse_args, Parameters
from python.br.com.neogrid.dataload.process.Process import Process
import python.br.com.neogrid.dataload.CustomLogger as CustomLogger

load_dotenv()

def main(args):
    logging.basicConfig(level=logging.INFO)
    parameters = parse_args(args)

    if not parameters:
        sys.exit(1)
    
    return run(parameters)


def run(parameters: Parameters):
    logger = logging.getLogger(__name__)
    logger.info("Starting process")
    custom_logger = CustomLogger.CustomLogger()
    config = ''
    try:
        custom_logger.log_custom_with_message(
            "Starting process", custom_logger.return_type_level("INFO"))

        config = read_json(parameters.config, parameters, custom_logger)

        Process.data_load(config, parameters, custom_logger)
        Process.transform_data(config, parameters, custom_logger)
        Process.delete_data(config, parameters, custom_logger)
        Process.data_output(config, custom_logger)

    except Exception as e:
        logger.error(str(e))
        custom_logger.log_custom_with_message(
            str(e), custom_logger.return_type_level("ERROR"))
        return {'status': 'error', 'config': parameters, 'error': str(e)}
    
    return {'status': 'success', 'config': parameters}
        


if __name__ == "__main__":
    main(sys.argv[1:])
