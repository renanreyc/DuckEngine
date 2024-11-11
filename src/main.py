import sys
from python.br.com.neogrid.dataload.Application import main as exec_main

# criar meotodo para chamar a aplicação
# usar try igual o do flask


def duckdb_engine(config):
    try:
        if len(config) == 2:
            status_exec = exec_main(['--config', config, '--master', 'obs'])
        else:
            status_exec = exec_main(config)
        return status_exec
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


# preciso chamar o metododo e atribuir a main com o if
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python main.py <config>")
        sys.exit(1)
    config = sys.argv[1:]
    response = duckdb_engine(config)
    print(response)
