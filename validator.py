import copy
import glob
import jsonschema
import multiprocessing
import logging
import logging.handlers
import pandas
import ujson as json


LOG_FILE = 'rules.log'
LOG_FILE_OUTPUT_FORMAT = '%(message)s'
LOG_LVL = logging.INFO
FILE_NAME_TEMPLATE = '*.json'
DATA_DIR = 'data/'
TEST_DIR = 'test/'



def clear_file(fp):
    fp.seek(0)
    fp.truncate()


def listener_configurator():
    # Listener process configurator
    root = logging.getLogger()
    h = logging.FileHandler(LOG_FILE, 'w')
    f = logging.Formatter(LOG_FILE_OUTPUT_FORMAT, datefmt='%d/%m/%Y %H:%M:%S')
    h.setFormatter(f)
    root.addHandler(h)


def listener_process(queue, logging_configurator):
    # Process that logs work from different worker processes
    logging_configurator()

    while True:
        try:
            message = queue.get()
            if message is None:
                break
            logger = logging.getLogger()
            logger.handle(message)
        except Exception:
            import sys, traceback
            traceback.print_exc(file=sys.stderr)


def worker_configurator(queue):
    # Worker logging process configurator
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(LOG_LVL)


def worker_process(bucket_name,
                   validation_schema,
                   queue,
                   logging_configurator):
    logging_configurator(queue)
    json_files_iterator = glob.iglob(TEST_DIR +
                                     bucket_name +
                                     '*/' +
                                     FILE_NAME_TEMPLATE)
    for json_file_name in json_files_iterator:
        with open(json_file_name, 'r+') as fp:
            # Sometimes shit happens
            json_data = json.load(fp)
            if not json_data: 
                continue

            declaration_id = json_data.get('id')

            # Preprocess check to filter bad or no-needed json files
            try:
                data = json_data['data']
            except KeyError:
                continue

            if 'step_0' not in data or \
                    'changesYear' in data['step_0'] or \
                    'declarationType' not in data['step_0']:
                continue
            try:
                v = jsonschema.Draft4Validator(json.loads(validation_schema))
                for error in sorted(v.iter_errors(json_data), key=str):
                    if len(list(error.absolute_path)) == 3:
                        path_to_error = '{0},{1},_,{2}'.format(*list(error.absolute_path))
                    else:
                        path_to_error = '{0},{1},*,{3}'.format(*list(error.absolute_path))
                    logging.info('{0},{1}'.format(path_to_error,
                                                   error.validator))
            except jsonschema.SchemaError as e:
                pass
            except OSError as e:
                pass



def main():
    queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(target=listener_process,
                                       args=(queue, listener_configurator))
    listener.start()
    workers = []

    with open("schema.json") as schema:
        validation_schema = schema.read()

    for i in range(1, 5):
        bucket_name = 'bucket' + str(i) + '/'
        worker = multiprocessing.Process(target=worker_process,
                                         args=(bucket_name,
                                               validation_schema,
                                               queue,
                                               worker_configurator))
        workers.append(worker)
        worker.start()

    for w in workers:
        w.join()
    queue.put_nowait(None)
    listener.join()

    print('Log is formed. Starting to make rules...')
    with open(LOG_FILE, 'r+') as log_file:
        try:
            df = pandas.read_csv(log_file, header=None)
            # we are interested in grouping by forth and second columns
            df.drop_duplicates([3,1], inplace=True)
            clear_file(log_file)
            df.to_csv(log_file, header=None, float_format=True, index=False)
        except (pandas.errors.EmptyDataError,
                pandas.errors.ParserError) as exception:
                print(repr(exception))

    print('Done.')

if __name__ == '__main__':
    main()