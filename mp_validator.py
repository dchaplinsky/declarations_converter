import copy
import glob
import jsonschema
import multiprocessing
import logging
import logging.handlers
import ujson as json


LOG_FILE = 'decla_validation.log'
LOG_FILE_OUTPUT_FORMAT = '%(levelname)s | %(message)s'
LOG_LVL = logging.WARNING
FILE_NAME_TEMPLATE = '*.json'
DATA_DIR = 'data/'
TEST_DIR = 'test/'


def clear_file(fp):
    """Clear file object before rewriting it."""
    fp.seek(0)
    fp.truncate()


def update_nested_dict(d, u, *keys):
    """Update dict with a new value under given 'keys' path."""
    d = copy.deepcopy(d)
    keys = keys[0]
    if len(keys) > 1:
        d[keys[0]] = update_nested_dict(d[keys[0]], u, keys[1:])
    else:
        d[keys[0]] = u
    return d


def try_to_fix(error_instance, json_data, json_file_name):
    """Fix jsonschema error occured during json_data validation process."""
    # from_json - to_python types mapping table
    JSON_TYPE_MAP = {
        'string': str,
        'number': (int, float),
        'integer': int,
        'object': dict,
        'array': list,
        'boolean': bool,
        'null': None.__class__,
    }
    logging.error('File: {0} - Validation error: {1}'.format(
                  json_file_name,
                  error_instance.message))

    def _type_fix():
        if error_instance.validator_value != 'number':
            value_class = JSON_TYPE_MAP[error_instance.validator_value]
        else:
            if isinstance(error_instance.instance, int):
                value_class = JSON_TYPE_MAP['number'][0]
            else:
                value_class = JSON_TYPE_MAP['number'][1]

        try:
            # trying to convert previous value to a new type
            newvalue = value_class(error_instance.instance)
        except Exception:
            newvalue = value_class()
            logging.debug('Could not convert {0} value to {1} type.'
                            'Set to default.'.format(
                                                    error_instance.instance,
                                                    error_instance.validator_value))
        logging.warning('Field "{0}": type {1} '
                        'was changed into <class "{2}"> type.'.format(
                              list(error_instance.path)[-1],
                              str(error_instance.instance.__class__),
                              error_instance.validator_value))
        return newvalue

    def _enum_fix():
        # only to groups have enum fields according jsonschema
        # real_estate and vehicle
        ESTATE_OBJECTS = [
            "Квартира",
            "Земельна ділянка",
            "Житловий будинок",
            "Кімната",
            "Гараж",
            "Садовий (дачний) будинок",
            "Офіс",
            "Інше"]

        VEHICLE_OBJECTS = [
            "Автомобіль легковий",
            "Автомобіль легковий (спеціальні)",
            "Сільськогосподарська техніка",
            "Автомобіль вантажний",
            "Мотоцикл (мопед)",
            "Водний засіб",
            "Повітряне судно",
            "Повітряний засіб",
            "Інше"]

        PAPERS_OBJECTS = [
          "Акції",
          "Інше",
          "Боргові цінні папери",
          "Інвестиційні сертифікати",
          "Чеки",
          "Іпотечні цінні папери",
          "Похідні цінні папери (деривативи)",
          "Приватизаційні цінні папери (ваучери тощо)"]

        try:
            # most commont case for enum errors
            # thers is an index instead of string desciption for field
            item_index = int(error_instance.instance)
        except ValueError:
            logging.debug('Value "{0}": Can not get item_index.'.format(
            error_instance.instance))
            return str(error_instance.instance)

        if 'step_3' in list(error_instance.path):
            list_to_look_up = ESTATE_OBJECTS
        elif 'step_6' in list(error_instance.path):
            list_to_look_up = VENICLE_OBJECTS
        else:
            list_to_look_up = PAPERS_OBJECTS
        
        try:
            newvalue = list_to_look_up[item_index]
        except IndexError:
            logging.debug('"{0}": No such value among possible in "enum" list'.format(
                          error_instance.instance))
            newvalue = 'Інше - {0}'.format(item_index)
        
        logging.warning('Field "{0}": value was changed to "{1}"'.format(
            error_instance.instance,
            newvalue))

        return newvalue

    def _oneOf_fix():
        if not error_instance.validator_value:
            msg = 'Can not get validator_value of error instance object.'
            logging.debug(msg)
            raise jsonschema.exceptions.ValidationError(msg, instance=error_instance)

        # pick up one possible type class among given ones according to 'oneOf' rule
        class_name = error_instance.validator_value[-1]['type']
        value_class = JSON_TYPE_MAP[class_name]

        try:
            newvalue = value_class(error_instance.instance)
        except Exception:
            newvalue = value_class()
            logging.debug('Could not convert {0} value to {1} type.'
                            'Set to default.'.format(
                                error_instance.instance,
                                error_instance.validator_value))

        logging.warning('Field "{0}": type {1} '
                        'was changed into <class "{2}"> type.'.format(
                              list(error_instance.path)[-1],
                              str(error_instance.instance.__class__),
                              class_name))
        return newvalue

    # dict to match needed fix function
    fix_function = {
        'type': _type_fix,
        'enum': _enum_fix,
        'oneOf': _oneOf_fix
    }

    try:
        newvalue = fix_function[error_instance.validator]()
    except KeyError:
        logging.warning(
                'No resolver implemented for {0} error type in {1}'.format(
                            error_instance.validator,
                            json_file_name))
        return json_data
    except jsonschema.exceptions.ValidationError:
        logging.error('{0}: Unhandled Validation Error - {1}'.format(
                            json_file_name,
                            error_instance.validator))
        return json_data

    # path to key where validation error occured
    keys_path = list(error_instance.path)
    # update previous value by given key_path
    fixed_data = update_nested_dict(json_data, newvalue, keys_path)

    return fixed_data


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
    # Get list of all json files by given bucket
    json_files_iterator = glob.iglob(TEST_DIR +
                                     bucket_name +
                                     '*/' +
                                     FILE_NAME_TEMPLATE)
    for json_file_name in json_files_iterator:
        with open(json_file_name, 'r+') as fp:
            # Sometimes shit happens
            json_data = json.load(fp)
            if not json_data: 
                logging.error('File: {0} can not be opened. Skiping it.'.format(json_file_name))
                continue

            declaration_id = json_data.get('id')
            logging.info('File: {0}; id: {1}'.format(json_file_name,
                                                     declaration_id))
            # Preprocess check to filter bad or no-needed json files
            try:
                data = json_data['data']
            except KeyError:
                logging.warning(
                    'File: {0} - API brainfart. Skipping it.'.format(
                        json_file_name))
                continue

            if 'step_0' not in data or \
                    'changesYear' in data['step_0'] or \
                    'declarationType' not in data['step_0']:
                logging.warning(
                    'File: {0} - Bad json. Skipping it.'.format(
                        json_file_name))
                continue
            # Try to fix each error that occured during validation
            # Then save fixed json data to already opened file
            try:
                v = jsonschema.Draft4Validator(json.loads(validation_schema))
                for error in sorted(v.iter_errors(json_data), key=str):
                    result = try_to_fix(error, json_data, json_file_name)
                    json_data = result
                clear_file(fp)
                json.dump(json_data, fp, ensure_ascii=False, indent=4)
            except jsonschema.SchemaError as e:
                logging.critical(
                    '[Schema]: {0} - Schema is not valid. Check it.'.format(e))
            except OSError as e:
                logging.critical(
                    'Can not dump data to file: {0}, OSError: {1}'.format(json_file_name, e))


def main():
    # Queue used to make worker proceesses put their logs messages there
    # where listener process can take them and put into global log.
    queue = multiprocessing.Queue(-1)
    listener = multiprocessing.Process(target=listener_process,
                                       args=(queue, listener_configurator))
    listener.start()
    workers = []

    with open("schema.json") as schema:
        validation_schema = schema.read()
    # Before run we need to split data into 4 bucket folders
    # and feed them to each worker process
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


if __name__ == '__main__':
    main()
