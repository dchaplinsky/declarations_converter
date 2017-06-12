import jmespath
import jsonschema
import json
import sys
import re


class JSONCorrectorError(Exception):
    pass


class JSONCorrector(object):
    """Fixes error in json declaration file according give json schema."""
    schema_file_name = 'schema.json'
    errors_list_file_name = 'errors.log'

    @staticmethod
    def load_schema():
        with open(JSONCorrector.schema_file_name, 'r') as schema:
            return json.loads(schema.read()) 

    @staticmethod
    def get_errors_dict():
        errors = {}
        with open(JSONCorrector.errors_list_file_name, 'r') as errors_list:
            for line in errors_list.readline():
                step_name = line.split('#')[0]
                errors.setdefault(step_name, []).append(line.rstrip())

        return errors

    def __init__(self, src=None):
        if src is None:
            raise JSONCorrectorError("No json file was provided.")

        self.src = src
        self.schema = JSONCorrector.load_schema()
        self._errors = JSONCorrector.get_errors_dict()

    def _jsearch(self, path, doc=None):
        if doc is None:
            doc = self.src

        return jmespath.search(path, doc)

    def fix_it(self):
        new_doc = self.src
        for step_name, step_errors in self._errors.items():
            new_doc['data'].update(self._fix_step(step_name,
                                                  step_errors))

        return new_doc

    def _fix_step(self, step_name, errors_list):
        output = {}
        step_info = self.src['data'][step_name]

        if step_info:
            for step_item_id, step_item_info in step_info.items():
                for error_info in errors_list:
                    path_to_error, error_validator, error_validator_value = \
                        error_info.rsplit('#', 2)

                    field_name = \
                        path_to_error.replace('*', step_item_id).split('#', 2)[-1]

                    origin_value = self._jsearch(field_name, step_item_info)

                    new_value = self._try_to_fix(origin_value,
                                           error_validator,
                                           error_validator_value)

                    step_item_info[field_name] = new_value

                output[step_item_id] = step_item_info
        
        return {
            step_name: output
        }

    def _try_to_fix(self, origin_value, error_validator, error_validator_value):
        JSON_TYPE_MAP = {
            'string': str,
            'number': (int, float),
            'integer': int,
            'object': dict,
            'array': list,
            'boolean': bool,
            'null': None.__class__,
        }

        def _type_fix():
            if not isinstance(origin_value, JSON_TYPE_MAP[error_validator_value]):
                if error_validator_value != 'number':
                    value_class = JSON_TYPE_MAP[error_validator_value]
                else:
                    if isinstance(origin_value, int):
                        value_class = JSON_TYPE_MAP['number'][0]
                    else:
                        value_class = JSON_TYPE_MAP['number'][1]    
                
                try:
                    new_value = value_class(origin_value)
                except ValueError:
                    new_value = value_class(origin_value.replace(',', '.'))
                except Exception:
                    new_value = value_class()

                return new_value

            return origin_value
            
        def _enum_fix():
            validator_value = \
                error_validator_value.replace('[', '').replace(']', '').replace("'",'')
            validator_value = [elem.strip() 
                               for elem in validator_value.split(',')]
            if not (origin_value in validator_value):
                try:
                    item_index = int(origin_value)
                except ValueError:
                    return 'Інше'

                try:
                    new_value = validator_value[item_index]
                except IndexError:
                    return 'Інше'

                return new_value

            return origin_value

        def _oneOf_fix():
            oneOf_schemas = json.loads(error_validator_value.replace("'", '"'))
            for schema in oneOf_schemas:
                if not ('pattern' in schema):
                    try:
                        if isinstance(origin_value,
                                      JSON_TYPE_MAP[(schema['type'])]):
                            return origin_value
                        
                        if schema['type'] != 'number':
                            value_class = JSON_TYPE_MAP[schema['type']]
                        else:
                            value_class = JSON_TYPE_MAP['number'][1]

                        try:
                            new_value = value_class(origin_value)
                        except (TypeError, ValueError):
                            new_value = value_class()
                        finally:
                            return new_value
                    except KeyError:
                        raise JSONCorrectorError('Can not match rule for "oneOf" or no "type" value in given schema')

                if re.search(schema['pattern'], origin_value):
                    return origin_value
                else:
                    continue
            else:
                raise JSONCorrectorError('oneOf converting fail.')


        fix_function = {
            'type': _type_fix,
            'enum': _enum_fix,
            'oneOf': _oneOf_fix
        }
        

        try:
            new_value = fix_function[error_validator]()
        except KeyError as exception:
            print('No resolver for {} is implemented'.format(error_validator))
            return origin_value
        except jsonschema.exceptions.ValidationError:
            print('Validation error.')
            return origin_value

        return new_value


if __name__ == '__main__':
    if not sys.argv[1]:
        print('You forgot to provide json file. Bye.')
        exit()

    with open(sys.argv[1]) as jsonfile: 
        json_to_fix = json.loads(jsonfile.read())

    corrector = JSONCorrector(json_to_fix)
    with open(sys.argv[1].split('.')[0] + '_fixed.json', 'w') as output:
        data = corrector.fix_it()
        json.dump(data, output, ensure_ascii=False, indent=2)
    print('Done.')