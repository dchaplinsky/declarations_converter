import os
import os.path
import sys
import glob
import json
import logging
import jmespath

logger = logging.getLogger('converter')

class ConverterError(Exception):
    pass


class Converter(object):
    def __init__(self, src):
        self.src = src

        if (src['id'].lower().startswith('nacp_')):
            raise ConverterError(
                "Declaration {} already has new format".format(src["id"]))

    def _jsrch(self, path, doc=None):
        if doc is None:
            doc = self.src

        return jmespath.search(path, doc)

    def _meta_information(self):
        new_doc = self._convert_using_rules(
            [
                ("id", "id", ""),
                ("declaration.date", "created_date", ""),
                ("declaration.date", "lastmodified_date", ""),
                (None, "data", {})
            ]
        )

        return new_doc

    def _convert_using_rules(self, rules):
        subdoc = {}

        for oldpath, new_key, default in rules:
            if not oldpath:
                subdoc[new_key] = default
            else:
                subdoc[new_key] = self._jsrch(oldpath) or default

        return subdoc

    def _convert_step0(self):
        extract = self._convert_using_rules(
            [
                ("intro.declaration_type", "declarationType", "1"),
                ("intro.declaration_year", "declarationYear1", "")
            ]
        )

        return {
            "step_0": extract
        }

    def _convert_step1(self):
        extract = self._convert_using_rules(
            [
                ("general.name", "firstname", ""),
                ("general.last_name", "lastname", ""),
                ("general.patronymic", "middlename", ""),
                ("general.post.region", "region_declcomua", ""),
                ("general.post.post", "workPost", ""),
                ("general.post.office", "workPlace", ""),
                (None, "place_of_living_declcomua", []),
            ]
        )

        # TODO: Parse into a NACP structure
        for addr in self._jsrch("general.addresses") or []:
            extract["place_of_living_declcomua"].append(
                ", ".join(filter(None, [
                    addr.get("place"),
                    addr.get("place_district"),
                    addr.get("place_city"),
                    addr.get("place_address"),
                ]))
            )

        return {
            "step_1": extract
        }

    def convert(self):
        new_doc = self._meta_information()

        # Basic information
        new_doc["data"].update(self._convert_step0())

        # Information on the declarant
        new_doc["data"].update(self._convert_step1())

        # Filling in the empty spaces
        new_doc["data"].update({
            "step_4": {},   # Об'єкти незавершеного будівництва
            "step_5": {},   # Бенефіціарна власність
            "step_9": {},   # Ціне рухоме майно
            "step_10": {},  # Нематеріальні активи
            "step_14": {},  # Видатки та правочини
            "step_15": {},  # Робота за сумісництвом
            "step_16": {},  # Членство декларанта в організаціях та їх органах
        })

        return new_doc


if __name__ == '__main__':
    if len(sys.argv) < 3:
        logger.error(
            "You should provide two params: input and output directories")
        exit()

    # TODO: check if both exists?
    in_dir = sys.argv[1]
    out_dir = sys.argv[2]

    for i, file_name in enumerate(glob.iglob(in_dir + '/*/' + "*.json")):
        basename = os.path.basename(file_name)
        subdir = os.path.dirname(file_name).split("/")[-1]
        new_file = os.path.join(out_dir, subdir, basename)

        with open(file_name, 'r') as infile:
            try:
                old_json = json.load(infile)
            except json.decoder.JSONDecodeError:
                logger.error('Empty or broken file: {}'.format(file_name))
                continue

            try:
                conv = Converter(old_json)

                os.makedirs(os.path.dirname(new_file), exist_ok=True)
                with open(new_file, "w") as fp:
                    json.dump(conv.convert(), fp, indent=4, ensure_ascii=False)

            # TODO: also intercept FS errors?
            except ConverterError as e:
                logger.error('Cannot convert file {}: {}'.format(
                    file_name, str(e)))
                continue

        if i and i % 100 == 0:
            logger.info("Processed {} declarations".format(i + 1))

            # TODO: remove
            break
