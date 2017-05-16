# -*- coding: utf-8 -*-

import glob
import json
import sys
import traceback



fields_to_copy = ['id',
          'created_date',
          'lastmodified_date',
          'data',
          'server_info']
"""
Each list of tuples describes every group that should be processed 
during old_format -- new_format json convertation.


Structure in next:

group_name1 = [('old_field_key1', 'new_field_key1', 'default_value1'),
              ('old_field_key2', 'new_field_key2', 'default_value2'),
              ...
              ...
              ('old_field_keyN', 'new_field_keyN', 'default_valueN')]

Every tuple in this list object is a matching table for certain field.

By iterating through this list it retrieves field values 
from 'old_json' dict withing 'old_field_keys' and saves them
to 'new_json' dict with 'new_field_keys'.

If there is no such field in old_dict - 'default_value' will be used.

"""
step_0_fields = [('declaration_type', 'declarationType', 1),
                 ('declaration_year' ,'declarationYear1', 'Щорічна (стара форма)')]
step_1_fields = [('name', 'firstname', ''),
                  ('last_name', 'lastname', ''),
                  ('patronymic', 'middlename', '')]
"""
Some groups have fields that are represented as a dict objects.
That why we need to process them as separated 'inner' group.

group_name1=[...]
group_name1_1=[...]

"""
step_1_1_fields = [('region', 'region_declcomua', ''),
                  ('post', 'workPost', ''),
                  ('office', 'workPlace', '')]
step_2_fields = [('non-key', 'lastname', ''),
            ('non-key', 'firstname', ''),
            ('non-key', 'middlename', ''),
            ('name_hidden', 'changedName', ''),
            ('non-key', 'citizenship', ''),
            ('non-key', 'eng_lastname', ''),
            ('non-key', 'no_taxNumber', ''),
            ('non-key', 'eng_firstname', ''),
            ('non-key', 'eng_middlename', ''),
            ('non-key', 'isNotApplicable', ''),
            ('relations', 'subjectRelation', ''),
            ('non-key', 'previous_lastname', ''),
            ('non-key', 'previous_firstname', ''),
            ('non-key', 'previous_middlename', ''),
            ('non-key', 'previous_eng_lastname', ''),
            ('non-key', 'previous_eng_firstname', ''),
            ('non-key', 'previous_eng_middlename', '')]
step_3_fields = [('non-key', 'city', ''),
            ('non-key', 'person', ''),
            ('region', 'place_oblast_declcomua', ''),
            ('address', 'place_address_declcomua', ''),
            ('non-key', 'rights', ''),
            ('non-key', 'country', ''),
            ('non-key', 'cityPath', ''),
            ('costs', 'costDate', ''),
            ('non-key', 'district', ''),
            ('non-key', 'postCode', ''),
            ('non-key', 'iteration', ''),
            ('space', 'totalArea', ''),
            ('non-key', 'ua_street', ''),
            ('non-key', 'objectType', ''),
            ('non-key', 'owningDate', ''),
            ('non-key', 'objectType', ''),
            ('non-key', 'ua_cityType', ''),
            ('non-key', 'ua_postCode', ''),
            ('address', 'ua_streetType', ''),
            ('non-key', 'costAssessment', ''),
            ('non-key', 'otherObjectType', ''),
            ('costs_rent', 'costRent_declcomua', ''),
            ('non-key', 'costAssessment_extendedstatus', ''),
            ('non-key', 'ua_housePartNum_extendedstatus', ''),
            ('non-key', 'ua_apartmentsNum_extendedstatus', '')]
step_3_1_fields = [
            ('non-key', 'citizen', ''),
            ('non-key', 'ua_city', ''),
            ('non-key', 'ua_street', ''),
            ('non-key', 'ua_lastname', ''),
            ('non-key', 'ua_postCode', ''),
            ('non-key', 'eng_lastname', ''),
            ('non-key', 'eng_postCode', ''),
            ('non-key', 'rightBelongs', ''),
            ('non-key', 'ua_firstname', ''),
            ('non-key', 'ukr_lastname', ''),
            ('non-key', 'eng_firstname', ''),
            ('non-key', 'ownershipType', ''),
            ('non-key', 'ua_middlename', ''),
            ('non-key', 'ua_streetType', ''),
            ('non-key', 'ukr_firstname', ''),
            ('non-key', 'eng_middlename', ''),
            ('non-key', 'otherOwnership', ''),
            ('non-key', 'ukr_middlename', ''),
            ('non-key', 'rights_cityPath', ''),
            ('non-key', 'ua_company_name', ''),
            ('non-key', 'eng_company_name', ''),
            ('non-key', 'ukr_company_name', ''),
            ('non-key', 'percent-ownership', '100'),
            ('non-key', 'ua_street_extendedstatus', ''),
            ('non-key', 'ua_houseNum_extendedstatus', ''),
            ('non-key', 'ua_postCode_extendedstatus', ''),
            ('non-key', 'eng_postCode_extendedstatus', ''),
            ('non-key', 'ua_middlename_extendedstatus', ''),
            ('non-key', 'eng_middlename_extendedstatus', ''),
            ('non-key', 'ukr_middlename_extendedstatus', ''),
            ('non-key', 'ua_housePartNum_extendedstatus', ''),
            ('non-key', 'ua_apartmentsNum_extendedstatus', '')]
step_6_fields = [('brand', 'brand', ''),
                 ('brand_info', 'brand_info_declcomua', ''),
                 ('non-key', 'person', ''),
                 ('non-key', 'model', ''),
                 ('non-key', 'rights', ''),
                 ('sum', 'costDate', ''),
                 ('sum_rent', 'costRent_declcomua', ''),
                 ('non-key', 'iteration', ''),
                 ('non-key', 'objectType', ''),
                 ('non-key', 'owningDate', ''),
                 ('year', 'graduationYear', ''),
                 ('non-key', 'otherObjectType', '')]
step_6_1_fields = step_3_1_fields
step_7_fields = [
            ('sum', 'cost', ''),
            ('non-key', 'amount', ''),
            ('non-key', 'costCurrentYear', ''),
            ('non-key', 'person', ''),
            ('non-key', 'rights', ''),
            ('non-key', 'emitent', ''),
            ('non-key', 'owningDate', ''),
            ('non-key', 'emitent_type', ''),
            ('non-key', 'typeProperty', ''),
            ('non-key', 'otherObjectType', ''),
            ('non-key', 'subTypeProperty1', ''),
            ('non-key', 'subTypeProperty2', ''),
            ('non-key', 'emitent_ua_lastname', ''),
            ('sum_foreign_comment', 'emitent_eng_fullname', ''),

            ('non-key', 'sizeAssets_currentYear_declcomua', ''),
            ('sum_foreign', 'sizeAssets_abroad_declcomua', ''),
            ('non-key', 'sizeAssets_abroad_currentYear_declcomua', ''),

            ('non-key', 'emitent_ua_firstname', ''),
            ('non-key', 'emitent_ukr_fullname', ''),
            ('non-key', 'emitent_ua_middlename', ''),
            ('non-key', 'emitent_ua_company_name', ''),
            ('non-key', 'emitent_eng_company_name', ''),
            ('non-key', 'emitent_ukr_company_name', ''),
            ('non-key', 'emitent_ua_sameRegLivingAddress', ''),
            ('non-key', 'emitent_eng_sameRegLivingAddress', '')]
step_7_1_fields = step_3_1_fields
step_8_fields = [('sum', 'cost', ''),
            ('non-key', 'name', ''),
            ('non-key', 'person', ''),
            ('non-key', 'rights', ''),
            ('non-key', 'country', ''),
            ('non-key', 'en_name', ''),
            ('non-key', 'typeProperty', ''),

            ('non-key', 'sizeAssets_currentYear_declcomua', ''),
            ('sum_foreign', 'sizeAssets_abroad_declcomua', ''),
            ('non-key', 'sizeAssets_abroad_currentYear_declcomua', ''),

            ('non-key', 'iteration', ''),
            ('non-key', 'legalForm', ''),
            ('non-key', 'cost_percent', '')]
step_8_1_fields = step_3_1_fields
step_11_fields = [('non-key', 'person', ''),
                  ('non-key', 'rights', ''),
                  ('non-key', 'iteration', ''),
                  ('non-key', 'objectType', ''),
                  ('non-key', 'sizeIncome', ''),
                  ('non-key', 'inner_or_outer_declcomua', ''),
                  ('non-key', 'source_citizen', ''),
                  ('non-key', 'otherObjectType', ''),
                  ('non-key', 'source_ua_lastname', ''),
                  ('non-key', 'source_eng_fullname', ''),
                  ('non-key', 'source_ua_firstname', ''),
                  ('non-key', 'source_ukr_fullname', ''),
                  ('non-key', 'source_ua_middlename', ''),
                  ('source_name', 'source_ua_company_name', ''),
                  ('non-key', 'source_eng_company_name', ''),
                  ('non-key', 'source_ukr_company_name', ''),
                  ('non-key', 'source_ua_sameRegLivingAddress', '')]
step_11_1_fields = step_3_1_fields
step_12_fields = [('non-key', 'person', ''),
                  ('non-key', 'rights', ''),
                  ('non-key', 'objectType', ''),
                  ('sum', 'sizeAssets', ''),
                  ('non-key', 'organization', ''),
                  ('non-key', 'costCurrentYear', ''),
                  ('non-key', 'assetsCurrency', 'UAH'),

                  ('non-key', 'sizeAssets_currentYear_declcomua', ''),
                  ('sum_foreign', 'sizeAssets_abroad_declcomua', ''),
                  ('non-key', 'sizeAssets_abroad_currentYear_declcomua', ''),

                  ('non-key', 'otherObjectType', ''),
                  ('non-key', 'organization_type', ''),
                  ('non-key', 'debtor_ua_lastname', ''),
                  ('non-key', 'debtor_eng_lastname', ''),
                  ('non-key', 'debtor_ua_firstname', ''),
                  ('non-key', 'debtor_ukr_lastname', ''),
                  ('non-key', 'debtor_eng_firstname', ''),
                  ('non-key', 'debtor_ua_middlename', ''),
                  ('non-key', 'debtor_ukr_firstname', ''),
                  ('non-key', 'debtor_eng_middlename', ''),
                  ('non-key', 'debtor_ukr_middlename', ''),
                  ('non-key', 'organization_ua_company_name', ''),
                  ('non-key', 'organization_eng_company_name', ''),
                  ('non-key', 'organization_ukr_company_name', ''),
                  ('non-key', 'debtor_ua_sameRegLivingAddress', ''),
                  ('non-key', 'debtor_eng_sameRegLivingAddress', '')]
step_12_1_fields = step_3_1_fields
step_13_fields = [('non-key', 'person', ''),
                  ('non-key', 'currency', ''),
                  ('non-key', 'guarantor', ''),
                  ('non-key', 'iteration', ''),
                  ('non-key', 'dateOrigin', ''),
                  ('non-key', 'objectType', ''),
                  ('non-key', 'margin-emitent', ''),
                  ('sum', 'sizeObligation', ''),
                  ('non-key', 'emitent_citizen', ''),
                  ('non-key', 'otherObjectType', ''),
                  ('non-key', 'guarantor_exist_', ''),
                  ('sum_foreign', 'sizeAssets_abroad_declcomua', ''),
                  ('non-key', 'guarantor_realty', ''),
                  ('non-key', 'ownerThirdPerson', ''),
                  ('non-key', 'emitent_ua_lastname', ''),
                  ('non-key', 'emitent_eng_fullname', ''),
                  ('non-key', 'emitent_ua_firstname', ''),
                  ('non-key', 'emitent_ukr_fullname', ''),
                  ('non-key', 'emitent_ua_middlename', ''),
                  ('non-key', 'ownerThirdPersonThing', ''),
                  ('non-key', 'emitent_ua_company_name', ''),
                  ('non-key', 'guarantor_realty_exist_', ''),
                  ('non-key', 'emitent_eng_company_name', ''),
                  ('sum_comment', 'emitent_ukr_company_name', ''),
                  ('sum_foreign_comment', 'emitent_ua_sameRegLivingAddress', ''),
                  ('non-key', 'emitent_eng_company_code_extendedstatus', '')]

"""
Iterating through json files that were found within following template:
"/declarations/page_number/count_nubmer.json"

First argument should be a folder name.

"""
for file_name in glob.iglob(sys.argv[1] + '/*/' + "*.json"):
    with open(file_name, 'r') as infile:
        data = infile.read()
        try:
          old_json = json.loads(data)
        except json.decoder.JSONDecodeError:
          print('Empty file: {}'.format(file_name))
          continue
    new_json = {}
    try:
        if (old_json['id'].lower().startswith('nacp')):
            print(file_name + 'is "Nacp" declartion. Nothing to do here.')
            continue
        ### ID ###
        new_json.__setitem__('id', old_json.get('id', ''))
        ### ID END ###
        ### CREATED DATE ##
        new_json.__setitem__('created_date', '')
        ### CREATED DATE END ###

        ### LAST MODIFIED DATE ###
        new_json.__setitem__('lastmodified_date',
                    old_json['declaration'].get('date', ''))
        ### LAST MODIFIED DATE END ###

        ### DATA ###
        new_json.__setitem__('data', {})
        ### DATA END ###    

        ### SERVER INFO ###
        new_json.__setitem__('server_info', {})
        ### SERVER INFO END ### 

        ### Data - step_0 ###
        new_json['data'].__setitem__('step_0', {})
        for column in step_0_fields:
            new_json['data']['step_0'].__setitem__(column[1],
                old_json['intro'].get(column[0], column[2]))
        ### Data - step_0 END ###

        ### Data - step_1 ###
        new_json['data'].__setitem__('step_1', {})
        general_info = old_json['general']

        if 'full_name' in general_info and 'post' in general_info:
            for column in step_1_fields:
                # column[0] - key for field in old_json
                # column[1] - new key field for new_json
                # column[2] - default value for field

                new_json['data']['step_1'].__setitem__(column[1],
                                            general_info.get(column[0], column[2]))

            for column in step_1_1_fields:
                new_json['data']['step_1'].__setitem__(column[1],
                                            general_info['post'].get(column[0], column[2]))
            
            new_json['data']['step_1'].__setitem__('place_of_living_declcomua', [])
            if 'addresses' in general_info:
                for i, address in enumerate(general_info['addresses']):
                    new_json['data']['step_1']['place_of_living_declcomua'].append('')
                    full_adress = ''
                    place_code = address.get('place', '')

                    if place_code:
                        full_adress = place_code + ', '
                    for field in ('place_district', 'place_city', 'place_address'):
                        part_adress = address.get(field, '')
                        if part_adress:
                            full_adress += part_adress + ', '
    
                    if full_adress:
                        new_json['data']['step_1']['place_of_living_declcomua'][i] = full_adress


                ### Data - step_1 END ###

        ### Data - step_2 ###
        VALID_POSITIONS = ["Син",
            "Дружина",
            "Чоловік",
            "Донька",
            "Дочка",
            "Мати",
            "Батько",
            "Жінка",
            "Брат",
            "Дружина брата",
            "Сестра",
            "Теща",
            "Онук",
            "Мама",
            "Невістка",
            "Племінник",
            "Баба",
            "Пасинок",
            "Дитина",
            "Матір",
            "Онука",
            "Зять",
            "Діти",
            "Свекор",
            "Бабуся",
            "Племінниця",
            "Донечка",
            "Тесть",
            "Внучка",
            "Сын",
            "Чоловик",
            "Співмешканець",
            "Супруга",
            "Допька",
            "Дружіна",
            "Падчерка",
            "Внук",
            "Свекруха",
            "Мать",
            "Доч",
            "Батьки",
            "Тітка",
            "Співмешканака",
            "Онучка",
            "Тато",
            "Жена"]

        def parse_family_member(s):
            try:
                position, person = s.split(None, 1)
                if "-" in position:
                    position, person = s.split("-", 1)

                position = position.strip(u" -—,.:").capitalize()
                person = person.strip(u" -—,")

                if position not in VALID_POSITIONS:
                    raise ValueError

                for pos in VALID_POSITIONS:
                    if person.capitalize().startswith(pos):
                        print("%s %s %s" % (s, person, pos))
                        raise ValueError

                return {
                    "relations": position,
                    "family_name": person
                }
            except ValueError:
                return {"raw": s}

        def parse_raw_family_string(family_raw):
            return map(parse_family_member, filter(None, family_raw.split(";")))

        new_json['data'].__setitem__('step_2', {})
        if 'family' in old_json['general']:
            family_info = old_json['general']['family']

            for fm_number, family_member in enumerate(family_info):
                new_json['data']['step_2'].__setitem__(fm_number, {})
                for column in step_2_fields:
                    new_json['data']['step_2'][fm_number].__setitem__(column[1],
                                                family_member.get(column[0], column[2]))
                    try:
                        lastname, firstname, middlename = \
                            family_member['family_name'].split(' ')
                        new_json['data']['step_2'][fm_number]['lastname'] = lastname
                        new_json['data']['step_2'][fm_number]['firstname'] = firstname
                        new_json['data']['step_2'][fm_number]['middlename'] = middlename
                    except ValueError:
                        new_json['data']['step_2'][fm_number]['bio_declcomua'] = \
                                family_member['family_name']
                if 'relations' in family_member:
                  if family_member['relations'] and \
                     str(family_member['relations']).lower() == 'інше':
                      new_json['data']['step_2'][fm_number]['subjectRelation'] += \
                                                      ', ' + family_member['relations_other']
                new_json['data']['step_2'][fm_number]['subjectRelation'] += ' (стара форма)'
        else:
            raw_family_info = old_json['general']['family_raw']

            for fm_number, member in enumerate(parse_raw_family_string(raw_family_info)):
                new_json['data']['step_2'].__setitem__(fm_number, {})
                new_json['data']['step_2'][fm_number].__setitem__('raw',
                                                member.get('raw', ''))
                new_json['data']['step_2'][fm_number].__setitem__('relations',
                                                member.get('relations', ''))
                new_json['data']['step_2'][fm_number].__setitem__('family_name',
                                                member.get('family_name', ''))

        ### Data - step_2 END ###

        ### Data - step_3 ###
        new_json['data'].__setitem__('step_3', {})
        record_counter = 1
        areas_koef = {
          "га": 10000,
          "cоток": 100
        }
        if 'estate' in old_json:
            # Additional dict that will help to iterate through 'estate' fields
            rightBelongs = {'1': {'23': 'Земельна ділянка',
                                    '24': 'Житловий будинок',
                                    '25': 'Квартира',
                                    '26': 'Садовий (дачний) будинок',
                                    '27': 'Гараж',
                                    '28': 'Інше нерухоме майно'},
                      'family': {'29': 'Земельна ділянка',
                                 '30': 'Житловий будинок',
                                 '31': 'Квартира',
                                 '32': 'Садовий (дачний) будинок',
                                 '33': 'Гараж',
                                 '34': 'Інше нерухоме майно'}}  
            estate_dict = old_json['estate']

            for owner_id, group_indxs in rightBelongs.items():
                for indx in group_indxs.keys():
                    if indx in estate_dict:
                        for estate in estate_dict[indx]:
                            if 'space' in estate and estate['space']:
                                new_json['data']['step_3'].__setitem__(record_counter, {})
                                for column in step_3_fields:
                                    new_json['data']['step_3'][record_counter].__setitem__(column[1],
                                                    estate.get(column[0], column[2]))
                                new_json['data']['step_3'][record_counter]['rights'] = {}
                                new_json['data']['step_3'][record_counter]['rights'].__setitem__(owner_id, {})
                                new_json['data']['step_3'][record_counter]['objectType'] = rightBelongs[owner_id][indx] + ' (стара форма)'
                                totalArea = new_json['data']['step_3'][record_counter]['totalArea']
                                space_unit_koef = 1
                                if 'space_units' in estate and estate['space_units']:
                                  space_unit_koef = areas_koef.get(estate['space_units'], 1)

                                try:
                                  new_json['data']['step_3'][record_counter]['totalArea'] = \
                                    float(totalArea) * space_unit_koef
                                except ValueError:
                                  pass

                                for column in step_3_1_fields:
                                    new_json['data']['step_3'][record_counter]['rights'][owner_id].__setitem__(column[1],
                                                    estate.get(column[0], column[2]))
                                new_json['data']['step_3'][record_counter]['rights'][owner_id]['rightBelongs'] = \
                                                                                        owner_id
                                record_counter += 1
        ### Data - step_3 END ###

        ### Data - step_4 ###
        new_json['data'].__setitem__('step_4', {})
        ### Data - step_4 END ###

        ### Data - step_5 ###
        new_json['data'].__setitem__('step_5', {})
        ### Data - step_5 END ###

        ### Data - step_6 ###
        new_json['data'].__setitem__('step_6', {})
        if 'vehicle' in old_json:
            rightBelongs = {'1': {'35': 'Автомобілі легкові',
                                    '36': 'Автомобілі вантажні (спеціальні)',
                                    '37': 'Водні засоби',
                                    '38': 'Повітряні судна',
                                    '39': 'Інші засоби'},
                      'family': {'40': 'Автомобілі легкові',
                                '41': 'Автомобілі вантажні (спеціальні)',
                                '42': 'Водні засоби',
                                '43': 'Повітряні судна',
                                '44': 'Інші засоби'}}
            vehicle_dict = old_json['vehicle']
            record_counter = 1

            for owner_id, group_indxs in rightBelongs.items():
                for indx in group_indxs:
                    if indx in vehicle_dict:
                        for vehicle in vehicle_dict[indx]:
                            new_json['data']['step_6'].__setitem__(record_counter, {})
                            for column in step_6_fields:
                                new_json['data']['step_6'][record_counter].__setitem__(column[1],
                                                vehicle.get(column[0], column[2]))
                            new_json['data']['step_6'][record_counter]['rights'] = {}
                            new_json['data']['step_6'][record_counter]['rights'].__setitem__(owner_id, {})
                            new_json['data']['step_6'][record_counter]['objectType'] = rightBelongs[owner_id][indx] + ' (стара форма)'

                            for column in step_6_1_fields:
                                new_json['data']['step_6'][record_counter]['rights'][owner_id].__setitem__(column[1],
                                                vehicle.get(column[0], column[2]))
                            new_json['data']['step_6'][record_counter]['rights'][owner_id]['rightBelongs'] = \
                                                                                    owner_id
                            record_counter += 1                                
        ### Data - step_6 END ###

        ### Data - step_7 ###
        new_json['data'].__setitem__('step_7', {})
        if 'banks' in old_json:
            rightBelongs = {'1': {'47': 'Номінальна вартість цінних паперів'},
                      'family': {'52': 'Номінальна вартість цінних паперів'}}
            papers_dict = old_json['banks']
            record_counter = 1
            for owner_id, group_indxs in rightBelongs.items():
                for indx in group_indxs.keys():
                    if indx in papers_dict:
                        for paper in papers_dict[indx]:
                            if paper['sum'] or paper['sum_foreign']:

                                new_json['data']['step_7'].__setitem__(record_counter, {})

                                for column in step_7_fields:
                                    new_json['data']['step_7'][record_counter].__setitem__(column[1],
                                                    paper.get(column[0], column[2]))

                                if indx == '47':
                                    if '48' in papers_dict and papers_dict['48']:
                                        new_json['data']['step_7'][record_counter]['sizeAssets_currentYear_declcomua'] = \
                                                                            papers_dict['48'][0]['sum']
                                        new_json['data']['step_7'][record_counter]['sizeAssets_abroad_currentYear_declcomua'] = \
                                                                            papers_dict['48'][0]['sum_foreign']

                                new_json['data']['step_7'][record_counter]['rights'] = {}
                                new_json['data']['step_7'][record_counter]['rights'].__setitem__(owner_id, {})
                                new_json['data']['step_7'][record_counter]['typeProperty'] = rightBelongs[owner_id][indx] + ' (стара форма)'
                                new_json['data']['step_7'][record_counter]['person'] = owner_id

                                for column in step_7_1_fields:
                                    new_json['data']['step_7'][record_counter]['rights'][owner_id].__setitem__(column[1],
                                                    paper.get(column[0], column[2]))
                                new_json['data']['step_7'][record_counter]['rights'][owner_id]['rightBelongs'] = \
                                                                                        owner_id
                                record_counter += 1
        ### Data - step_7 END ###

        ### Data - step_8 ###
        new_json['data'].__setitem__('step_8', {})
        if 'banks' in old_json:
            rightBelongs = {'1': {'49': 'Розмір внесків до статутного капіталу товариства, підприємства, організації'},
                      'family': {'53': 'Розмір внесків до статутного капіталу товариства, підприємства, організації'}}
            papers_dict = old_json['banks']
            record_counter = 1
            for owner_id, group_indxs in rightBelongs.items():
                for indx in group_indxs.keys():
                    if indx in papers_dict:
                        for paper in papers_dict[indx]:
                            if paper['sum'] or paper['sum_foreign']:
                                new_json['data']['step_8'].__setitem__(record_counter, {})

                                for column in step_8_fields:
                                    new_json['data']['step_8'][record_counter].__setitem__(column[1],
                                                    paper.get(column[0], column[2]))
                                
                                if indx == '49':
                                    if '50' in papers_dict and papers_dict['50']:
                                        new_json['data']['step_8'][record_counter]['sizeAssets_currentYear_declcomua'] = \
                                                                            papers_dict['50'][0]['sum']
                                        new_json['data']['step_8'][record_counter]['sizeAssets_abroad_currentYear_declcomua'] = \
                                                                            papers_dict['50'][0]['sum_foreign']


                                new_json['data']['step_8'][record_counter]['rights'] = {}
                                new_json['data']['step_8'][record_counter]['rights'].__setitem__(owner_id, {})
                                new_json['data']['step_8'][record_counter]['typeProperty'] = rightBelongs[owner_id][indx] + ' (стара форма)'
                                new_json['data']['step_8'][record_counter]['person'] = owner_id
                                
                                for column in step_8_1_fields:
                                    new_json['data']['step_8'][record_counter]['rights'][owner_id].__setitem__(column[1],
                                                    paper.get(column[0], column[2]))
                                new_json['data']['step_8'][record_counter]['rights'][owner_id]['rightBelongs'] = \
                                                                                        owner_id
                                
                                record_counter += 1
        ### Data - step_8 END ###

        ### Data - step_9 ###
        new_json['data'].__setitem__('step_9', {})
        ### Data - step_9 END ###

        ### Data - step_10 ###
        new_json['data'].__setitem__('step_10', {})
        ### Data - step_10 END ###


        ### Data - step_11 ###
        new_json['data'].__setitem__('step_11', {})
        if 'income' in old_json:
            income_types = {'5': 'Загальна сума сукупного доходу',
                       '6': 'Заробітна плата, інші виплати та винагороди, нараховані (виплачені) декларанту відповідно до умов трудового або цивільно-правового договору',
                       '7': 'Дохід від викладацької, наукової і творчої діяльності, медичної практики, інструкторіської та суддівської практики із спорту',
                       '8': 'Авторська винагорода, інші доходи від реалізації майнових прав інтелектуальної власності',
                       '9': 'Дивіденди, проценти',
                       '10': 'Матеріальна допомога',
                       '11': 'Дарунки, призи, виграші',
                       '12': 'Допомога по безробіттю',
                       '13': 'Аліменти',
                       '14': 'Cпадщина',
                       '15': 'Cтрахові виплати, страхові відшкодування',
                       '16': 'Дохід від відчуження рухомого та нерухомого майна',
                       '17': 'Дохід від провадження підприємницької та незалежної професійної діяльності',
                       '18': 'Дохід від відчуження цінних паперів та корпоративних прав',
                       '19': 'Дохід від передачі в оренду майна',
                       '20': 'Iнші види доходів'}
            owner_types = ('1', 'family')
            source_types = {22: 'Одержані з джерел за межами України членами сім’ї декларанта',
                            21: 'Одержані  з джерел за межами України декларантом'}
            record_counter = 1
            incomes_dict = old_json['income']

            for income_key, income_dict in incomes_dict.items():
                if int(income_key) in (21, 22):
                    if not income_dict:
                        continue

                    for income_item in income_dict:
                        new_json['data']['step_11'].__setitem__(record_counter, {})
                        for column in step_11_fields:
                            new_json['data']['step_11'][record_counter].__setitem__(column[1],
                                                        income_item.get(column[0], column[2]))
                        new_json['data']['step_11'][record_counter]['rights'] = {}
                        right_owner = (owner_types[0]
                                       if income_key == '21'
                                       else owner_types[1])
                        new_json['data']['step_11'][record_counter]['rights'].__setitem__(right_owner, {})
                        new_json['data']['step_11'][record_counter]['person'] = right_owner

                        new_json['data']['step_11'][record_counter]['objectType'] = \
                                                    source_types[int(income_key)] + ' (стара форма)'
                        new_json['data']['step_11'][record_counter]['inner_or_outer_declcomua'] = 'outer'

                        new_json['data']['step_11'][record_counter]['source_citizen'] = \
                                                    'Юридична особа, зареєстрована за кордоном'


                        new_json['data']['step_11'][record_counter]['income_country_name_declcomua'] = \
                                                            income_item.get('country', '')
                        new_json['data']['step_11'][record_counter]['sizeIncome'] = income_item.get('uah_equal', '')
                        for column in step_11_1_fields:
                            new_json['data']['step_11'][record_counter]['rights'][right_owner].__setitem__(column[1],
                                                income_item.get(column[0], column[2]))
                        new_json['data']['step_11'][record_counter]['rights'][right_owner]['rightBelongs'] = \
                                    right_owner
                        record_counter += 1
                else:
                  if 'family' in incomes_dict and 'value' in incomes_dict:
                    if income_dict['family'] or income_dict['value']:
                        for sum_type in ('value', 'family'):
                            if not income_dict[sum_type]:
                                continue

                            new_json['data']['step_11'].__setitem__(record_counter, {})
                            for column in step_11_fields:
                                new_json['data']['step_11'][record_counter].__setitem__(column[1],
                                                        income_dict.get(column[0], column[2]))
                            new_json['data']['step_11'][record_counter]['rights'] = {}

                            right_owner = (owner_types[0]
                                       if sum_type == 'value'
                                       else owner_types[1])

                            new_json['data']['step_11'][record_counter]['rights'].__setitem__(right_owner, {})
                            new_json['data']['step_11'][record_counter]['person'] = right_owner
                            new_json['data']['step_11'][record_counter]['objectType'] = \
                                                        income_types[str(income_key)] + ' (стара форма)'
                            new_json['data']['step_11'][record_counter]['sizeIncome'] = \
                                                                income_dict[sum_type]
                            new_json['data']['step_11'][record_counter]['source_citizen'] = \
                                                                'Юридична особа, зареєстрована в Україні'
                            new_json['data']['step_11'][record_counter]['inner_or_outer_declcomua'] = 'inner'
                            for column in step_11_1_fields:
                                new_json['data']['step_11'][record_counter]['rights'][right_owner].__setitem__(column[1],
                                                income_dict.get(column[0], column[2]))
                            new_json['data']['step_11'][record_counter]['rights'][right_owner]['rightBelongs'] = \
                                    right_owner
                            record_counter += 1                
        ### Data - step_11 END ###

        ### Data - step_12 ###
        new_json['data'].__setitem__('step_12', {})
        if 'banks' in old_json:
            rightBelongs = {'1': {'45': 'Сума коштів на рахунках у банках та інших фінансових установах'},
                      'family': {'51': 'Сума коштів на рахунках у банках та інших фінансових установах'}}
            papers_dict = old_json['banks']
            record_counter = 1
            for owner_id, group_indxs in rightBelongs.items():
                for indx in group_indxs.keys():
                    if indx in papers_dict:
                        for paper in papers_dict[indx]:
                            if paper['sum'] or paper['sum_foreign']:
                                new_json['data']['step_12'].__setitem__(record_counter, {})
                                for column in step_12_fields:
                                    new_json['data']['step_12'][record_counter].__setitem__(column[1],
                                                    paper.get(column[0], column[2]))                                

                                if indx == '45':
                                    if '46' in papers_dict and papers_dict['46']:
                                        new_json['data']['step_12'][record_counter]['sizeAssets_currentYear_declcomua'] = \
                                                                            papers_dict['46'][0]['sum']
                                        new_json['data']['step_12'][record_counter]['sizeAssets_abroad_currentYear_declcomua'] = \
                                                                            papers_dict['46'][0]['sum_foreign']

                                new_json['data']['step_12'][record_counter]['rights'] = {}
                                new_json['data']['step_12'][record_counter]['rights'].__setitem__(owner_id, {})
                                new_json['data']['step_12'][record_counter]['objectType'] = \
                                                                rightBelongs[owner_id][indx] + ' (стара форма)'
                                new_json['data']['step_12'][record_counter]['person'] = owner_id


                                for column in step_12_1_fields:
                                    new_json['data']['step_12'][record_counter]['rights'][owner_id].__setitem__(column[1],
                                                    paper.get(column[0], column[2]))
                                new_json['data']['step_12'][record_counter]['rights'][owner_id]['rightBelongs'] = \
                                                                                        owner_id
                                record_counter += 1
        ### Data - step_12 END ###

        ### Data - step_13 ###
        new_json['data'].__setitem__('step_13', {})
        if 'liabilities' in old_json:
            rightBelongs = {'1': {'54': 'Добровільне страхування',
                                    '55': 'Недержавне пенсійне забезпечення',
                                    '56': 'Утримання зазначеного у розділах ІІІ–V майна',
                                    '57': 'Погашення основної суми позики (кредиту)',
                                    '58': 'Погашення суми процентів за позикою (кредитом)',
                                    '59': 'Інші не зазначені у розділах ІІІ–V витрати'},
                      'family': {'60': 'Добровільне страхування',
                                '61': 'Недержавне пенсійне забезпечення',
                                '62': 'Утримання зазначеного у розділах ІІІ–V майна',
                                '63': 'Погашення основної суми позики (кредиту)',
                                '64': 'Погашення суми процентів за позикою (кредитом)'}}
            liabilities_dict = old_json['liabilities']
            record_counter = 1
            for owner_id, group_indxs in rightBelongs.items():
                for loan_indx in group_indxs:
                    loan_dict = liabilities_dict[loan_indx]

                    if loan_dict['sum'] or loan_dict['sum_foreign']:    
                        new_json['data']['step_13'].__setitem__(record_counter, {})
                        
                        for column in step_13_fields:
                            new_json['data']['step_13'][record_counter].__setitem__(column[1],
                                                        loan_dict.get(column[0], column[2]))
                        
                        new_json['data']['step_13'][record_counter]['person'] = owner_id

                        new_json['data']['step_13'][record_counter]['objectType'] = \
                                                rightBelongs[owner_id][loan_indx] + ' (стара форма)'
                        sizeObligation = new_json['data']['step_13'][record_counter]['sizeObligation']
                        try:
                          new_json['data']['step_13'][record_counter]['sizeObligation'] = \
                            float(sizeObligation)
                        except ValueError:
                          pass
                            
                        record_counter += 1
        ### Data - step_13 END ###

        ### Data - step_14 ###
        new_json['data'].__setitem__('step_14', {})
        ### Data - step_14 END ###

        ### Data - step_15 ###
        new_json['data'].__setitem__('step_15', {})
        ### Data - step_15 END ###

        ### Data - step_16 ###
        new_json['data'].__setitem__('step_16', {})
        ### Data - step_16 END ###

    except KeyError as e:
        print('No such key in json data: ', file_name, e)
        traceback.print_exc()
        continue
    else:
        print('Rewriting file: {}'.format(file_name))
        with open(file_name, 'w') as outfile:
          data = json.dumps(new_json,
                            ensure_ascii=False,
                            indent=4)
          outfile.write(data)