#! /usr/bin/python
# -*- coding: utf-8 -*-

from string import ascii_lowercase
from itertools import product
import requests
import sys
from pprint import pprint

"""Call APIs to get patient information from OpenMRS"""

__author__ = "Nafisa Bulsara"

server = "http://demo.openmrs.org/openmrs/"


def get_patients():
    patient_uuid = []

    keywords = [''.join(i) for i in product(ascii_lowercase, repeat=3)]
    for j in keywords:
        ext = "ws/rest/v1/patient?q=%s" % j
        r = requests.get(server + ext, headers={"Content-Type": "application/json"}, auth=('*****', '********')).json()
        if len(patient_uuid) == 1:
            break
        for item in r['results']:
            uuid = item.get('uuid')
            if len(uuid) != 0:
                patient_uuid.append(uuid)
    return patient_uuid


def print_encounter_and_observations(patient_uuid, outfile):
    encounter = "ws/rest/v1/encounter?patient=%s&v=full" % patient_uuid
    rx = requests.get(server + encounter, headers={"Content-Type": "application/json"},
                      auth=('*****', '********')).json()
    for item in rx['results']:
        encounter_uuid = item.get('uuid')
        encounter_datetime = item.get('encounterDatetime').replace('T', ' ')
        observations = item.get('obs')
        for one_observation in observations:
            observation_uuid = one_observation.get('uuid')
            observation_datetime = one_observation.get('obsDatetime').replace('T', ' ')
            observation_value = str(one_observation.get('value'))
            concept = one_observation.get('concept')
            group_members = one_observation.get('groupMembers')
            if group_members is not None:
                one_member = group_members[1]
                concept_uuid = ""
                observation_diagnosis = ""
                concept_obs = ""
                mapstr = ""
                one_value = one_member.get("value")
                concept_uuid = one_value.get("uuid")
                observation_diagnosis = one_value.get("display")
                concept_obs = "" + concept_uuid + '\t' + observation_diagnosis
                concept_extension= "ws/rest/v1/concept/%s" % concept_uuid
                con= requests.get(server+concept_extension, headers={"Content-Type":"application/json"}, auth=('*****', '********')).json()
                # pprint(con)
                mappings = con.get("mappings")
                for m in mappings:
                    mapstr += m.get("display") + '|'
                outfile.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(patient_uuid, encounter_uuid, encounter_datetime, observation_uuid, observation_datetime, concept_obs, observation_value, mapstr))
            else:
                mapstr = ""
                concept_uuid = "" + concept.get('uuid')
                concept_extension= "ws/rest/v1/concept/%s" % concept_uuid
                con= requests.get(server+concept_extension, headers={"Content-Type":"application/json"}, auth=('*****', '********')).json()
                mappings = con.get("mappings")
                for m in mappings:
                    mapstr += m.get("display")+'|'
                observation_diagnosis = 'null'
                concept_obs = "" + concept_uuid + '\t' + observation_diagnosis
                outfile.write("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n".format(patient_uuid, encounter_uuid, encounter_datetime, observation_uuid, observation_datetime, concept_obs, observation_value, mapstr))

if __name__ == "__main__":
    patient_dict = {}
    patients = get_patients()
    output_file = "information.txt"
    out = open(output_file, 'w')
    out.write("patient_uuid\tencounter_uuid\tencounter_datetime\tobservation_uuid\tobservation_datetime\tconcept_uuid\tobservation_diagnosis\tobservation_value\tmapping_id\n")
    for patient_uuid in patients:
        print_encounter_and_observations(patient_uuid, out)
    out.close()
