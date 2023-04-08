#! /usr/bin/python3

import glob
from lxml import etree

"""Parse TCGA Clinical files to extract information"""

__author__ = "Nafisa Bulsara"


for files in glob.glob('/path/to/files/*.xml'):
    clinical = open(files, 'r')
    tree = etree.parse(clinical)
    root = tree.getroot()
    root.find('./luad:tcga_bcr/luad:patient', root.nsmap)
    for rac in root.findall('luad:patient', root.nsmap):
        races = rac.find('clin_shared:race_list/clin_shared:race', root.nsmap).text
        race = ""
        if races == 'WHITE':
            race = races
            for elements in root.findall('luad:patient', root.nsmap):
                barcodes = elements.find('shared:bcr_patient_barcode', root.nsmap).text
                site = elements.find('clin_shared:tumor_tissue_site', root.nsmap).text
                histo_type = elements.find('shared:histological_type', root.nsmap).text
                other_dx = elements.find('shared:other_dx', root.nsmap).text
                genx = elements.find('shared:gender', root.nsmap).text
                try:
                    karnofsky = int(elements.find('clin_shared:karnofsky_performance_score', root.nsmap).text)
                except:
                    karnofsky = elements.find('clin_shared:karnofsky_performance_score', root.nsmap).text
                survive = elements.find('clin_shared:vital_status', root.nsmap).text
                kras_mut = elements.find('lung_shared:kras_mutation_found', root.nsmap).text
                smoker = elements.find('shared:tobacco_smoking_history', root.nsmap).text
                smoking_years = elements.find('clin_shared:number_pack_years_smoked', root.nsmap).text
                therapy_rad = elements.find('clin_shared:radiation_therapy', root.nsmap).text
                therapy_mol = elements.find('clin_shared:targeted_molecular_therapy', root.nsmap).text
                therapy_outcome = elements.find('clin_shared:primary_therapy_outcome_success', root.nsmap).text
                age_at_diagnosis=elements.find('clin_shared:age_at_initial_pathologic_diagnosis', root.nsmap).text
                drugs = elements.findall('.//rx:drug_name', root.nsmap)

                if len(drugs) >= 1:
                    drug_name = ""
                    for drug_names in drugs:
                        drug_name += str(drug_names.text) + " "

                    print(("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t".format(race, barcodes, site, histo_type, other_dx, genx, karnofsky, kras_mut, smoker, smoking_years, therapy_rad, therapy_mol, therapy_outcome,age_at_diagnosis ,drug_name)))

                else:
                    drug_name = 'null'
                    print(("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t".format(race, barcodes, site, histo_type, other_dx, genx, karnofsky, kras_mut, smoker, smoking_years, therapy_rad, therapy_mol, therapy_outcome,age_at_diagnosis ,drug_name)))
