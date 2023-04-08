import os

import pandas as pd

import auxilliary_functions as fx
from glob import glob
from main.backend.src.python_v6.auxilliary_functions import *


class AggregateAllDataInit():
    def __init__(self, prediction_year):
        # TODO: import paths
        self.client_data_path = ""
        self.during_analysis_data_path = ""
        self.raw_RFs_path = ""
        self.prediction_year = prediction_year
        self.clinical_data_path = os.path.join(self.client_data_path, "clinical")
        self.claims_based_data_path = os.path.join(self.during_analysis_data_path,
                                                   "aggregatedData/claimsBasedDataPath")
        self.lab_based_data_path = os.path.join(self.during_analysis_data_path,
                                                "aggregatedData/labBasedData")
        self.RF_data = my_read_csv(self.raw_RFs_path)
        self.aggregated_data_path = os.path.join(self.during_analysis_data_path, "aggregatedData")

    def _preprocess_data(self):
        demog_cols = {"MEMBER_SK": "MEMBER_SK", "Age": "Patient_Age", "Gender": "Gender",
                      "Ethnicity": "Ethnicity", "DOD": "MEM_DOD", "DOD IND": "DOD_IND"}
        lifestyle_cols = {"Smoking Status": "Smoker_Status", "Smoking Freq": "Smoker_Freq",
                          "Alcohol Status": "Alcohol_Status", "Alcohol Freq": "Alcohol_Freq"}
        disease_flag_cols = {"DIB Flag": "Diabetes", "CAD Flag": "Coronary_artery_disease",
                             "CRC Flag": "CANCER_COLORECTAL", "BSC Flag": "CANCER_BREAST",
                             "Dementia Flag": "Dementia", "Chol Drug": "CHOLESTEROL_DRUG",
                             "BP Drug": "BP_DRUG", "Hyperlipidemia Flag": "Hyperlipidemia",
                             "Hypertension Flag": "Hypertension"}
        columns_to_be_extracted = {**demog_cols, **lifestyle_cols, **disease_flag_cols}
        missing_fields = [i for i in columns_to_be_extracted.keys() if i not in self.RF_data.columns]
        if len(missing_fields) > 0:
            raise f"BASEHEALTH: The following typically available fields are " \
                  f"not available in the risk factor file: {missing_fields}"

        columns_to_be_extracted = {i: columns_to_be_extracted[i] for i in
                                   columns_to_be_extracted if i not in missing_fields}
        self.RF_data = self.RF_data[columns_to_be_extracted.keys()]
        self.RF_data.columns = self.RF_data[columns_to_be_extracted.keys()]

        data_to_model = self.RF_data
        return data_to_model

    def process_lab_data(self):
        data_to_model = AggregateAllDataInit._preprocess_data(self)
        lab_extreme_values_yearly = pd.read_csv(os.path.join(self.lab_based_data_path))
        data_to_model = pd.merge(data_to_model, lab_extreme_values_yearly)

        lab_latest_values_yearly = pd.read_csv(os.path.join(self.lab_based_data_path,
                                                            "labLatestValuesYearly.csv"))
        data_to_model = pd.merge(data_to_model, lab_latest_values_yearly)
        self.data_to_model = data_to_model
        return self

    def aggregate_claims(self, include_RAF=True):
        claims_user_year = pd.read_csv(os.path.join(self.claims_based_data_path,"claimsUserYear.csv"))
        self.data_to_model = my_join(self.data_to_model, claims_user_year, NA_columns=0)
        self.claims_data_years = [re.sub(pattern="Claims ", repl="", string=i) for i in claims_user_year.columns]
        claims_user_quarter = pd.read_csv(os.path.join(self.claims_based_data_path,"claimsUserQuarter.csv"))
        self.data_to_model = my_join(self.data_to_model, claims_user_quarter, NA_columns=0)

        claims_category_year = pd.read_csv(os.path.join(self.claims_based_data_path,"claimsCategoryYear.csv"))
        self.data_to_model = my_join(self.data_to_model, claims_category_year, NA_columns=0)

        claims_category_quarter = pd.read_csv(os.path.join(self.claims_based_data_path,"claimsCategoryQuarter.csv"))
        self.data_to_model = my_join(self.data_to_model, claims_category_quarter, NA_columns=0)

        claims_term_year = pd.read_csv(os.path.join(self.claims_based_data_path,"claimsTermYear.csv"))
        self.data_to_model = my_join(self.data_to_model, claims_term_year, NA_columns=0)
        claims_term_quarter = pd.read_csv(os.path.join(self.claims_based_data_path,"claimsTermYearQuarter.csv"))
        self.data_to_model = my_join(self.data_to_model, claims_term_quarter, NA_columns=0)

        claims_model_condition = pd.read_csv(os.path.join(self.claims_based_data_path, "claimsModelConditions.csv"))
        self.data_to_model = my_join(self.data_to_model, claims_model_condition, NA_columns=0)

        print("Condition statuses...\n\n")
        condition_status_years = glob(os.path.join(self.client_data_path, "yearWiseRFdata/extreme/rawRFs/") + '201.$')
        disease_external_data = pd.read_csv(os.path.join(preDeterminedDataPath, "diseaseStatistics/diseasesData.csv"))

        for condition_status_year in condition_status_years:
            data_file_path = os.path.join(self.client_data_path,
                                          "yearWiseRFdata/extreme/rawRFs",
                                          condition_status_year)
            condition_statuses = my_read_csv(data_file_path)
            condition_statuses = condition_statuses[["MEMBER_SK"] + disease_external_data['Disease Name']]
            col_matches = condition_statuses.columns.isin()
            condition_statuses.columns = disease_external_data['Disease Full Name']\
                [np.where(condition_statuses.columns.isin(disease_external_data['Disease Name']))]
            condition_statuses.columns = ["Status " + str(condition_status_year) + "_" +
                                          i for i in condition_statuses.columns]
            self.data_to_model = my_join(self.data_to_model, condition_statuses)

        disease_status_year = pd.read_csv(os.path.join(self.claims_based_data_path, "diseaseStatusYear.csv"))
        disease_status_year = disease_status_year[~disease_status_year.columns.isin(["MEMBER_SK"])].apply(lambda x: 1 if True else 0)
        if include_RAF:
            print("RAF aggregate...\n\n")
            file_dir = os.path.join(self.client_data_path, "RAF")
            file_names = glob(file_dir + "*.csv")
            for file_name in file_names:
                filename = os.path.basename(file_name)
                year = re.search(r'\d+', filename).group(0)
                if year is None:
                    raise "BASEHEALTH: The format of RAF data files should be \"totalSumCommunity_[YEAR].csv\""
                RAF_data = pd.read_csv(file_name)
                RAF_data['RAF_nonDemog'] = RAF_data["totalSum"] - RAF_data["ageSexSum"]
                RAF_data.columns = ["MEMBER_SK", "RAF_"+ str(year), "RAF_demog_"+str(year), "RAF_nonDemog_"+str(year)]
                self.data_to_model = pd.merge(self.data_to_model, RAF_data)

    def adjust_eligibility(self, include_RAF_analysis=True):
        if len(glob(self.clinical_data_path + "/*eligibility*")) > 0:
            eligibility_data = my_read_csv(self.clinical_data_path, namePart="eligibility")
            eligibility_data['Year'] = pd.to_datetime(eligibility_data['YEARMONTH']).year
            eligibility_data['NumericMonth'] = pd.to_datetime(eligibility_data['YEARMONTH']).month
            eligibility_data['NumericQuarter'] = round(eligibility_data['NumericMonth'] / 4)
            eligibility_data['Quarter'] = eligibility_data['NumericQuarter'].apply(lambda x: "Q"+str(x))
            eligibility_data_year = eligibility_data.pivot_table(index=["MEMBER_SK"],
                                                           columns=["Year"],
                                                           aggfunc=sum, values=["ELIGIBLE"])
            eligibility_data_year.columns = ["Eligibility" + i for i in eligibility_data_year.columns]
            self.data_to_model = pd.merge(self.data_to_model, eligibility_data_year)
            eligibility_data_quarter = eligibility_data.pivot_table(index=["MEMBER_SK"],
                                                                 columns=["Year", "Quarter"],
                                                                 aggfunc=sum, values=["ELIGIBLE"])
            eligibility_data_quarter.columns = ["Eligibility" + i for i in eligibility_data_quarter.columns]
            self.data_to_model = pd.merge(self.data_to_model, eligibility_data_quarter)
            eligibility_data_month = eligibility_data.pivot_table(index=["MEMBER_SK"],
                                                                    columns=["Year", "NumericMonth"],
                                                                    aggfunc=sum, values=["ELIGIBLE"])
            eligibility_data_month.columns = ["Eligibility" + i for i in eligibility_data_month.columns]
            self.data_to_model = pd.merge(self.data_to_model, eligibility_data_month)

            for year in self.claims_data_years:
                print(f"Claim year: {year}")
                not_eligible_indices = self.data_to_model[(self.data_to_model[f"Eligibility {year}"].isnull()) | \
                                                         (self.data_to_model[f"Eligibility {year}"] <= 1)].index
                # TODO: Verify this regex
                yearly_column_headers = self.data_to_model.columns[
                                        self.data_to_model.columns.str.contain("^(Claims |Category *)" + str(year)) &
                                        ~self.data_to_model.columns.str.contain("_Q[1-4]")]
                self.data_to_model[yearly_column_headers] = self.data_to_model[yearly_column_headers] * \
                                                            (12 / self.data_to_model["Eligibility " +str(year)])
                self.data_to_model.loc[not_eligible_indices, yearly_column_headers] = None

                if include_RAF_analysis:
                    yearly_column_headers = self.data_to_model.columns[
                                        self.data_to_model.columns.str.contain("^(RAF |RAF_nonDemog *)" + str(year)) &
                                        ~self.data_to_model.columns.str.contain("_Q[1-4]")]
                    self.data_to_model.loc[not_eligible_indices, yearly_column_headers] = None

                # |--- |--- |--- Statuses ----
                yearly_column_headers = self.data_to_model.columns[
                    self.data_to_model.columns.str.contain("^Status0 " + str(year)) &
                    ~self.data_to_model.columns.str.contain("_Q[1-4]")]
                self.data_to_model.loc[not_eligible_indices, yearly_column_headers] = \
                    fx.set_zero_to_na(self.data_to_model.loc[not_eligible_indices, yearly_column_headers])

                for quarter in range(1,5):
                    not_eligible_indices = self.data_to_model[(self.data_to_model[f"Eligibility {year}_Q{quarter}"].isnull()) | \
                                                         (self.data_to_model[f"Eligibility {year}_Q{quarter}"] == 0)].index
                    not_enough_data_indices = self.data_to_model[(self.data_to_model[f"Eligibility {year}_Q{quarter}"]>0)
                                                        & (self.data_to_model[f"Eligibility {year}_Q{quarter}"]<2)].index
                    quarterly_column_headers = self.data_to_model.columns[self.data_to_model.columns.str.contain("^Claims " + str(year)) &
                    self.data_to_model.columns.str.contain("_Q[1-4]")]
                    self.data_to_model.loc[not_eligible_indices, quarterly_column_headers] = \
                        fx.set_zero_to_na(self.data_to_model.loc[not_eligible_indices, quarterly_column_headers])
                    self.data_to_model.loc[not_enough_data_indices, quarterly_column_headers] = \
                        fx.set_zero_to_na(self.data_to_model.loc[not_enough_data_indices, quarterly_column_headers])
        return self

    def aggregate_adjusted_ORs(self):
        file_dir = os.path.join(self.during_analysis_data_path, "augmentedORs/hierarchial/mergedHierarchialData/")
        diseases_to_study = re.sub("(InterventionData_|.csv)", "", glob(file_dir+".csv"))
        for disease_to_study in diseases_to_study:
            BH_data = pd.read_csv(disease_to_study)
            BH_data.index.rename("MEMBER_SK", inplace=True)
            BH_data.columns = [f"BH-H{self.prediction_year - 1}{disease_to_study}{i}" for i in BH_data.columns]
            self.data_to_model = pd.merge(self.data_to_model, BH_data)
        return self

    def aggregate_clinical_data(self):
        if len(glob(self.clinical_data_path + "*clinics*.csv") > 0):
            clinic_data = my_read_csv(os.path.join(self.clinical_data_path, namePart="clinic"))
            clinic_data = clinic_data[["MEMBER_SK", "POVIDER_NAME", "PROVIDER_OFFICE_NAME", "PROVIDER_NETWORK"]]
            self.data_to_model = pd.merge(self.data_to_model, clinic_data)
        return self

    def save_aggregate_data(self, write=True):
        if write:
            self.data_to_model.to_csv(os.path.join(self.aggregated_data_path, "aggregatedData.csv"))








