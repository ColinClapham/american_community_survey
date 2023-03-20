import requests
import pandas
import os
from loguru import logger

def extract_acs_data(api_key):
    ### american_community_survey/reference/variable_definitions.csv
    ### https://www.census.gov/content/dam/Census/library/publications/2019/acs/acs_summary-file_handbook_2019_ch03.pdf
    variables = [
                'DP05_0001E',
                'DP05_0002E',
                'DP05_0002PE',
                'DP05_0003E',
                'DP05_0003PE',
                'DP05_0009E',
                'DP05_0009PE',
                'DP05_0010E',
                'DP05_0010PE',
                'DP05_0011E',
                'DP05_0011PE',
                'DP05_0012E',
                'DP05_0012PE',
                'DP05_0018E',
                'DP05_0037E',
                'DP05_0037PE',
                'DP05_0038E',
                'DP05_0038PE',
                'DP05_0039E',
                'DP05_0039PE',
                'DP05_0044E',
                'DP05_0044PE',
                'DP05_0052E',
                'DP05_0052PE',
                'DP05_0069E',
                'DP05_0069PE',
                'DP03_0018E',
                'DP03_0019E',
                'DP03_0019PE',
                'DP03_0020E',
                'DP03_0020PE',
                'DP03_0021E',
                'DP03_0021PE',
                'DP03_0022E',
                'DP03_0022PE',
                'DP03_0023E',
                'DP03_0023PE',
                'DP03_0024E',
                'DP03_0024PE',
                'DP03_0002E',
                'DP03_0002PE',
                'DP03_0007E',
                'DP03_0007PE',
                'DP02_0057E',
                'DP02_0057PE',
                'DP02_0026E',
                'DP02_0026PE',
                'DP02_0032E',
                'DP02_0032PE',
                'DP02_0066E',
                'DP02_0066PE',
                'DP02_0064E',
                'DP02_0064PE',
                'DP02_0065E',
                'DP02_0065PE',
                'DP02_0001E',
                'DP02_0002E',
                'DP02_0002PE',
                'DP02_0010E',
                'DP02_0010PE',
                'DP03_0062E',
                'DP04_0134E',
                'DP03_0052PE',
                'DP03_0053PE',
                'DP03_0054PE',
                'DP03_0055PE',
                'DP03_0056PE',
                'DP03_0057PE',
                'DP03_0058PE',
                'DP03_0059PE',
                'DP03_0060PE',
                'DP03_0061PE',
                'DP03_0025E',
                'DP04_0058PE',
                'DP04_0059PE',
                'DP04_0060PE',
                'DP04_0061PE'
                 ]

    code_book = [
                'zcta',
                'population_total',
                'sex_male',
                'sex_male_percent',
                'sex_female',
                'sex_female_percent',
                'age_20_24',
                'age_20_24_percent',
                'age_25_34',
                'age_25_34_percent',
                'age_35_44',
                'age_35_44_percent',
                'age_45_54',
                'age_45_54_percent',
                'age_median',
                'race_white',
                'race_white_percent',
                'race_black',
                'race_black_percent',
                'race_am_indian_alaska_ntv',
                'race_am_indian_alaska_ntv_percent',
                'race_asian',
                'race_asian_percent',
                'race_ntv_hi_pacific_isl',
                'race_ntv_hi_pacific_isl_percent',
                'race_multiracial',
                'race_multiracial_percent',
                'workers_pop',
                'workers_commuting_vehicle_solo',
                'workers_commuting_vehicle_solo_percent',
                'workers_commuting_vehicle_carpool',
                'workers_commuting_vehicle_carpool_percent',
                'workers_commuting_public_transit',
                'workers_commuting_public_transit_percent',
                'workers_commuting_walked',
                'workers_commuting_walked_percent',
                'workers_commuting_other',
                'workers_commuting_other_percent',
                'workers_commuting_wfh',
                'workers_commuting_wfh_percent',
                'employment_status_laborforce',
                'employment_status_laborforce_percent',
                'employment_status_not_laborforce',
                'employment_status_not_laborforce_percent',
                'school_enrollment_college',
                'school_enrollment_college_percent',
                'marital_status_male_married',
                'marital_status_male_married_percent',
                'marital_status_female_married',
                'marital_status_female_married_percent',
                'education_attainment_high_school',
                'education_attainment_high_school_percent',
                'education_attainment_bachelor',
                'education_attainment_bachelor_percent',
                'education_attainment_graduate_degree',
                'education_attainment_graduate_degree_percent',
                'households_total',
                'households_type_family',
                'households_type_family_percent',
                'households_type_nonfamily',
                'households_type_nonfamily_percent',
                'households_income_median_dollars',
                'gross_rent_median',
                'income_less_than_10000_percent',
                'income_10000_to_14999_percent',
                'income_15000_to_24999_percent',
                'income_25000_to_34999_percent',
                'income_35000_to_49999_percent',
                'income_50000_to_74999_percent',
                'income_75000_to_99999_percent',
                'income_100000_to_149999_percent',
                'income_150000_to_199999_percent',
                'income_over_200000_percent',
                'workers_commuting_mean_travel_time_minutes',
                'household_no_vehicle_available_percent',
                'household_1_vehicle_available_percent',
                'household_2_vehicles_available_percent',
                'household_3_or_more_vehicles_available_percent'
                ]

    years = ['2018']

    census_output = pandas.DataFrame()

    for year in years:
        logger.info(f'extracting acs data for {year}')
        for each in variables:

            tmp = requests.get(
                'https://api.census.gov/data/{}/acs/acs5/profile?get={}&for=zip%20code%20tabulation%20area:*&key={}'.format(
                    year, each, api_key))

            tmp = tmp.json()

            tmp = pandas.DataFrame(tmp[1:], columns=tmp[0])

            tmp = tmp[['zip code tabulation area', str(each)]]

            if census_output.empty is True:

                census_output = census_output.append(tmp, ignore_index=True)

            else:

                census_output = pandas.merge(census_output,
                                             tmp,
                                             on='zip code tabulation area',
                                             how='outer')
        logger.info(f'extraction for {year} finished!')

    # convert values to float from str
    census_output[variables] = census_output[variables].astype(float)

    # variables to convert to %
    percent_vars = [
                'DP05_0002PE',
                'DP05_0003PE',
                'DP05_0009PE',
                'DP05_0010PE',
                'DP05_0011PE',
                'DP05_0012PE',
                'DP05_0037PE',
                'DP05_0038PE',
                'DP05_0039PE',
                'DP05_0044PE',
                'DP05_0052PE',
                'DP05_0069PE',
                'DP03_0019PE',
                'DP03_0020PE',
                'DP03_0021PE',
                'DP03_0022PE',
                'DP03_0023PE',
                'DP03_0024PE',
                'DP03_0002PE',
                'DP03_0007PE',
                'DP02_0057PE',
                'DP02_0026PE',
                'DP02_0032PE',
                'DP02_0066PE',
                'DP02_0064PE',
                'DP02_0065PE',
                'DP02_0002PE',
                'DP02_0010PE',
                'DP03_0052PE',
                'DP03_0053PE',
                'DP03_0054PE',
                'DP03_0055PE',
                'DP03_0056PE',
                'DP03_0057PE',
                'DP03_0058PE',
                'DP03_0059PE',
                'DP03_0060PE',
                'DP03_0061PE',
                'DP04_0058PE',
                'DP04_0059PE',
                'DP04_0060PE',
                'DP04_0061PE'
    ]

    # divide variables that should be percent by 100 so they'll be used as percents
    census_output[percent_vars] = census_output[percent_vars] / 100

    # assign labels to columns
    census_output.columns = code_book

    # replace incorrect ACS values with NULLs
    census_output.replace(-666666666, pandas.NA, inplace=True)
    census_output.replace(-6666666.66, pandas.NA, inplace=True)

    return census_output

