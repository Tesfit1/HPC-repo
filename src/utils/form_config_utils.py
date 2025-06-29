FORM_CONFIGS = {
    "BloodSamplingForPK": {
        "required_columns": ['Subject Number', 'Folder', 'Time Point', 'Sample Date', 'Sample Time', 'Comment'],
        "rename_map": {
            'Subject Number': 'subject',
            'Folder': 'Folder',
            'Time Point': 'TPT_TPT_1',
            'Sample Date': 'DAT_TPT_1',
            'Sample Time': 'TIM_TPT_1',
            'Comment': 'COM_TPT_1'
        },
        "item_mappings": {
            "TPT_TPT_1": "TPT_TPT_1",
            "PERF_TPT_1": "PERF_TPT_1",
            "DAT_TPT_1": "DAT_TPT_1",
            "TIM_TPT_1": "TIM_TPT_1",
            "FAST_TPT_1": "FAST_TPT_1",
            "COM_TPT_1": "COM_TPT_1"
        },
        "form_name": "SAMP_TPT_IS",
        "itemgroup_name": "ig_SAMP_TPT_01_A_1",
        "eventgroup_logic": lambda row: 'eg_SCREEN' if row['Folder'] == 'V01' else 'eg_TREAT_SD',
        "event_logic": lambda row: (
            'ev_V01' if row['Folder'] == 'V01' else
            'ev_V02' if row['Folder'] == 'V02' else
            'ev_V03' if row['Folder'] == 'V03' else
            'ev_V04'
        )
    },
    "Demographics": {
        "required_columns": [
            'Subject Number', 'Birth Year', 'Age', 'Sex', 'Child Bearing Potential',
            'Gender', 'Ethnicity', 'American Indian or Alaska Native', 'Asian',
            'Black or African American', 'Native Hawaiian or Other Pacific Islander', 'White'
        ],
        "rename_map": {
            'Subject Number': 'subject',
            'Birth Year': 'BRTHDAT',
            'Age': 'AGE',
            'Sex': 'SEX',
            'Child Bearing Potential': 'CHILDPOT_1',
            'Gender': 'GENIDENT',
            'Ethnicity': 'ETHNIC',
            'American Indian or Alaska Native': 'RACE_1',
            'Asian': 'RACE_2',
            'Black or African American': 'RACE_3',
            'Native Hawaiian or Other Pacific Islander': 'RACE_4',
            'White': 'RACE_5'
        },
        "value_mappings": {
        "ETHNIC": {
            "Not Hispanic or Latino": "NOT HISPANIC OR LATINO",
            "Hispanic or Latino": "HISPANIC OR LATINO"
        },
        "SEX": {
            "Male": "M",
            "Female": "F"
        },
        "RACE_1": {"American Indian or Alaska Native": True},
        "RACE_2": {"Asian": True},
        "RACE_3": {"Black or African American": True},
        "RACE_4": {"Native Hawaiian or Other Pacific Islander": True},
        "RACE_5": {"White": True}
    },
        "item_mappings": {
            "BRTHDAT": "BRTHDAT",
            "AGE": "AGE",
            "SEX": "SEX",
            # "CHILDPOT_1": "CHILDPOT_1",
            # "GENIDENT": "GENIDENT",
            "ETHNIC": "ETHNIC",
            "RACE_1": "RACE_1",
            "RACE_2": "RACE_2",
            "RACE_3": "RACE_3",
            "RACE_4": "RACE_4",
            "RACE_5": "RACE_5"
        },
        "unit_mappings": {
        "AGE": "Year"
        },
        "form_name": "DM_02_v001",
        "itemgroup_name": "ig_DM_02_A",
        "eventgroup_name": "eg_SCREEN",
        "event_name": "ev_V01"
    },
     "DrugAdmin": {
        "required_columns": [
            'Subject Number', 'Folder', 'Time Point', 'Administration Date', 'Administration Time', 'Comment'
        ],
        "rename_map": {
            'Subject Number': 'subject',
            'Folder': 'Folder',
            'Time Point': 'ECTPT',
            'Administration Date': 'ECSTDAT',
            'Administration Time': 'ECSTTIM',
            'Comment': 'COVAL'
        },
        "item_mappings": {
            "ECTPT": "ECTPT",
            "ECOCCUR": "ECOCCUR",
            "ECSTDAT": "ECSTDAT",
            "ECSTTIM": "ECSTTIM",
            "COVAL": "COVAL"
        },
        "form_name": "EC_01_v002",
        "itemgroup_name": "ig_EC_01_A",
        "eventgroup_logic": lambda row: 'eg_SCREEN' if row['Folder'] == 'V01' else 'eg_TREAT_SD',
        "event_logic": lambda row: (
            'ev_V01' if row['Folder'] == 'V01' else
            'ev_V02' if row['Folder'] == 'V02' else
            'ev_V03' if row['Folder'] == 'V03' else
            'ev_V04'
        )
    },
     "Eligibility": {
        "required_columns": [
            'Subject Number', 'Randomized', 'Reason Non-Randomized', 'Randomization Date', 'Randomization Number'
        ],
        "rename_map": {
            'Subject Number': 'subject',
            'Randomized': 'DSCOMP_ELIG',
            'Reason Non-Randomized': 'DSNCOMP_ELIG',
            'Randomization Date': 'DSSTDAT_ELIG',
            'Randomization Number': 'RANDNO'
        },
        "item_mappings": {
            "DSCOMP_ELIG": "DSCOMP_ELIG",
            "DSNCOMP_ELIG": "DSNCOMP_ELIG",
            "DSSTDAT_ELIG": "DSSTDAT_ELIG",
            "RANDNO": "RANDNO"
        },
        "value_mappings": {
        "DSCOMP_ELIG": {"Yes": "Y", "No": "N"},
        "DSNCOMP_ELIG": {
            "Adverse Event": "ADVERSE EVENT",
            "Screen Failure": "SCREEN FAILURE",
            "Screened in Error": "SCREENED IN ERROR",
            "Lost to Follow-Up": "LOST TO FOLLOW-UP",
            "Withdrawal by Subject": "WITHDRAWAL BY SUBJECT",
            "Other": "OTHER"
        }
    },
        "form_name": "ELIG_02_v001",
        "itemgroup_name": "ig_ELIG_02_A",
        "eventgroup_name": "eg_COMMON",
        "event_name": "ev_COMMON"
    },
     "EOS": {
        "required_columns": [
            'Subject Number', 'EOS Date', 'Completed', 'Reason Non-Completion', 'Death date', 'Lost to FUP date'
        ],
        "rename_map": {
            'Subject Number': 'subject',
            'EOS Date': 'DSSTDAT_EOS',
            'Completed': 'DSCOMP_EOS',
            'Reason Non-Completion': 'DSNCOMP_EOS',
            'Death date': 'DTHDAT_EOS',
            'Lost to FUP date': 'LTFDAT'
        },
        "item_mappings": {
            "DSSTDAT_EOS": "DSSTDAT_EOS",
            "DSCOMP_EOS": "DSCOMP_EOS",
            "DSNCOMP_EOS": "DSNCOMP_EOS",
            "DSNCOMP_SPECIFY": "reason",
            "DTHDAT_EOS": "DTHDAT_EOS",
            "LTFDAT": "LTFDAT"
        },
        "value_mappings": {
            "DSNCOMP_EOS": {
                "Lost to Follow-Up": "LOST TO FOLLOW-UP",
                "Death": "DEATH",
                "Withdrawal by Subject": "WITHDRAWAL BY SUBJECT",
                "other": "OTHER"
            },
            "DSCOMP_EOS": {
                "Yes": "Y",
                "No": "N"
            }
        },
        "form_name": "EOS_01_v002",
        "itemgroup_name": "ig_EOS_01_A",
        "eventgroup_name": "eg_COMMON",
        "event_name": "ev_COMMON"
    },
    "Inc_Ex": {
        "required_columns": [
            'Subject Number', 'CTP Version', 'Criteria Meet', 'Criterion Category', 'Criterion Number'
        ],
        "rename_map": {
            'Subject Number': 'subject',
            'CTP Version': 'CTPNUMG',
            'Criteria Meet': 'IEYN',
            'Criterion Category': 'IECAT',
            'Criterion Number': 'IENUM'
        },
        "item_mappings": {
            "CTPNUMG": "CTPNUMG",
            "IEYN": "IEYN",
            "IECAT": "IECAT",
            "IENUM": "IENUM"
        },
        "value_mappings": {
            "IEYN": {"Yes": "Y", "No": "N"}
        },
        "form_name": "IE_01_v001",
        "itemgroup_name_A": "ig_IE_01_A",  # non-repeating
        "itemgroup_name_B": "ig_IE_01_B",  # repeating
        "eventgroup_name": "eg_COMMON",
        "event_name": "ev_COMMON",
        
    },
    "IC": {
    "required_columns": [
        'Subject Number', 'Informed Consent Type', 'Informed Consent Version ID', 'Informed Consent Obtained', 'Informed Consent Date'
    ],
    "rename_map": {
        'Subject Number': 'subject',
        'Informed Consent Type': 'DSSCAT_IC',
        'Informed Consent Version ID': 'DSREFID_IC',
        'Informed Consent Obtained': 'IC',
        'Informed Consent Date': 'DSSTDAT_IC'
    },
    "item_mappings": {
        "DSSCAT_IC": "DSSCAT_IC",
        "DSREFID_IC": "DSREFID_IC",
        "IC": "IC",
        "DSSTDAT_IC": "DSSTDAT_IC"
    },
    "value_mappings": {
        "IC": {"Yes": "Y", "No": "N"},
        "DSSCAT_IC": {"Main": "MAIN"}
    },
    "form_name": "IC_01_v002",
    "itemgroup_name": "ig_IC_01_A",
    "eventgroup_name": "eg_COMMON",
    "event_name": "ev_COMMON"
},
"IntervalSampling": {
    "required_columns": [
        'Subject Number', 'Folder', 'Planned Start Time Point', 'Planned Stop Time Point',
        'Sample Start Date', 'Sample Start Time', 'Sample Stop Date', 'Sample Stop Time',
        'Weight Empty', 'Weight Filled', 'Comment'
    ],
    "rename_map": {
        'Subject Number': 'subject',
        'Folder': 'Folder',
        'Planned Start Time Point': 'TPT_INT',
        'Planned Stop Time Point': 'TPTSP_INT',
        'Sample Start Date': 'DAT_INT',
        'Sample Start Time': 'TIM_INT',
        'Sample Stop Date': 'ENDAT_INT',
        'Sample Stop Time': 'ENTIM_INT',
        'Weight Empty': 'CEWT_INT',
        'Weight Filled': 'CFWT_INT',
        'Comment': 'COM_INT'
    },
    "item_mappings": {
        "TPT_INT": "TPT_INT",
        "TPTSP_INT": "TPTSP_INT",
        "DAT_INT": "DAT_INT",
        "TIM_INT": "TIM_INT",
        "ENDAT_INT": "ENDAT_INT",
        "ENTIM_INT": "ENTIM_INT",
        "CEWT_INT": "CEWT_INT",
        "CFWT_INT": "CFWT_INT",
        "COM_INT": "COM_INT"
    },
    "form_name": "SAMP_INT_01_v002",
    "itemgroup_name": "ig_SAMP_INT_01_A",
    "eventgroup_logic": lambda row: 'eg_SCREEN' if row['Folder'] == 'V01' else 'eg_TREAT_SD',
    "event_logic": lambda row: (
        'ev_V01' if row['Folder'] == 'V01' else
        'ev_V02' if row['Folder'] == 'V02' else
        'ev_V03' if row['Folder'] == 'V03' else
        'ev_V04'
    )
},
"SubstanceUse": {
    "required_columns": [
        'Subject Number', 'Tobacco Use', 'Vaping Product'
        # 'Alcohol Use'  # Uncomment if needed
    ],
    "rename_map": {
        'Subject Number': 'subject',
        'Tobacco Use': 'SUNCF_TOB',
        'Vaping Product': 'SUNCF_VAP',
        # 'Alcohol Use': 'SUNCF_ALC'
    },
    "item_mappings": {
        "SUNCF_TOB": "SUNCF_TOB",
        "SUNCF_VAP": "SUNCF_VAP",
        # "SUNCF_ALC": "SUNCF_ALC"
    },
    "value_mappings": {
        "SUNCF_TOB": {"Current": "CURRENT", "Former": "FORMER", "Never": "NEVER"},
        "SUNCF_VAP": {"Current": "CURRENT", "Former": "FORMER", "Never": "NEVER"},
        # "SUNCF_ALC": {"Current": "CURRENT", "Former": "FORMER", "Never": "NEVER"}
    },
    "form_name": "SU_01_v001",
    "itemgroup_name": "ig_SU_01_A",
    "eventgroup_name": "eg_SCREEN",
    "event_name": "ev_V01"
},
"TreatmentSummary": {
    "required_columns": [
        'Subject Number', 'Subject completed', 'Treatment discontinuation decision',
        'Primary trt discontinuation reason', 'Reason Subject Not Treated'
    ],
    "rename_map": {
        'Subject Number': 'subject',
        'Subject completed': 'DSCOMP_TPS',
        'Treatment discontinuation decision': 'DSDECIS',
        'Primary trt discontinuation reason': 'DSREAS',
        'Reason Subject Not Treated': 'DSNTRTR'
    },
    "item_mappings": {
        "DSCOMP_TPS": "DSCOMP_TPS",
        "DSDECIS": "DSDECIS",
        "DSREAS": "DSREAS",
        "DSNTRTR": "DSNTRTR"
    },
    "form_name": "TPS_03_VISIT2",
    "itemgroup_name": "ig_TPS_03_A",
    "eventgroup_name": "eg_COMMON",
    "event_name": "ev_COMMON"
},
"VitalSignEOS": {
    "required_columns": [
        'Subject Number', 'Date of Measurements', 'Weight', 'Pulse Rate',
        'Systolic Blood Pressure', 'Diastolic Blood Pressure'
        # Add 'Respiratory Rate', 'Temperature' if needed
    ],
    "rename_map": {
        'Subject Number': 'subject',
        'Date of Measurements': 'VSDAT_2',
        'Weight': 'WEIGHT_2',
        'Pulse Rate': 'PULSE_2',
        'Systolic Blood Pressure': 'SYSBP_2',
        'Diastolic Blood Pressure': 'DIABP_2',
        # 'Respiratory Rate': 'RESP_2',
        # 'Temperature': 'TEMP_2'
    },
    "item_mappings": {
        "VSDAT_2": "VSDAT_2",
        "WEIGHT_2": "WEIGHT_2",
        "PULSE_2": "PULSE_2",
        "SYSBP_2": "SYSBP_2",
        "DIABP_2": "DIABP_2",
        # "RESP_2": "RESP_2",
        # "TEMP_2": "TEMP_2"
    },
    "unit_mappings": {
        "WEIGHT_2": "Kilogram",
        "PULSE_2": "Beats per Minute",
        "SYSBP_2": "Millimeter of Mercury",
        "DIABP_2": "Millimeter of Mercury",
        # "RESP_2": "Breaths per Minute",
        # "TEMP_2": "Degree-Celsius"
    },
    "form_name": "VS_01_EOS",
    "itemgroup_name": "ig_VS_01_A_2",
    "eventgroup_name": "eg_EOS",
    "event_name": "ev_EOS"
},
"VitalSignScreening": {
    "required_columns": [
        'Subject Number', 'Date of Measurement', 'Time of Measurement', 'Height', 'Weight',
        'Pulse Rate', 'Systolic Blood Pressure', 'Diastolic Blood Pressure'
        # Add 'Respiratory Rate', 'Temperature', 'Oxygen Saturation' if needed
    ],
    "rename_map": {
        'Subject Number': 'subject',
        'Date of Measurement': 'VSDAT',
        'Time of Measurement': 'VSTIM',
        'Height': 'HEIGHT',
        'Weight': 'WEIGHT',
        'Pulse Rate': 'PULSE',
        'Systolic Blood Pressure': 'SYSBP',
        'Diastolic Blood Pressure': 'DIABP',
        # 'Respiratory Rate': 'RESP',
        # 'Temperature': 'TEMP',
        # 'Oxygen Saturation': 'OXYSAT'
    },
    "item_mappings": {
        "VSDAT": "VSDAT",
        "VSTIM": "VSTIM",
        "HEIGHT": "HEIGHT",
        "WEIGHT": "WEIGHT",
        "PULSE": "PULSE",
        "SYSBP": "SYSBP",
        "DIABP": "DIABP",
        # "RESP": "RESP",
        # "TEMP": "TEMP",
        # "OXYSAT": "OXYSAT"
    },
    "unit_mappings": {
        "HEIGHT": "Centimeter",
        "WEIGHT": "Kilogram",
        "PULSE": "Beats per Minute",
        "SYSBP": "Millimeter of Mercury",
        "DIABP": "Millimeter of Mercury",
        # "RESP": "Breaths per Minute",
        # "TEMP": "Degree-Celsius",
        # "OXYSAT": "Percent"
    },
    "form_name": "VS_01_SCREEN",
    "itemgroup_name": "ig_VS_01_A",
    "eventgroup_name": "eg_SCREEN",
    "event_name": "ev_V01"
},
"VitalSignTreatment": {
    "required_columns": [
        'Subject Number', 'Folder', 'Planned Time Point', 'Date of Measurement', 'Time of Measurement',
         'Systolic Blood Pressure', 'Diastolic Blood Pressure', 'Respiratory Rate', 'Temperature'
    ],
    "rename_map": {
        'Subject Number': 'subject',
        'Folder': 'Folder',
        'Planned Time Point': 'VSTPT_1',
        'Date of Measurement': 'VSDAT_1',
        'Time of Measurement': 'VSTIM_1',
        'Weight': 'WEIGHT_1',
        'Pulse Rate': 'PULSE_1',
        'Systolic Blood Pressure': 'SYSBP_1',
        'Diastolic Blood Pressure': 'DIABP_1',
        'Respiratory Rate': 'RESP_1',
        'Temperature': 'TEMP_1'
    },
    "item_mappings": {
        "VSTPT_1": "VSTPT_1",
        "VSDAT_1": "VSDAT_1",
        "VSTIM_1": "VSTIM_1",
        # "WEIGHT_1": "WEIGHT_1",
        # "PULSE_1": "PULSE_1",
        "SYSBP_1": "SYSBP_1",
        "DIABP_1": "DIABP_1",
        # "RESP_1": "RESP_1",
        # "TEMP_1": "TEMP_1"
    },
    "unit_mappings": {
        "WEIGHT_1": "Kilogram",
        "PULSE_1": "Beats per Minute",
        "SYSBP_1": "Millimeter of Mercury",
        "DIABP_1": "Millimeter of Mercury",
        "RESP_1": "Breaths per Minute",
        "TEMP_1": "Degree-Celsius"
    },
    "form_name": "VS_01_TREAT",
    "itemgroup_name": "ig_VS_01_A_1",
    "eventgroup_logic": lambda row: 'eg_SCREEN' if row['Folder'] == 'V01' else 'eg_TREAT_SD',
    "event_logic": lambda row: (
        'ev_V01' if row['Folder'] == 'V01' else
        'ev_V02' if row['Folder'] == 'V02' else
        'ev_V03' if row['Folder'] == 'V03' else
        'ev_V04'
    )
},
"WithdrawalIC": {
    "required_columns": ['subject', 'Informed Consent Record', 'Withdrawal Date'],
    "rename_map": {
        'Informed Consent Record': 'LK_WIC_IC',
        'Withdrawal Date': 'DSSTDAT_WIC'
    },
    "item_mappings": {
        "LK_WIC_IC": "LK_WIC_IC",
        "DSSTDAT_WIC": "DSSTDAT_WIC"
    },
    "item_links": {
        "LK_WIC_IC": {
            "item_to_form_link": {
                "eventgroup_name": "eg_COMMON",
                "event_name": "ev_COMMON",
                "form_name": "IC_01_v002",
                "form_sequence": 2
            }
        }
    },
    "form_name": "WIC_01_v002",
    "itemgroup_name": "ig_WIC_01_A",
    "eventgroup_name": "eg_COMMON",
    "event_name": "ev_COMMON"
}

    # Add more configs for other forms/scripts here...
}
EVENT_CONFIGS = {
    "VisitTwoDate": {
        "required_columns": ['Subject Number', 'Informed Consent Date'],
        "rename_map": {
            'Subject Number': 'subject',
            'Informed Consent Date': 'DSSTDAT_IC'
        },
        "eventgroup_name": "eg_TREAT_SD",
        "eventgroup_sequence": 1,
        "event_name": "ev_V02",
        "date_column": "DSSTDAT_IC",
        "event_date_conversion": True  # Use convert_date_format
    },
    "VisitThreeDate": {
        "required_columns": ['Subject Number', 'Informed Consent Date'],
        "rename_map": {
            'Subject Number': 'subject',
            'Informed Consent Date': 'DSSTDAT_IC'
        },
        "eventgroup_name": "eg_TREAT_SD",
        "eventgroup_sequence": 1,
        "event_name": "ev_V03",
        "date_column": "DSSTDAT_IC",
        "event_date_conversion": True
    },
    "VisitFourDate": {
    "required_columns": ['Subject Number', 'Informed Consent Date'],
    "rename_map": {
        'Subject Number': 'subject',
        'Informed Consent Date': 'DSSTDAT_IC'
    },
    "eventgroup_name": "eg_TREAT_SD",
    "eventgroup_sequence": 1,
    "event_name": "ev_V04",
    "date_column": "DSSTDAT_IC",
    "event_date_conversion": True
},    
    "VisitPlaceDate": {
    "required_columns": ['Subject Number', 'Folder', 'Visit Date'],
    "rename_map": {
        'Subject Number': 'subject',
        'Folder': 'folder',
        'Visit Date': 'visit_date'
    },
    "eventgroup_name": lambda row: 'eg_SCREEN' if row['folder'] == 'V01' else 'eg_TREAT_SD',
    "eventgroup_sequence": 1,
    "event_name": lambda row: f"ev_{row['folder']}",
    "date_column": "visit_date",
    "event_date_conversion": True
},
"SetEventDateEndOfStudy": {
    "required_columns": ['Subject Number', 'Informed Consent Date'],
    "rename_map": {
        'Subject Number': 'subject',
        'Informed Consent Date': 'DSSTDAT_IC'
    },
    "eventgroup_name": "eg_EOS",
    "eventgroup_sequence": 1,
    "event_name": "ev_EOS",
    "date_column": "DSSTDAT_IC",
    "event_date_conversion": True       
    # Add similar configs for VisitOneDate, VisitThreeDate, etc.
}
}