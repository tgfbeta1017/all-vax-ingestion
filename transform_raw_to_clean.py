from google.cloud import bigquery

# === CONFIGURATION ===
RAW_TABLE = "all-vax.all_vax.all-vax-raw"
CLEAN_TABLE = "all-vax.all_vax.clean-table"

def transform_raw_to_clean():
    client = bigquery.Client()

    query = f"""
        INSERT INTO `{CLEAN_TABLE}` (
        ASSIGNEE,
        PROVIDER_PIN,
        PROVIDER_NAME,
        PRAC_TYPE,
        FAMILY,
        PCT_OTHER,
        PCT_UNK,
        UNKNOWN_GENDER,
        HL7_ETHNICITY_COUNT_8_DAYS,
        HL7_TOTAL_COUNT_8_DAYS,
        ETHNITCITY_PCT,
        OVER_100_IMM_SHOTS,
        IMM_DEDUP_QUEUE_COUNT,
        PATIENT_DEDUP_QUEUE_COUNT,
        BABY_IN_NAMES,
        BABY_IN_NAMES_ROLLING_6_MONTHS,
        SHOT_ADMIN_DATE_ON_DOB,
        ALL_VAX_COUNT_LESS_72_8_DAYS,
        ALL_VAX_COUNT_MORE_72_8_DAYS,
        Week
        )
        SELECT
            ASSIGNEE,
            PROVIDER_PIN,
            PROVIDER_NAME,
            PRAC_TYPE,
            FAMILY,
            COALESCE(SAFE_CAST(PCT_OTHER AS INT64), 0),
            COALESCE(SAFE_CAST(PCT_UNK AS INT64), 0),
            COALESCE(SAFE_CAST(UNKNOWN_GENDER AS INT64), 0),
            COALESCE(SAFE_CAST(HL7_ETHNICITY_COUNT_8_DAYS AS INT64), 0),
            COALESCE(SAFE_CAST(HL7_TOTAL_COUNT_8_DAYS AS INT64), 0),
            COALESCE(SAFE_CAST(ETHNITCITY_PCT AS FLOAT64), 0) AS ETHNICITY_PCT,
            COALESCE(SAFE_CAST(OVER_100_IMM_SHOTS AS INT64), 0),
            COALESCE(SAFE_CAST(IMM_DEDUP_QUEUE_COUNT AS INT64), 0),
            COALESCE(SAFE_CAST(PATIENT_DEDUP_QUEUE_COUNT AS INT64), 0),
            COALESCE(SAFE_CAST(BABY_IN_NAMES AS INT64), 0),
            COALESCE(SAFE_CAST(BABY_IN_NAMES_ROLLING_6_MONTHS AS INT64), 0),
            COALESCE(SAFE_CAST(SHOT_ADMIN_DATE_ON_DOB AS INT64), 0),
            COALESCE(SAFE_CAST(ALL_VAX_COUNT_LESS_72_8_DAYS AS INT64), 0),
            COALESCE(SAFE_CAST(ALL_VAX_COUNT_MORE_72_8_DAYS AS INT64), 0),
            PARSE_DATE('%m/%d/%Y', Week)
        FROM `{RAW_TABLE}`;

    """

    job = client.query(query)
    job.result()

    print("✅ Transformed and cleaned data from raw to clean table (nulls replaced with 0).")

if __name__ == "__main__":
    transform_raw_to_clean()
