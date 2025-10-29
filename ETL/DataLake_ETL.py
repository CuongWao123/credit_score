"""
ETL: Read MinIO + PostgreSQL -> Join -> Write to training-data bucket
"""
from datetime import datetime
from utils import MinIOClient, DataJoiner
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


APPLICATIONS_COLS = [
    "reco_id_curr","target","contract_type_name","gender","own_car_flag","own_realty_flag",
    "children_count","income","loan_body","annuity_payment","goods_price","type_suite_name",
    "income_type_name","education_type_name","family_status_name","housing_type_name",
    "population_relative_region","days_birth","days_employed","registration_timestamp",
    "publication_timestamp","age_own_car","mobile_flag","employee_phone_flag",
    "work_phone_flag","mobile_contact_flag","phone_flag","email_flag","type_of_occupation",
    "family_members__count","rating_client_region","rating_client_w_city_region",
    "start_weekday_appr_process","hour_of_approval_process_start","not_live_region_reg_region",
    "not_work_region_reg_region","living_region_not_work_region_flag","not_live_city_reg_city",
    "not_work_city_reg_city","living_city_not_work_city_flag","type_of_organization",
    "external_source_1","external_source_2","external_source_3",
    "average_apartments","average_basementarea","average_years_beginexpluatation",
    "average_years_building","average_commonarea","average_elevator_count",
    "average_entrance_count","average_max_floors","average_min_floors","average_land_area",
    "average_living_apartments","average_living_area","non_living_apartments_avg",
    "non_living_area_avg","mode_apartments","mode_basementarea",
    "mode_years_beginexpluatation","mode_years_building","mode_commonarea",
    "mode_elevator_count","mode_entrance_count","mode_max_floors","mode_min_floors",
    "mode_land_area","mode_living_apartments","mode_living_area","non_living_apartments_mode",
    "non_living_area_mode","median_apartments","median_basementarea",
    "median_years_beginexpluatation","median_years_building","median_commonarea",
    "median_elevator_count","median_entrance_count","median_max_floors","median_min_floors",
    "median_land_area","median_living_apartments","median_living_area",
    "non_living_apartments_medi","non_living_area_medi","fondkapremon_mode","mode_house_type",
    "mode_total_area","mode_walls_material","emergency_state_mode",
    "observes_30_count_social_circle","social_circle_defaults_30_days",
    "observes_60_count_social_circle","social_circle_defaults_60_days",
    "last_phone_number_change","document_2_flag","document_3_flag","document_4_flag",
    "document_5_flag","document_6_flag","document_7_flag","document_8_flag","document_9_flag",
    "document_10_flag","document_11_flag","document_12_flag","document_13_flag",
    "document_14_flag","document_15_flag","document_16_flag","document_17_flag",
    "document_18_flag","document_19_flag","document_20_flag","document_21_flag",
    "requests_bki_hour","requests_bki_day","requests_bki_week","requests_bki_month",
    "requests_bki_qrt","requests_bki_year"
]

BKI_COLS = [
    "reco_id_curr","reco_bureau_id","credit_status","credit_currency","days_credit",
    "credit_day_overdue","days_credit_enddate","days_enddate_fact","credit_limit_max_overdue",
    "credit_prolong_count","credit_sum","credit_sum_debt","credit_sum_limit",
    "credit_sum_overdue","credit_type","days_credit_update","bureau_annuity_payment"
]

BKI_BALANCE_COLS = ["reco_bureau_id","months_balance","stat_for_bureau"]


def read_from_postgres(db_config: dict, query: str) -> pd.DataFrame:
    """
    Read data from PostgreSQL database
    
    Args:
        db_config: Database connection config
        query: SQL query string
        
    Returns:
        DataFrame
    """
    try:
        # Create SQLAlchemy engine
        connection_string = (
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        engine = create_engine(connection_string)
        
        logger.info(f"🔌 Connecting to PostgreSQL: {db_config['host']}:{db_config['port']}/{db_config['database']}")
        logger.info(f"📊 Executing query...")
        
        # Read data
        df = pd.read_sql(query, engine)
        
        logger.info(f"✅ Query completed: {len(df):,} rows × {len(df.columns)} columns")
        
        engine.dispose()
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read from PostgreSQL: {e}")
        raise


def main():
    print("="*100)
    print("🚀 ETL: MinIO + PostgreSQL -> Training Data")
    print("="*100)
    
    try:
        # ====================================
        # Configuration
        # ====================================
        MINIO_CONFIG = {
            "endpoint": "localhost:9900",
            "access_key": "minioAccessKey",
            "secret_key": "minioSecretKey",
            "bucket": "applications-bureau-data",
            "use_ssl": False
        }
        
        POSTGRES_CONFIG = {
            "host": "localhost",
            "port": 5432,
            "database": "BankDB",
            "user": "postgres",
            "password": "postgres"
        }

        APPLICATIONS_PREFIX = "applications/"
        BUREAU_CREDITS_PREFIX = "bureau_credits/"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # ====================================
        # Step 1: Read from MinIO
        # ====================================
        logger.info("\n" + "="*80)
        logger.info("STEP 1: Reading from MinIO")
        logger.info("="*80)
        
        minio = MinIOClient(**MINIO_CONFIG)

        logger.info("  📂 Reading applications...")
        apps = minio.read_all_partitions(APPLICATIONS_PREFIX)
        apps = apps.loc[:, [c for c in APPLICATIONS_COLS if c in apps.columns]]
        logger.info(f"     ✅ Applications: {len(apps):,} rows × {len(apps.columns)} cols")

        logger.info("  📂 Reading bureau_credits...")
        bureau = minio.read_all_partitions(BUREAU_CREDITS_PREFIX)
        all_bureau_cols = BKI_COLS + BKI_BALANCE_COLS
        bureau = bureau.loc[:, [c for c in all_bureau_cols if c in bureau.columns]]
        logger.info(f"     ✅ Bureau credits: {len(bureau):,} rows × {len(bureau.columns)} cols")

        # ====================================
        # Step 2: Read from PostgreSQL
        # ====================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: Reading from PostgreSQL")
        logger.info("="*80)
        
        PREVIOUS_LOAN_QUERY = """
        SELECT
            COALESCE(pl.reco_id_last, cpb.reco_id_last, ccb.reco_id_last, ip.reco_id_last) AS reco_id_last,
            COALESCE(pl.reco_id_curr, cpb.reco_id_curr, ccb.reco_id_curr, ip.reco_id_curr) AS reco_id_curr,

            -- ====== previous_loan ======
            pl.contract_type_name AS prev_contract_type_name,
            pl.annuity_payment AS prev_annuity_payment,
            pl.loan_body_requested,
            pl.loan_body AS prev_loan_body,
            pl.first_payment,
            pl.goods_price AS prev_goods_price,
            pl.start_weekday_appr_process AS prev_start_weekday,
            pl.hour_of_approval_process_start AS prev_hour_approval,
            pl.last_application_per_contract_flag,
            pl.last_day_app_f,
            pl.down_payment_rate,
            pl.interest_primary_rate,
            pl.interest_privileged_rate,
            pl.cash_loan_purpose_name,
            pl.contract_status_name AS prev_contract_status,
            pl.days_decision,
            pl.payment_type_name,
            pl.reject_reason_code,
            pl.type_suite_name AS prev_type_suite_name,
            pl.client_type_name,
            pl.goods_category_name,
            pl.portfolio_name,
            pl.product_type_name,
            pl.channel_type,
            pl.area_seller_place,
            pl.seller_industry_name,
            pl.payment_count,
            pl.yield_group_name,
            pl.combination_of_product,
            pl.days_first_drawing,
            pl.days_first_due,
            pl.first_due_date,
            pl.last_due_date,
            pl.termination_date,
            pl.insured_last_f,

            -- ====== cash_pos_balance ======
            cpb.months_balance AS cashpos_months_balance,
            cpb.installment_count,
            cpb.installment_future_count,
            cpb.contract_status_name AS cashpos_contract_status_name,
            cpb.reco_dpd AS cashpos_reco_dpd,
            cpb.reco_dpd_def AS cashpos_reco_dpd_def,

            -- ====== credit_card_balance ======
            ccb.months_balance AS cc_months_balance,
            ccb.balance,
            ccb.credit_limit,
            ccb.drawings_atm,
            ccb.drawings,
            ccb.drawings_other,
            ccb.drawings_pos,
            ccb.minimal_payment,
            ccb.payment_now,
            ccb.payment_total_now,
            ccb.receivable_principal,
            ccb.receivable,
            ccb.total_receivable,
            ccb.drawings_atm_count,
            ccb.drawings_count,
            ccb.drawings_other_count,
            ccb.drawings_pos_count,
            ccb.installment_mature_cum_count,
            ccb.contract_status_name AS cc_contract_status_name,
            ccb.reco_dpd AS cc_reco_dpd,
            ccb.reco_dpd_def AS cc_reco_dpd_def,

            -- ====== installments_payments ======
            ip.number_of_instalment_ver,
            ip.number_of_instalment_num,
            ip.installment_due_date,
            ip.days_entry_payment,
            ip.first_payment_requested,
            ip.payment AS actual_payment

        FROM previous_loan pl
        LEFT JOIN cash_pos_balance cpb
            ON pl.reco_id_last = cpb.reco_id_last
           AND cpb.months_balance BETWEEN -3 AND 0     
        LEFT JOIN credit_card_balance ccb
            ON pl.reco_id_last = ccb.reco_id_last
           AND ccb.months_balance BETWEEN -3 AND 0
        LEFT JOIN installments_payments ip
            ON pl.reco_id_last = ip.reco_id_last
        WHERE pl.days_decision >= -365
        """
        
        logger.info("  🔍 Querying PostgreSQL (previous loans + balances)...")
        try:
            postgres_df = read_from_postgres(POSTGRES_CONFIG, PREVIOUS_LOAN_QUERY)
            logger.info(f"     ✅ PostgreSQL data: {len(postgres_df):,} rows × {len(postgres_df.columns)} cols")
            logger.info(f"     📊 Unique reco_id_curr: {postgres_df['reco_id_curr'].nunique():,}")
            has_postgres_data = True
        except Exception as e:
            logger.warning(f"     ⚠️  PostgreSQL query failed: {e}")
            logger.warning(f"     ⚠️  Continuing without PostgreSQL data...")
            has_postgres_data = False

        # ====================================
        # Step 3: Join MinIO data (apps + bureau)
        # ====================================
        logger.info("\n" + "="*80)
        logger.info("STEP 3: Joining MinIO data (applications + bureau)")
        logger.info("="*80)
        
        joined = apps.merge(
            bureau,
            on="reco_id_curr",
            how="left",
            suffixes=("", "_bureau"),
            validate="1:m"
        )
        joined = joined.loc[:, ~joined.columns.duplicated()]
        
        logger.info(f"  ✅ Join completed: {len(joined):,} rows × {len(joined.columns)} cols")
        logger.info(f"  📊 Unique applications: {joined['reco_id_curr'].nunique():,}")

        # ====================================
        # Step 4: Join with PostgreSQL data
        # ====================================
        if has_postgres_data:
            logger.info("\n" + "="*80)
            logger.info("STEP 4: Joining with PostgreSQL previous loans data")
            logger.info("="*80)
            
            logger.info(f"  🔗 Joining on 'reco_id_curr'...")
            final_df = joined.merge(
                postgres_df,
                on="reco_id_curr",
                how="left",
                suffixes=("", "_prev")
            )
            final_df = final_df.loc[:, ~final_df.columns.duplicated()]
            
            logger.info(f"  ✅ Final join completed: {len(final_df):,} rows × {len(final_df.columns)} cols")
            logger.info(f"  📊 Records with previous loan data: {final_df['reco_id_last'].notna().sum():,}")
            logger.info(f"  📊 Records with cash/pos balance: {final_df['cashpos_months_balance'].notna().sum():,}")
            logger.info(f"  📊 Records with credit card balance: {final_df['cc_months_balance'].notna().sum():,}")
            logger.info(f"  📊 Records with installments: {final_df['number_of_instalment_ver'].notna().sum():,}")
        else:
            final_df = joined
            logger.info("\n  ⚠️  Skipping PostgreSQL join (no data)")

        # ====================================
        # Step 5: Reorder columns
        # ====================================
        logger.info("\n" + "="*80)
        logger.info("STEP 5: Organizing final columns")
        logger.info("="*80)
        
        # Build final column list
        FINAL_COLUMNS = list(APPLICATIONS_COLS)  # Start with applications columns
        
        # Add bureau columns
        FINAL_COLUMNS += [
            "reco_bureau_id", "credit_status", "credit_currency", "days_credit",
            "credit_day_overdue", "days_credit_enddate", "days_enddate_fact",
            "credit_limit_max_overdue", "credit_prolong_count", "credit_sum",
            "credit_sum_debt", "credit_sum_limit", "credit_sum_overdue", "credit_type",
            "days_credit_update", "bureau_annuity_payment",
            "months_balance", "stat_for_bureau"
        ]
        
        # Add PostgreSQL columns if available
        if has_postgres_data:
            # Get all postgres columns except reco_id_curr (already exists)
            pg_cols = [col for col in postgres_df.columns if col != 'reco_id_curr']
            FINAL_COLUMNS += pg_cols
        
        # Keep only columns that exist in final_df
        final_cols = [c for c in FINAL_COLUMNS if c in final_df.columns]
        final_df = final_df.loc[:, final_cols]
        
        logger.info(f"  ✅ Final dataset: {len(final_df.columns)} columns")
        logger.info(f"     - Applications: {len(APPLICATIONS_COLS)} cols")
        logger.info(f"     - Bureau: 18 cols")
        if has_postgres_data:
            logger.info(f"     - Previous loans & balances: {len(pg_cols)} cols")

        # ====================================
        # Step 6: Write to MinIO training-data
        # ====================================
        logger.info("\n" + "="*80)
        logger.info("STEP 6: Writing to MinIO training-data bucket")
        logger.info("="*80)
        
        file_path = f"data_{timestamp}.parquet"
        
        s3_uri = minio.write_parquet_to_bucket(
            df=final_df,
            bucket_name="training-data",
            file_path=file_path
        )

        # ====================================
        # Summary
        # ====================================
        print("\n" + "="*100)
        print("✅ ETL COMPLETED SUCCESSFULLY!")
        print("="*100)
        print(f"📁 Output: {s3_uri}")
        print(f"📊 Final shape: {len(final_df):,} rows × {len(final_df.columns)} columns")
        print(f"\n📋 Data sources joined:")
        print(f"   1. MinIO - Applications: {len(apps):,} rows")
        print(f"   2. MinIO - Bureau credits: {len(bureau):,} rows")
        if has_postgres_data:
            print(f"   3. PostgreSQL - Previous loans: {postgres_df['reco_id_curr'].nunique():,} unique customers")
            print(f"      - Cash/POS balance records: {postgres_df['cashpos_months_balance'].notna().sum():,}")
            print(f"      - Credit card balance records: {postgres_df['cc_months_balance'].notna().sum():,}")
            print(f"      - Installment records: {postgres_df['number_of_instalment_ver'].notna().sum():,}")
        print(f"\n🎯 Training data ready for ML models!")
        print("="*100)

    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()