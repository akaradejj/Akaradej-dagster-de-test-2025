import dagster as dg
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb

# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
@dg.asset(compute_kind="duckdb", group_name="plan")
def kpi_fy(context: dg.AssetExecutionContext):
    #ไฟล์ kpi_fy มาจาก function pivot_data
    kpi_fy_pivot = pivot_data()
    load_to_duckdb(kpi_fy_pivot, "KPI_FY") #ใส่ชื่อ

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan")
def m_center(context: dg.AssetExecutionContext):
    #ไฟล์มาจาก m_center มาจาก function read_csv
    m_center = read_csv()
    load_to_duckdb(m_center, "M_Center") #ใส่ชื่อตาราง

# 2.3.2 Create asset kpi_fy_final_asset()
# Asset สำหรับการโหลด KPI_FY_Final
@dg.asset(
    compute_kind="duckdb", 
    group_name="plan", 
    required_resource_keys={"duckdb_conn"},  # ใช้ DuckDB connection
    dependencies=["kpi_fy", "m_center"]  # กำหนด dependencies ไปยัง asset kpi_fy และ m_center
)

def kpi_fy_final_asset(context: dg.AssetExecutionContext):
    conn = context.resources.duckdb_conn # เชื่อมต่อ DuckDB

    # ดึงข้อมูลจากตาราง KPI_FY และ M_Center
    kpi_fy_df = conn.execute("SELECT * FROM KPI_FY").fetchdf()
    m_center_df = conn.execute("SELECT * FROM M_Center").fetchdf()

    # join ตาราง KPI_FY และ M_Center โดยคอลัมน์ Center_ID
    merged_df = kpi_fy_df.merge(m_center_df[['Center_ID', 'Center_Name']], on='Center_ID', how='left')
    merged_df['updated_at'] = datetime.now() # เพิ่มคอลัมน์ติดตามเวลาที่อัปเดตข้อมูล

    # โหลดข้อมูลลงในตาราง KPI_FY_Final
    load_to_duckdb(merged_df, "KPI_FY_Final", conn)

    # บันทึกข้อมูลระหว่างการทำงานของ Dagster Asset
    context.log.info(f"Data loaded into KPI_FY_Final with {len(merged_df)} records.")
    
    return merged_df 