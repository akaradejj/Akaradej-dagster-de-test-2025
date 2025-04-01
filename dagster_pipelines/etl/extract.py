import pandas as pd

# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel() -> pd.DataFrame:
    #อ่านไฟล์ excel
    df = pd.read_excel("dagster_pipelines/data/KPI_FY.xlsm", sheet_name = 'Data to DB')
    return df

# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv() -> pd.DataFrame:
    #อ่านไฟล์ csv
    df = pd.read_csv("dagster_pipelines/data/M_Center.csv)
    return df