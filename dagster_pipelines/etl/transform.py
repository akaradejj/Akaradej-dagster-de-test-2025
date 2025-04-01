import pandas as pd

# 2.2.1 Pivot data in the "KPI_FY.xlsm" file
def pivot_data() -> pd.DataFrame:
    df = pd.read_excel("dagster_pipelines/data/KPI_FY.xlsm", sheet_name="Data to DB")
    columns_pivot = ["Plan_Total", "Plan_Q1", "Plan_Q2", "Plan_Q3", "Plan_Q4", 
    "Actual_Total", "Actual_Q1", "Actual_Q2", "Actual_Q3", "Actual_Q4"]

    # แปลงเป็น long format และให้ col ที่ไม่ได้กล่าวยังคงอยู่ 
    long_format = pd.melt(df, id_vars=[col for col in df.columns if col not in columns_pivot],
                        value_vars=columns_pivot, 
                        var_name="Amount Name", #ให้ชื่อคอลัมน์มาอยู่ใน row of col นี้
                        value_name="Amount")  #ให้ value of each col มาอยู่ใน row of col นี้

    #แยกประเภท plan or actual
    long_format['Amount Type'] = long_format['Amount Name'].apply(lambda x: 'Plan' if 'Plan' in x else 'Actual')
    
    #แยกไตรมาสและเปลี่ยนรูปแบบการเขียน
    long_format['Amount Name'] = long_format['Amount Name'].apply(lambda x: x.replace('Plan_', 'Plan {Quarter }').replace('Actual_', 'Actual {Quarter }'))

    #เรียงลำดับ col
    column_real = ['Amount Type', 'Amount Name', 'Amount'] + [col for col in long_format.columns if col not in ['Amount Type', 'Amount Name', 'Amount']]
    df_completed = long_format[column_real]

    return df_completed