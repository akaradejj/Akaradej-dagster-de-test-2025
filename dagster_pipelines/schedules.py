import dagster as dg

# 2.4.1 Set the cron_schedule for the job kpi_fy_monthly_job
kpi_fy_monthly_job_schedule = dg.ScheduleDefinition(
    name="kpi_fy_monthly_job",
    target=dg.define_asset_job(name="kpi_fy_monthly_job", selection=dg.AssetSelection.groups("plan")),
    cron_schedule=["0 0 3,21 * *"],  #รันตอน 00.00 ของวันที่ 3 และ 21 ของทุกเดือน ทุกปีgit add .

    default_status=dg.DefaultScheduleStatus.RUNNING,
)