# -*- coding: utf-8 -*-
"""
Schedules for br_ibge_pnadc
"""
from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants


every_three_months = Schedule(
    clocks=[
        CronClock(
            cron="0 0 1 */3 *",
            start_date=datetime(2021, 1, 1),
            labels=[constants.BASEDOSDADOS_DEV_AGENT_LABEL.value],
            parameter_defaults={
                "dataset_id": "br_ibge_pnadc",
                "materialization_mode": "dev",
                "materialize after dump": True,
                "table_id": "microdados",
            },
        )
    ]
)
