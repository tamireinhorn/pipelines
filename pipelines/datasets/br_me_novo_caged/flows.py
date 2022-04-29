"""
Flows for br_me_novo_caged
"""
from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.shell import ShellTask

from pipelines.constants import constants
from pipelines.datasets.br_me_novo_caged.tasks import build_partitions
from pipelines.utils.tasks import create_table_and_upload_to_gcs
from pipelines.datasets.br_me_novo_caged.schedules import every_month

download = ShellTask(
    command="bash bash_scripts/download.sh cagedmov", stream_output=True
)

install_sevenz = ShellTask(
    command="apt-get install p7zip-full", stream_output=True
)

# pylint: disable=C0103
with Flow("br_me_novo_caged.microdados_mov") as cagedmov:
    dataset_id = "br_me_novo_caged"
    table_id = "microdados_mov"

    installation=install_sevenz()

    get_data=download(upstream_tasks=[installation])

    # pylint: disable=E1123
    filepath = build_partitions(group="cagedmov", upstream_tasks=[get_data])

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="append",
        wait=filepath,
    )

cagedmov.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cagedmov.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
cagedmov.schedule = every_month
