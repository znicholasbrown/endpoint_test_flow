# Prefect Modules
import prefect
from prefect import Flow, Task
from prefect.schedules import IntervalSchedule
from prefect import Parameter
from prefect.tasks.control_flow.conditional import switch
from prefect.environments.storage import Docker

# Utils
from urllib.error import HTTPError
from urllib import request
from datetime import timedelta, timezone, datetime


class PrintURL(Task):
    def run(self, url):
        self.logger.info(f"Testing the status of {url}")
        return


class GetCode(Task):
    def run(self, url):
        try:
            self.logger.info("Opening connection...")
            connection = request.urlopen(url)
            self.logger.info("Connection success.")
            code = connection.getcode()
        except HTTPError as e:
            code = e.code

        return code


class CheckStatusCode(Task):
    def run(self, code):
        self.logger.info("Status code check complete.")
        return "elevate" if code != 200 else None


class Elevate(Task):
    def run(self, code):
        self.logger.error(f"Status code {code}: endpoint is down!")
        raise Exception(f"Status code {code}: endpoint is down!")


schedule = IntervalSchedule(interval=timedelta(minutes=5))
with Flow("Endpoint Test", schedule=schedule) as flow:
    url = Parameter("url", default="//Some url//")

    p = PrintURL()(url=url)

    code = GetCode()(upstream_tasks=[p], url=url)

    check = CheckStatusCode()(code=code)

    switch(check, dict(elevate=Elevate()(code=code)))

flow.storage = Docker(
    base_image="python:3.8",
    python_dependencies=[],
    registry_url="//registry",
    image_name="endpoint_test",
    image_tag="endpoint_test",
)

flow.register(project_name="//project")
