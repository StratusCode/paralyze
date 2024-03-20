__all__ = (
    "Config",
    "GenericTask",
)


# https://cloud.google.com/monitoring/api/resources#tag_generic_task
class GenericTask:
    # GCP project ID where the task is running.
    project_id: str
    # Location where the task is running. This is a GCP region/zone.
    location: str
    # Namespace of the task. This is typically the application label
    namespace: str
    # Job name of the task. This is typically the service label.
    job: str
    # Task ID of the task. Contextual to the job.
    task_id: str


class Config:
    # GCP project ID that the metrics are sent to.
    project_id: str
    prefix: str

    task: GenericTask | None
