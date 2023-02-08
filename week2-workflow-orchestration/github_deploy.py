from prefect.deployments import Deployment
from prefect.filesystems import GitHub


github_block = GitHub.load("hw2-github-storage-block")

github_block.get_directory("hw2_github_storage_block")

from hw2_github_storage_block.etl_web_to_gcs_github import etl_parent_flow

github_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow",
    entrypoint="hw2_github_storage_block/etl_web_to_gcs_github.py:etl_parent_flow"
)

if __name__ == "__main__":
    github_dep.apply()
