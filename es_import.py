import time

from elasticsearch import helpers, Elasticsearch
from elasticsearch.helpers import streaming_bulk

import yaml


es = Elasticsearch([''], http_auth=('', ''), use_ssl=True, verify_certs=False)


def generate_projects():
    with open("./data.yml") as f:
        x = yaml.load(f, Loader=yaml.FullLoader)
        users = x.get("users")
        for user in users:
            repos = user.get('repos', [])
            for repo in repos:
                gitee_user = user.get('gitee_id', 'Unknow')
                github_user = user.get('github_id', 'Unknow')
                doc = {
                    "name": user.get('name', 'Unknow'),
                    "user": gitee_user if 'gitee.com' in repo else github_user,
                    "repo": repo,
                    "created_at": time.strftime("%Y-%m-%dT%H:00:00+0800")
                }
                yield doc


def _main():
    # Cleanup es index
    es.indices.delete(index='whitebox_projects')

    actions = streaming_bulk(
        client=es, index="whitebox_projects", actions=generate_projects()
    )
    for ok, action in actions:
        if not ok:
            print("Failed to insert doc...")
    print("Load complete.")


if __name__ == "__main__":
    _main()
