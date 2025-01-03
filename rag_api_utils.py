"""
新建一个知识库
"""

import traceback

import requests
from loguru import logger

OPENAI_API_BASE_URL = "http://localhost:3000/api"
OPENAI_API_KEY = "fastgpt-z7KUTTKRmeAuIBbn4h9vpEK0Dtkge3wAVMbT4Yg8rq7P4CjdUSOIWT"

def create_dataset(dataset_name):
    """
    创建知识库
    :return:
    """
    create_dataset_url = rf'{OPENAI_API_BASE_URL}/core/dataset/create'
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {
        "type": "dataset",
        "name": dataset_name,
        "vectorModel": "text-embedding-3-large",
        "agentModel": "glm-4"
    }
    result = requests.post(url=create_dataset_url, json=data, headers=headers)
    print(result.json())


def query_dataset():
    """
    查询知识库
    :return:
    """
    dataset_dict = {}
    query_dataset_url = rf'{OPENAI_API_BASE_URL}/core/dataset/list'
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {OPENAI_API_KEY}"}
    try:
        result = requests.post(url=query_dataset_url, headers=headers, verify=False)
        ret = result.json()
        # print(ret)
        # 按照name-id解析知识库
        for data in ret.get('data', []):
            dataset_dict[data.get('name')] = {
                "id": data.get('_id'),
                "vectorModel": data.get('vectorModel')
            }
    except requests.exceptions.SSLError as e:
        print("SSL Error:", e)
    except Exception as e:
        logger.error(f"get token failed: {e}, {result} {result.json()}")
        traceback.print_exc()
    return dataset_dict


def create_text_collection(dataset_name, dataset, text):
    """
    创建纯文本集合
    :return:
    """
    text_collection_url = rf'{OPENAI_API_BASE_URL}/core/dataset/collection/create/text'
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {
        "text": text,
        "datasetId": dataset.get('id'),
        "name": dataset_name,
        "trainingType": "chunk"
    }
    result = requests.post(url=text_collection_url, json=data, headers=headers)
    # print(result.json())


if __name__ == '__main__':
    dataset_dict = query_dataset()
    dataset_name = r'舆情知识库实时'
    if dataset_name not in dataset_dict:
        create_dataset(dataset_name)
    print(dataset_dict[dataset_name])
    create_text_collection(dataset_name, dataset_dict[dataset_name], "67868768768787")
    # print(dataset_dict)
