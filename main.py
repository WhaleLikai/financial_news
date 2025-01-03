# !usr/bin/env python
# -*- coding:utf-8 _*-
"""

"""

# 第一步获取id
# 第二步进入首页
# 第三步获取各部分数据及遍历更多页
import tqdm
import random
import string
import time
import traceback

import requests
from loguru import logger
from lxml import etree

from MySqlClient import MySqlClient
from rag_api_utils import query_dataset, create_text_collection, create_dataset
from utils import save_financial_news, query_financial_news_id_by_doc_id
from paddleocr import PaddleOCR, draw_ocr
import requests

# 初始化OCR模型
ocr = PaddleOCR(use_angle_cls=True,
                lang="ch",
                det_model_dir=r'D:\huishiwei\project_18\code\ch_PP-OCRv4_det_server_infer', # 文本检测模型
                rec_model_dir=r'D:\huishiwei\project_18\code\ch_PP-OCRv4_rec_server_infer', # 文本识别模型
                show_log=False)


def get_code():
    return ''.join(random.sample(string.ascii_letters + string.digits, 4))


def get_page(page_id, mysql_client, dataset_dict):
    try:
        url = f"https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&k=&num=50&page={page_id}"
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "sec-ch-ua":  "\"Microsoft Edge\";v=\"131\", \"Not=A;Brand\";v=\"24\", \"Chromium\";v=\"131\"",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        r = requests.get(url=url, headers=headers).json()
        # print(r)
        # exit()
        result = r.get('result', {})
        data = result.get('data', {})
        for passage in tqdm.tqdm(data):
            doc_id = passage.get('docid')
            if query_financial_news_id_by_doc_id(mysql_client, doc_id):
                logger.error(f"{doc_id}已经存在")
                continue
            images_url_list = passage.get('images')
            image_ocr_content = ''
            for image_url in images_url_list:
                try:
                    response = requests.get(image_url.get('u'))
                    # print(response)
                    # exit()

                    # 保存图片到本地
                    # save_path = url_path.split('/')[-1]  # 图片保存路径
                    with open('temp.jpg', 'wb') as f:  # 以二进制写入文件保存
                        f.write(response.content)
                    # exit()
                    result = ocr.ocr('temp.jpg', cls=True)
                    result1 = result[0]
                    if result1 is None:
                        continue
                    question_type_str = ''.join([i[1][0] for i in result1])
                    image_ocr_content += question_type_str
                except Exception as e:
                    logger.error(f"{e} ocr图片有误")
                    traceback.print_exc()
                logger.info(f"{image_ocr_content} {image_url}")
            
            published_time = passage.get('ctime')
            published_time = time.localtime(float(published_time))
            published_time = time.strftime('%Y-%m-%d %H:%M:%S', published_time)
            title = passage.get('title')
            url = passage.get('url')
            intro = passage.get('intro')
            media_name = passage.get('media_name')
            doc_detail = get_passage_detail(url) + image_ocr_content
            # print(url, doc_detail)
            # exit()
            financial_news_dict = {
                'published_time': published_time,
                'title': title,
                'url': url,
                'intro': intro,
                'media_name': media_name,
                'doc_id': doc_id,
                'detail': doc_detail
            }
            rag_dataset_str = save_financial_news(mysql_client, financial_news_dict)
            dataset_name = '财经新闻'
            text_collection_name = fr'{title}_{get_code()}'
            create_text_collection(text_collection_name, dataset_dict[dataset_name], rag_dataset_str)
    except Exception as e:
        logger.info(f"{e}_{page_id}_搜索报错")
        traceback.print_exc()


def get_passage_detail(passage_url):
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9",
        "Cookie": 'HWWAFSESID=32c2a20ba36cb639845; HWWAFSESTIME=1728807758347; csrfToken=LttcMC2OjNTnorc_fNwKH5pO; CUID=f702448020df2ede3c93c0326f3e2c09; TYCID=50fbff70893c11ef90f255142fb24550; sajssdk_2015_cross_new_user=1; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1728807762; HMACCOUNT=B575FA3CD7EC5323; bannerFlag=true; ssuid=2562439313; _ga=GA1.2.1103262685.1728808239; _gid=GA1.2.1392362671.1728808239; tyc-user-phone=%255B%252216583199793%2522%255D; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22286973644%22%2C%22first_id%22%3A%2219284f8f76a864-06610ff16439078-26001051-2073600-19284f8f76bb24%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTkyODRmOGY3NmE4NjQtMDY2MTBmZjE2NDM5MDc4LTI2MDAxMDUxLTIwNzM2MDAtMTkyODRmOGY3NmJiMjQiLCIkaWRlbnRpdHlfbG9naW5faWQiOiIyODY5NzM2NDQifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22286973644%22%7D%2C%22%24device_id%22%3A%2219284f8f76a864-06610ff16439078-26001051-2073600-19284f8f76bb24%22%7D; tyc-user-info=%7B%22state%22%3A%224%22%2C%22vipManager%22%3A%220%22%2C%22mobile%22%3A%2216624603516%22%2C%22userId%22%3A%22286973644%22%2C%22isExpired%22%3A%220%22%7D; tyc-user-info-save-time=1728815804468; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNjYyNDYwMzUxNiIsImlhdCI6MTcyODgxNTgwNSwiZXhwIjoxNzMxNDA3ODA1fQ.PjVJM1j0MboQQdNcxMBKA8XmDn5dhSCcYYZIqNe3tYTu8n5QEtcdf9XXahrkbSHal0XYiWpHV8Vc-KESiDJcbw; searchSessionId=1728815828.79417950; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1728815829',
        "sec-ch-ua":  "\"Microsoft Edge\";v=\"131\", \"Not=A;Brand\";v=\"24\", \"Chromium\";v=\"131\"",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    }
    ret = requests.get(passage_url, headers=headers, timeout=50)
    ret.encoding = 'UTF-8'
    with open(f'D:\huishiwei\project_18\code\html/list_{time.time()}.html', 'w', encoding='utf-8') as write_file:
        write_file.write(ret.text)
    # exit()
    #
    root_element = etree.HTML(ret.text)
    page_tools = root_element.xpath(
        "//div[@class='article-content clearfix']//div[@class='article']//p[not (@*)]/text()|//strong//*//text()|//p[@cms-style='font-L']/text()|//p//font//text()")[
                 1:]
    # print(page_tools)
    # exit()
    page_tools = list(filter(lambda x: x and x.strip(), page_tools))
    page_tools = [x.replace(u'\u3000', u' ').strip() for x in page_tools]
    # print(page_tools)
    detail_page = ''.join(page_tools)
    # print(detail_page)
    return detail_page


def main():
    mysql_client = MySqlClient(port=3306, user='root', password='123456',
                               database='briefing_test', charset='utf8mb4')
    
    # 创建知识库
    dataset_dict = query_dataset()
    dataset_name = fr'财经新闻'
    if dataset_name not in dataset_dict:
        create_dataset(dataset_name)
    
    dataset_dict = query_dataset()
    
    for i in tqdm.tqdm(range(51)):
        print(f"=========第{i + 1}页=========")
        get_page(i + 1, mysql_client, dataset_dict)
        time.sleep(15)


if __name__ == '__main__':
    main()
