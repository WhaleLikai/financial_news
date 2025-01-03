import time
from loguru import logger


def save_financial_news(mysql_client, financial_news_dict):
    """

    :param mysql_client:
    :param financial_news_dict:
    :return:
    """
    financial_news_sql = (f"insert into briefing_test.financial_news(docid, title,url,intro,"
                          f"detail, media_name,published_time) "
                          f"values(%s,%s,%s,%s,%s,%s,%s)")
    financial_news_id = mysql_client.insert(financial_news_sql,
                                            (financial_news_dict.get('doc_id'), financial_news_dict.get('title'),
                                             financial_news_dict.get('url'), financial_news_dict.get('intro'),
                                             financial_news_dict.get('detail'), financial_news_dict.get('media_name'),
                                             financial_news_dict.get('published_time')))
    published_time = time.strptime(financial_news_dict.get('published_time'), '%Y-%m-%d %H:%M:%S')
    if financial_news_id is None:
        logger.error(f"插入财经新闻失败：{financial_news_dict}")
        return None

    return f"在{published_time.tm_year}年{published_time.tm_mon}月{published_time.tm_mday}日，" \
           f"发布了如下新闻：{financial_news_dict.get('detail')}"


def query_financial_news_id_by_doc_id(mysql_client, doc_id):
    sql = f"select * from briefing_test.financial_news fn where docid=%s"
    _, result_all = mysql_client.select_many(sql, (doc_id,))
    
    if len(result_all) == 0:
        return None
    else:
        return result_all[0][0]
