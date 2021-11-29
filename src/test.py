# -*- coding:utf-8 -*-
import os
from datetime import datetime, timedelta
import datetime
import sys
import pymysql
import json
from elasticsearch import Elasticsearch, helpers
from logging.handlers import TimedRotatingFileHandler
import logging
import constants as const


logger = None
config = None
con_pymysql = None
cur_pymysql = None

nudge_date = (datetime.datetime.now()).strftime("%Y.%m.%d.%H%M%S")
SCRIPT_INDEX = 'index-nudge-stb-info'


def setLogger(logFileName):
    global logger
    # curPath = os.path.dirname(os.path.abspath(__file__))
    curPath = '/svc/nudge/collect'
    log_handler = logging.handlers.TimedRotatingFileHandler(curPath + '/logs/' + logFileName, when='midnight',
                                                            interval=1, backupCount=7, encoding='utf-8',
                                                            atTime=datetime.time(0, 0, 0, 0))

    logger = logging.getLogger(__file__)
    formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(logging.DEBUG)


def getConfig():
    # curPath = os.path.dirname(os.path.abspath(__file__))
    curPath = sys.argv[1]
    # with open(curPath + '/../config/config_kim.json', 'r') as f:
    with open(curPath, 'r') as f:
        logger = None
        config = json.load(f)
        if config['DEBUG']:
            config = config['DEV']
        else:
            config = config['PRD']

    return config


def connectMariaDB(dbConfig):
    global con_pymysql, cur_pymysql
    try:
        con_pymysql = pymysql.connect(**dbConfig)
    except Exception as e:
        logger.error('MariaDB DB connect error: {0}'.format(e))
        pass

    cur_pymysql = con_pymysql.cursor(pymysql.cursors.DictCursor)

    return con_pymysql, cur_pymysql


def connectEs(esConfig):
    global con_es
    try:
        con_es = Elasticsearch([esConfig['host_1'], esConfig['host_2'], esConfig['host_3'], esConfig['host_4'], esConfig['host_5']],
                               http_auth=(esConfig['user'], esConfig['password']), port=esConfig['port'], max_retries=500)

    except Exception as e:
        logger.error('Elasticsearch connect error: {0}'.format(e))
        pass

    return con_es


def index():

    try:

        # index가 이미 있는 경우 삭제 ( 장애발생을 대비해서 삭제여부 고려필요 )
        # if con_es.indices.exists(index=SCRIPT_INDEX):
        #     con_es.delete_by_query(index=SCRIPT_INDEX, body={'query': {'match_all': {}}})
        #     logger.info('{0} delete ..'.format(SCRIPT_INDEX))

        # 조회범위, 건수
        offset = 0
        count = 10000
        chk = 0

        while True:

            # 결과셋 저장
            list_temp = []
            list_results = []
            # stb 정보 조회쿼리
            sql = const.QRY_SELECT_STB_INFO % (offset, count)

            cur_pymysql.execute(sql)
            maria_rows = cur_pymysql.fetchmany(size=count)

            # 데이터가 없는 경우 중단
            if maria_rows == (): break

            # 조회한 데이터 가공
            for data in maria_rows:

                dict_data = {'stb_id': data['stb_id'], 'exclude_all_yn': data['exclude_all_yn'],
                             'service_day': nudge_date}

                if data['seg_ids'] is None:
                    dict_data['seg_ids'] = []
                else:
                    dict_data['seg_ids'] = str(data['seg_ids']).split(',')

                if data['exclude_menu_ids'] is None:
                    dict_data['exclude_menu_ids'] = []
                else:
                    dict_data['exclude_menu_ids'] = str(data['exclude_menu_ids']).split(',')

                if data['exclude_all_yn'] is None:
                    dict_data['exclude_all_yn'] = 'N'

                # logger.info('{0}'.format(dict_data))
                list_temp.append(dict_data)

            # bulk 생성
            for temp in list_temp:
                bulk_date = {"_index": SCRIPT_INDEX + '-{}'.format(nudge_date), "_id": temp['stb_id'], "_source": temp}
                list_results.append(bulk_date)
            # 색인
            helpers.bulk(con_es, list_results)

            # next
            offset = offset + count
            logger.info('[{}] : {} processing..'.format(chk, offset))
            print('[{}] : {} processing..'.format(chk, offset))
            chk = chk + 1

            # if chk == 2:
            #     break

    except Exception as err:
        logger.error('index() error : {0}'.format(err))
        pass


def alias():
    try:
        index_today = SCRIPT_INDEX+'-{}'.format(nudge_date)
        if con_es.indices.exists(index=index_today):
            con_es.indices.delete_alias(index=['{}'.format(SCRIPT_INDEX+'*')], name=SCRIPT_INDEX)
            con_es.indices.put_alias(index=['{}'.format(index_today)], name=SCRIPT_INDEX)
            print('index[{}] ==> alias[{}]'.format(index_today, SCRIPT_INDEX))
    except Exception as err:
        logger.error('alias() error : {0}'.format(err))
        pass


if __name__ == '__main__':
    setLogger("index_nudge_stb_info.log")
    config = getConfig()
    connectMariaDB(config["NUDGE_DB"])
    connectEs(config["NUDGE_ES"])

    index()
    if config["alias"]:
        alias()

    cur_pymysql.close()
    con_pymysql.close()
