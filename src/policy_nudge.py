# -*- coding: utf-8 -*-
import ast
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import logging.handlers
from datetime import datetime, timedelta
import json
from query_dsl import mysql_qry
from elasticsearch import Elasticsearch
import pymysql

# commercial=PRD, development=STG
state = 'STG'

# charset
charset = 'UTF8'

# manual nudge type list
manual_nudge_type_list = ['break_time', 'assign_contents', 'text', 'search_keyword', 'view_text']

sql_qry = mysql_qry
day = int(sys.argv[1])

log_time = datetime.today() + timedelta(days=day)
log_time = log_time.strftime('%Y.%m.%d')

time = datetime.today()
index_time = time.strftime('%Y.%m.%d.%H%M%S')

# index name
index_name = f'index-nudge-policy-v532-'
#alias name
alias_name = 'index-nudge-policy-v532'

def make_log():
    log_name = "policy_log.log"

    log_handler = logging.handlers.TimedRotatingFileHandler(
        filename='logs/' + log_name,
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    logger = logging.getLogger(__file__)
    formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)

    return logger


log_txt = make_log()


# read server information
def read_config():
    if state == 'PRD' or state == 'STG':
        with open('config/v532_connect_info.json', 'r') as f:
            connect_info = json.load(f)
            connect_info = connect_info[state]
    else:
        # if the state is 'PRD' or 'STG', the function is stopped.
        print(f"WRONG STATE : {state}")
        quit()

    # return server information when calling a function
    return connect_info


# server information
info = read_config()


# connect Elastic Search
def connect_elastic():
    es_client = ''

    e_info = info['POLICY_ELASTIC']
    try:
        if state == 'PRD':
            es_client = Elasticsearch(
                e_info['POLICY'],
                port=e_info['PORT'],
                http_auth=(e_info['USER'],
                           e_info['PASSWORD'])
            )
        elif state == 'STG':
            es_client = Elasticsearch(
                e_info['POLICY'],
                port=e_info['PORT']
            )

        return es_client

    except Exception as err:
        log_txt.info(f'ELASTIC SEARCH CONNECTION ERROR : {err}')
        print(f'ELASTIC SEARCH CONNECTION ERROR : {err}')
        quit()


# connect Nudge Admin DB
def connect_mysql():
    m_info = info['nudge_admin_maria_db']
    conn = ''

    try:
        # connect mysql
        conn = pymysql.connect(
            host=m_info['HOST'],
            user=m_info['USER'],
            password=m_info['PASSWORD'],
            db=m_info['DB_NAME'],
            port=m_info['PORT'],
            charset=charset,
            cursorclass=pymysql.cursors.DictCursor
        )

        return conn

    except TimeoutError or RuntimeError as timeout:
        log_txt.info(f'CONNECT MY SQL TIME OUT ERROR : {timeout}')
        print(f'TIME OUT ERROR : {timeout}')
        conn.close()
        quit()
    except Exception as err:
        log_txt.info(f'CONNECT MY SQL ERROR : {err}')
        print(f'MYSQL ERROR : {err}')
        quit()


def get_admin_data():
    try:
        log_txt.info(f'SELECT_NUDGE_ADMIN_DB_START')
        # connect nudge admin db
        curs = connect_mysql().cursor()
        # get select sql query
        sql = sql_qry.query[state]['SELECT'].format(day=day)
        curs.execute(sql)

        rows = curs.fetchall()
        log_txt.info(f'SELECT_NUDGE_ADMIN_DB_COUNT : {len(rows)}건')
        log_txt.info(f'SELECT_NUDGE_ADMIN_DB_END')
        return rows

    except Exception as err:
        log_txt.info(f"GET_ADMIN_DATA_FUNCTION_ERROR : {err}")
        print(f"SQL_ERROR : {err}")
        quit()


def insert_history():
    conn = connect_mysql()

    curs = conn.cursor()

    delete_hist_sql = sql_qry.query[state]['DELETE_SLOT_HIST'].format(day=day)
    delete_seg_hist_sql = sql_qry.query[state]['DELETE_SLOT_SEG_HIST'].format(day=day)
    delete_model_hist_sql = sql_qry.query[state]['DELETE_SLOT_MODEL_HIST'].format(day=day)

    del_slot_hist_cnt = curs.execute(delete_hist_sql)
    del_seg_hist_cnt = curs.execute(delete_seg_hist_sql)
    del_model_hist_cnt = curs.execute(delete_model_hist_sql)
    log_txt.info(f'DELETE_SLOT_HIST_COUNT : {del_slot_hist_cnt}건')
    log_txt.info(f'DELETE_SLOT_SEG_HIST_COUNT : {del_seg_hist_cnt}건')
    log_txt.info(f'DELETE_SLOT_MODEL_HIST_COUNT : {del_model_hist_cnt}건')

    insert_hist_sql = sql_qry.query[state]['INSERT_SLOT_HIST'].format(day=day)
    insert_seg_hist_sql = sql_qry.query[state]['INSERT_SLOT_SEG_HIST'].format(day=day)
    insert_model_hist_sql = sql_qry.query[state]['INSERT_SLOT_MODEL_HIST'].format(day=day)

    insert_hist_cnt = curs.execute(insert_hist_sql)
    insert_seg_hist_cnt = curs.execute(insert_seg_hist_sql)
    insert_model_hist_cnt = curs.execute(insert_model_hist_sql)
    log_txt.info(f'INSERT_SLOT_HIST_COUNT : {insert_hist_cnt}건')
    log_txt.info(f'INSERT_SLOT_SEG_HIST_COUNT : {insert_seg_hist_cnt}건')
    log_txt.info(f'INSERT_SLOT_MODEL_HIST_COUNT : {insert_model_hist_cnt}건')

    conn.commit()
    conn.close()


def make_policy_index():
    log_txt.info(f'MAKE_POLICY_INDEX_START : {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')

    # connect elastic search
    es_client = connect_elastic()
    rows = get_admin_data()
    # body = sql_qry.query[state]['DELETE_ES_DSL']
    # body['query']['bool']['filter'].append({"term": {"log_time": log_time}})
    # del_res = es_client.delete_by_query(index=index_name, body=body)
    # log_txt.info(f'DELETE_{log_time}_DATA_COUNT : {del_res["deleted"]}건')

    list_insert_res = []

    for nudge_data in rows:

        # print(nudge_data)

        nudge_data['@timestamp'] = datetime.today()
        nudge_data['ext_info'] = json.loads(nudge_data['ext_info'])
        nudge_data['txt'] = ast.literal_eval(nudge_data['txt'])
        nudge_data['polling_time'] = '4'

        # nudge_data 의 key 를 추출
        key_list = list(dict(nudge_data).keys())
        # None 일 경우 '' 로 변환
        for key in key_list:
            if nudge_data[key] is None:
                nudge_data[key] = ''

        if nudge_data['menu_id'] != 'menu001':
            nudge_data.pop('display_epg_policy')

        if nudge_data['menu_id'] != 'menu009':
            nudge_data.pop('display_breaktime_policy')
            nudge_data.pop('display_specific_policy')

        if nudge_data['menu_id'] == 'menu001' or nudge_data['menu_id'] == 'menu009':
            nudge_data.pop('display_general_policy')

        list_insert_res.append(es_client.index(index=f'{index_name}{index_time}', body=nudge_data))

    # ALIAS CHANGE
    if es_client.indices.exists(index=f'{index_name}{index_time}'):
        es_client.indices.delete_alias(index=[f'{index_name}*'], name=alias_name)
        es_client.indices.put_alias(index=[f'{index_name}{index_time}'], name=alias_name)
    else:
        log_txt.info(f'ALIAS ERROR : CHECK ALIAS!')
        quit()

    log_txt.info(f'INSERT_POLICY_INDEX_COUNT : {len(list_insert_res)}건')
    es_client.indices.refresh(index=f'{index_name}{index_time}')
    log_txt.info(f'MAKE_POLICY_INDEX_END : {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')


if __name__ == '__main__':
    log_txt.info(f'START_POLICY_NUDGE : {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')
    log_txt.info(f'TARGET_DATE : {log_time}')
    try:
        make_policy_index()
        if day == 1:
            insert_history()
    except Exception as err:
        log_txt.info(f'ERROR : {err}')
    log_txt.info(f'END_POLICY_NUDGE : {datetime.today().strftime("%Y-%m-%d %H:%M:%S")}')
    log_txt.info(f'--------------------------------------------------------------------')
