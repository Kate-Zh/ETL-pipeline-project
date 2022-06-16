from datetime import datetime, timedelta
import pandas as pd
import requests
import pandahouse as ph
from pandahouse.core import to_clickhouse, read_clickhouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection = {
    'host': 'https://clickhouse.',
    'password': '',
    'user': '',
    'database': 'prod'}


 # Дефолтные параметры
default_args = {
    'owner': 'e-zhuzhman-17',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 17),
}

connect = {
    'host': 'https://clickhouse.',
    'password': '',
    'user': '',
    'database': 'test'}


# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_zhuzhman():
    
    @task
    # формируем датафрейм из ленты новостей
    def extract_feed():
        query_feed = '''SELECT toDate(time) as event_date, 
                                user_id,
                                gender,
                                age,
                                os, 
                                countIf(action='like')  as likes,
                                countIf(action='view') as views
                            FROM {db}.feed_actions
                            WHERE event_date = today() - 1
                            GROUP BY event_date,
                                    user_id, 
                                    gender,
                                    age,
                                    os
                             '''
        df_feed = ph.read_clickhouse(query=query_feed, connection = connection)
        return df_feed
    
    @task
    # формируем датафрейм по сообщениям
    def extract_msg():
        query_msg = '''SELECT event_date,
                                user_id,
                                messages_sent,
                                users_sent,
                                messages_received,
                                users_received
                    FROM
                        (SELECT toDate(time) as event_date,  
                                user_id,
                                count(*) as messages_sent,
                                count(distinct reciever_id) as users_sent
                            FROM {db}.message_actions
                            WHERE event_date = today() - 1
                            GROUP BY user_id, event_date) t1
                        FULL OUTER JOIN
                        (SELECT toDate(time) as event_date, reciever_id, 
                                count(*) as messages_received,
                                count(distinct user_id) as users_received
                            FROM {db}.message_actions
                            WHERE event_date = today() - 1
                            GROUP BY reciever_id, event_date) t2
                            ON t1.user_id = t2.reciever_id'''
        
        df_msg = ph.read_clickhouse(query=query_msg, connection = connection)
        return df_msg
    
    @task
    # объединение двух датафреймов
    def merge_df(df_feed, df_msg): 
        msg_and_feed = df_feed.merge(df_msg, on=['event_date', 'user_id'] , how='outer')
        return msg_and_feed
    
    @task
    # агрегируем данные по полу
    def transform_gender(msg_and_feed):
        
        # заменим кодировку на описание пола 
        def gender_category(x):
            if x == 1.0:
                return 'male'
            elif x == 0.0: 
                return 'female'
            else:
                return 'unspecified'

        msg_and_feed['gender'] = msg_and_feed['gender'].apply(gender_category)

        df_gender = msg_and_feed.groupby('gender').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_received':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()

        df_gender['metric'] = 'gender'
        df_gender.rename(columns={'gender':'metric_value'},inplace=True)
        return df_gender
    
    @task
    # агрегируем данные по возрасту
    def transform_age(msg_and_feed): 

        msg_and_feed['age_group'] = pd.cut(msg_and_feed['age'], bins=[0, 20, 30, 40, 50, 99], labels = ['0-20', '21-30', '31-40', '41-50', '50+'])

        df_age = msg_and_feed.groupby('age_group').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_received':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()

        df_age['metric'] = 'age_group'
        df_age.rename(columns={'age_group':'metric_value'},inplace=True)
        return df_age

    @task
    # агрегируем данные по платформе
    def transform_os(msg_and_feed):
        df_os = msg_and_feed.groupby('os').agg({'event_date':'min', \
                                    'likes':'sum', \
                                    'views': 'sum', \
                                    'messages_received':'sum', \
                                    'users_received':'sum', \
                                    'messages_sent':'sum', \
                                    'users_sent':'sum'}).reset_index().copy()
        df_os['metric'] = 'os'
        df_os.rename(columns={'os':'metric_value'}, inplace=True)
        return df_os
        

    
    @task
    # соединяем полученные в ходе агрегаций датафреймы
    def df_concat(df_gender, df_age, df_os):
        final_table = pd.concat([df_gender, df_age, df_os])
        final_table = final_table.reindex(columns = ['event_date', 'metric', 'metric_value', 'views', 'likes',
                                             'messages_received', 'messages_sent', 'users_received', 'users_sent'])
        final_table = final_table.reset_index().drop('index', axis =1)
        final_table['event_date'] = final_table['event_date'].apply(lambda x: datetime.isoformat(x))
        final_table = final_table.astype({
                        'metric':'str',
                        'metric_value':'str',  
                       'views':'int', \
                       'likes':'int', \
                       'messages_received':'int', \
                       'messages_sent':'int', \
                       'users_received':'int', \
                       'users_sent':'int'})
        return final_table
    
    @task
    def load(final_table):
        ph.to_clickhouse(df=final_table, table='ezhuzhman', index=False, connection = connect)


    feed = extract_feed()
    msg = extract_msg()
    feed_msg = merge_df(feed, msg)
    gender = transform_gender(feed_msg)
    age = transform_age(feed_msg)
    os = transform_os(feed_msg)
    final_table = df_concat(gender, age, os)
    load(final_table)
    
dag_zhuzhman = dag_zhuzhman()


    

        
    
    
    
