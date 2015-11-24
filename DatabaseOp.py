import MySQLdb
import datetime
from celery import Celery
from db_op import DB
app = Celery('mtn_tasks', broker='amqp://guest@localhost//')

@app.task
def update_subscribe(subscriber, service_id, subscribed, status, shortcode):
    db = DB()
    cursor = db.query('''select idservices,mtn_id,name_slug,db_name from mtn_services.services where mtn_id=%s;''' % service_id)
    result = cursor.fetchone()
    str_sql = "update %s.mtn_service set is_enable=%s, date_modify=NOw() where subscriber = '%s';" % (result[2], subscribed, subscriber)
    update_selected_table.delay(result[2], str_sql)
    db.commit()


@app.task()
def update_selected_table(db_name, string_sql):
    db = DB()
    result = db.query(string_sql)
    write_to_file.delay(db_name, string_sql, result)
    db.commit()


@app.task
def write_to_file(subscriber, query, result):
    f = open('Log/myfile.out', 'a')
    f.write('update DataBase with name: %s Successfull:{%s} at :%s and result:%s\n' % (
        subscriber, query, datetime.datetime.now(), result))
    f.close()


