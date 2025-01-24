from celery import Celery
from flask import Flask,jsonify
import time

app = Flask(__name__)
celery = Celery("tasks",broker = "redis://localhost",backend="redis://localhost")

celery.conf.task_routes = {
    "tasks.add" : {'queue' : 'add_queue'},
    "tasks.sub" : {'queue' : 'sub_queue'}
}

@celery.task()
def add(x,y):
    
    time.sleep(10)
    return (x+y)


@celery.task()
def sub(x,y):
    return (x+y)

@app.route("/add/<int:x>/<int:y>")
def trigger_add_task(x,y):
    
    for i in range(100):
        result = add.apply_async(args=(x,y),queue = "add_queue")
    return jsonify(message={'status':'Task Submitted to add_queue'})

@app.route("/sub/<int:x>/<int:y>")
def trigger_sub_task(x,y):
    task_id="custom_task_id_123"
    result = sub.apply_async(args=(x,y), task_id=task_id, queue = "sub_queue")
    return jsonify(message={"task_id":task_id,'status':'Task Submitted to sub_queue'})