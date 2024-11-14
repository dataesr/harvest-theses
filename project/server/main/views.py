import redis
import datetime
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, request, current_app

#from project.server.main.tasks import create_task_harvest
from project.server.main.referentiel import harvest_and_save_idref
from project.server.main.utils_swift import upload_object, get_last_ref_date
from project.server.main.parse import parse_theses, get_idref_from_OS
from project.server.main.feed import harvest_and_insert_year

main_blueprint = Blueprint("main", __name__,)
from project.server.main.logger import get_logger

logger = get_logger(__name__)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")

@main_blueprint.route("/harvest", methods=["POST"])
def run_task_download():
    args = request.get_json(force=True)
    collection_name = args.get('collection_name')
    harvest_referentiel = args.get('harvest_referentiel', True)
    # 1. save aurehal structures
    if harvest_referentiel:
        harvest_and_save_idref(collection_name)
    try:
        referentiel = get_idref_from_OS(collection_name)
        #idref api too slow
    except:
        last_ref_date = get_last_ref_date()
        logger.debug(f'using last referentiel date : {last_ref_date}')
        referentiel = get_idref_from_OS(last_ref_date)
    
    year_start = args.get('year_start')
    year_end = args.get('year_end')
    if year_start is None:
        year_start = 1990
    if year_end is None:
        year_end = datetime.date.today().year
    for year in range(year_start, year_end + 1):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("harvest-theses", default_timeout=2160000)
            task = q.enqueue(harvest_and_insert_year,collection_name, year, year, referentiel)
            response_object = {
                "status": "success",
                "data": {
                    "task_id": task.get_id()
                }
            }
    return jsonify(response_object), 202

@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-theses")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)
