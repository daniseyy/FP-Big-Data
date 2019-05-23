from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/<int:model>/<int:userId>/stars/top/<int:count>", methods=["GET"])
def top_stars(model, userId, count):
    logger.debug("User %s TOP stars requested", userId)
    top_rated = recommendation_engine.get_top_stars(model, userId, count)
    return json.dumps(top_rated)


@main.route("/<int:model>/business/<int:businessId>/recommend/<int:count>", methods=["GET"])
def business_recommending(model, businessId, count):
    logger.debug("BusinessId %s TOP user recommending", businessId)
    top_rated = recommendation_engine.get_top_business_recommend(model, businessId, count)
    return json.dumps(top_rated)


@main.route("/<int:model>/<int:userId>/stars/<int:businessId>", methods=["GET"])
def business_stars(model, userId, businessId):
    logger.debug("User %s rating requested for business %s", userId, businessId)
    stars = recommendation_engine.get_stars_for_movie_ids(model, userId, businessId)
    return json.dumps(stars)




def create_app(spark_session, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
