import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect = True)

Measurement = Base.classes.measurement
Station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    
    session = Session(engine)
    year_query = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= '2016-08-23')
    session.close()

    # Convert to pandas DataFrame and set index to date, then: sort, group, sum, and round
    year_df = pd.read_sql(year_query.statement, engine)
    year_df.set_index('date', inplace = True)
    year_df.sort_values(by = "date", inplace = True)
    year_df_dsum = year_df.groupby("date").sum()
    year_df_dsum = year_df_dsum["prcp"].round(2).to_frame()
    
    # Convert to dictionary
    year_dict = year_df_dsum.to_dict()

    return jsonify(year_dict)

@app.route("/api/v1.0/stations")
def stations():
    
    session = Session(engine)
    stations = session.query(Station.station).all()
    session.close()
    
    stations_dict = [item for t in stations for item in t]

    return jsonify(stations_dict)

@app.route("/api/v1.0/tobs")
def tobs():

    session = Session(engine)
    station_activity = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    station_id = station_activity[0][0]
    temps_last_year = session.query(Measurement.tobs).filter(Measurement.station == station_id).filter(Measurement.date >= '2016-08-23').all()
    session.close()
    
    temps_list = [item for t in temps_last_year for item in t]

    return jsonify(temps_list)


@app.route("/api/v1.0/<start>")
def start_temp(start):

    session = Session(engine)
    station_activity = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    station_id = station_activity[0][0]
    temp_min = session.query(func.min(Measurement.tobs)).filter(Measurement.station == station_id).filter(Measurement.date >= start).first()[0]
    temp_max = session.query(func.max(Measurement.tobs)).filter(Measurement.station == station_id).filter(Measurement.date >= start).first()[0]
    temp_avg = session.query(func.avg(Measurement.tobs)).filter(Measurement.station == station_id).filter(Measurement.date >= start).first()[0]
    session.close()

    start_temp_list = [{"TMIN":temp_min, "TMAX":temp_max, "TAVG":temp_avg}]

    return jsonify(start_temp_list)

@app.route("/api/v1.0/<start>/<end>")
def start_end_temp(start, end):

    session = Session(engine)
    station_activity = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    station_id = station_activity[0][0]
    temp_min = session.query(func.min(Measurement.tobs)).filter(Measurement.station == station_id).filter(Measurement.date >= start).filter(Measurement.date <= end).first()[0]
    temp_max = session.query(func.max(Measurement.tobs)).filter(Measurement.station == station_id).filter(Measurement.date >= start).filter(Measurement.date <= end).first()[0]
    temp_avg = session.query(func.avg(Measurement.tobs)).filter(Measurement.station == station_id).filter(Measurement.date >= start).filter(Measurement.date <= end).first()[0]
    session.close()

    start_end_temp_list = [{"TMIN":temp_min, "TMAX":temp_max, "TAVG":temp_avg}]

    return jsonify(start_end_temp_list)


if __name__ == '__main__':
    app.run(debug=True)



