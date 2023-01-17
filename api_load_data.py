from flask import Flask, request, Response, jsonify, make_response
from sqlalchemy import create_engine
import pandas as pd
import logging
import sys

FORMAT='%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(level=logging.INFO,
                    format=FORMAT,
                    handlers=[
                      logging.FileHandler("app_load_api.log",mode="a"),
                      logging.StreamHandler(sys.stdout)
                    ])


if __name__ == "__main__":
    v_max_rows = 2
    app = Flask(__name__)
    
    @app.route('/load_data', methods=['POST'])
    def load_data():
        request_data = request.get_json()
        v_table = request_data['table']
        logging.info("Procesando {}".format(v_table))
        v_data = request_data['data']
        if(len(v_data)) > v_max_rows:
            logging.error("No se pueden cargar mas de {} registros".format(v_max_rows))
            v_status = 400
            return make_response(jsonify({"mesagge_error": "No se pueden cargar mas de {} registros".format(v_max_rows)}),v_status)
        df = pd.DataFrame(v_data)
        if v_table == "jobs":
            df_not_null = df[df.id.notnull() & df.job.notnull()]
            df_null = df[df.id.isnull() | df.job.isnull()]
        elif v_table == "departments":
            df_not_null = df[df.id.notnull() & df.department.notnull()]
            df_null = df[df.id.isnull() | df.department.isnull()]
        elif v_table == "hired_employees":
            df_not_null = df[df.id.notnull() & df.name.notnull() & df.datetime.notnull() &
                             df.department_id.notnull() & df.job_id.notnull()]
            df_null = df[df.id.isnull() | df.name.isnull() | df.datetime.isnull() |
                         df.department_id.isnull() | df.job_id.isnull()]
            

        engine = create_engine("mysql+pymysql://root:Admin123@localhost:3306/globant")
        try:
            df_not_null.to_sql(v_table, con=engine, if_exists='append', index=False)
        except Exception as e:
            logging.error("Error al insertar en base de datos: {}".format(e))
            v_status = 500
            return make_response(jsonify({"mesagge_error": "Error al insertar en base de datos, verifique el log para mas detalles"}),v_status)
        
        logging.warning("No se cargaron los siguientes registros\n{}".format(df_null))
        logging.info("Se cargaron {} registros en la tabla {}".format(len(df_not_null),v_table))

        v_status = 200
        return make_response(jsonify({"message_ok": "Se cargaron {} registros en la tabla {}".format(len(df_not_null),v_table),
                                      "message_warning": "No se cargaron loas siguientes registros\n{}".format(df_null)}),v_status)

    app.run(host='0.0.0.0', port=4000)