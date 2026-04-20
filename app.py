"""
Dashboard Ramos Peru - Zyra
Panel de control y analisis financiero
Usa tabla DashbordLk (datos pre-calculados en USD)
"""

import os
import json
import traceback
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, request, jsonify, send_from_directory

import pymysql
import pymysql.cursors

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, 'static')

app = Flask(__name__, static_folder=STATIC_DIR)

# ─── Configuracion de Base de Datos ───
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', '34.125.156.253'),
    'port': int(os.environ.get('DB_PORT', 3306)),
    'user': os.environ.get('DB_USER', 'mantenedor'),
    'password': os.environ.get('DB_PASSWORD', 'DDI!dev%2024'),
    'database': os.environ.get('DB_NAME', 'zeruk'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'connect_timeout': 10,
    'read_timeout': 30,
}

# ─── Tabla principal ───
T_DASH = 'DashbordLk'
T_DET = 'RamosPeruCuota'
T_CAB = 'RamosPeru'

# ─── Columnas de DashbordLk ───
COL_FEE = "d.DashbordLkFeeNetoUSD"
COL_MC_ZYRA = "d.DashbordLkMCZyraUSD"
COL_MC_PROD = "d.DashbordLkMCProducerUSD"
COL_PRIMA = "d.DashbordLkPrimaNetaUSD"
COL_FECHA = "d.DashbordLkInicioVigencia"
COL_RAZON = "d.DashbordLkRazonSocial"
COL_RAMO = "d.DashbordLkRamo"
COL_PRODUCER = "d.DashbordLkProducer"
COL_ASEG = "d.DashbordLkAseguradora"
COL_EJEC = "d.DashbordLkEjecutivo"
COL_ESTADO = "d.DashbordLkEstadoPago"


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def get_db():
    return pymysql.connect(**DB_CONFIG)


def json_response(data, status=200):
    return app.response_class(
        response=json.dumps(data, cls=CustomEncoder, ensure_ascii=False),
        status=status,
        mimetype='application/json'
    )


# ─── Rutas ───

@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'index.html')


@app.route('/api/test')
def api_test():
    """Diagnostico completo."""
    result = {'status': 'ok', 'steps': []}
    try:
        conn = get_db()
        result['steps'].append('Conexion exitosa!')

        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cur.fetchall()]
            result['total_tables'] = len(tables)

            # DashbordLk
            cur.execute(f"DESCRIBE `{T_DASH}`")
            cols = [r['Field'] for r in cur.fetchall()]
            result['dashbordlk_columns'] = cols

            cur.execute(f"SELECT COUNT(*) AS total FROM `{T_DASH}`")
            result['dashbordlk_count'] = cur.fetchone()['total']

            cur.execute(f"SELECT * FROM `{T_DASH}` LIMIT 1")
            sample = cur.fetchone()
            if sample:
                result['sample_row'] = {k: str(v)[:100] for k, v in sample.items()}

        conn.close()

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        result['traceback'] = traceback.format_exc()

    return json_response(result)


@app.route('/api/describe/<table_name>')
def api_describe_table(table_name):
    """Describe la estructura de una tabla."""
    result = {'table': table_name}
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cur.fetchall()]
            if table_name not in tables:
                return json_response({'error': f'Tabla {table_name} no existe', 'tables': tables}, 404)

            cur.execute(f"DESCRIBE `{table_name}`")
            result['columns'] = cur.fetchall()

            cur.execute(f"SELECT COUNT(*) AS total FROM `{table_name}`")
            result['count'] = cur.fetchone()['total']

            cur.execute(f"SELECT * FROM `{table_name}` LIMIT 3")
            rows = cur.fetchall()
            result['sample'] = [{k: str(v)[:120] for k, v in r.items()} for r in rows]

        conn.close()
    except Exception as e:
        result['error'] = str(e)
        result['traceback'] = traceback.format_exc()
    return json_response(result)


@app.route('/api/filters')
def api_filters():
    try:
        conn = get_db()
        result = {}

        try:
            with conn.cursor() as cur:
                filters_map = {
                    'producer': COL_PRODUCER,
                    'razon_social': COL_RAZON,
                    'aseguradora': COL_ASEG,
                    'ejecutivo': COL_EJEC,
                    'estado_pago': COL_ESTADO,
                }
                for key, col in filters_map.items():
                    try:
                        cur.execute(f"""
                            SELECT DISTINCT {col} AS val
                            FROM `{T_DASH}` d
                            WHERE {col} IS NOT NULL AND {col} != ''
                            ORDER BY val
                        """)
                        result[key] = [str(row['val']) for row in cur.fetchall()]
                    except Exception as e:
                        result[key] = []
                        result[f'{key}_error'] = str(e)
        finally:
            conn.close()

        return json_response(result)

    except Exception as e:
        return json_response({'error': str(e), 'traceback': traceback.format_exc()}, 500)


@app.route('/api/dashboard')
def api_dashboard():
    try:
        conn = get_db()

        # ─── Filtros ───
        conditions = []
        params = []

        filters_map = {
            'producer': COL_PRODUCER,
            'razon_social': COL_RAZON,
            'aseguradora': COL_ASEG,
            'ejecutivo': COL_EJEC,
            'estado_pago': COL_ESTADO,
        }

        for key, col in filters_map.items():
            val = request.args.get(key, '').strip()
            if val:
                values = val.split('||')
                placeholders = ','.join(['%s'] * len(values))
                conditions.append(f"{col} IN ({placeholders})")
                params.extend(values)

        inicio_desde = request.args.get('inicio_desde', '').strip()
        inicio_hasta = request.args.get('inicio_hasta', '').strip()
        if inicio_desde:
            conditions.append(f"{COL_FECHA} >= %s")
            params.append(inicio_desde)
        if inicio_hasta:
            conditions.append(f"{COL_FECHA} <= %s")
            params.append(inicio_hasta)

        where = " AND ".join(conditions) if conditions else "1=1"
        base = f"FROM `{T_DASH}` d"

        try:
            with conn.cursor() as cur:
                # ─── KPIs ───
                cur.execute(f"""
                    SELECT
                        COALESCE(SUM({COL_FEE}), 0) AS total_fee_neto,
                        COALESCE(SUM({COL_MC_PROD}), 0) AS total_mc_producer,
                        COALESCE(SUM({COL_MC_ZYRA}), 0) AS total_mc_zyra,
                        COALESCE(SUM({COL_PRIMA}), 0) AS total_prima_neta,
                        COUNT(*) AS total_polizas
                    {base}
                    WHERE {where}
                """, params)
                kpis = cur.fetchone()

                # ─── Top Contratantes por Fee Neto ───
                cur.execute(f"""
                    SELECT COALESCE({COL_RAZON}, 'N/A') AS name,
                           COALESCE(SUM({COL_FEE}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY {COL_RAZON}
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_contratantes_fee = [dict(r) for r in cur.fetchall()]

                # ─── Top Contratantes por Comision Producer ───
                cur.execute(f"""
                    SELECT COALESCE({COL_RAZON}, 'N/A') AS name,
                           COALESCE(SUM({COL_MC_PROD}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY {COL_RAZON}
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_contratantes_mc = [dict(r) for r in cur.fetchall()]

                # ─── Top Ramos por Prima Neta ───
                cur.execute(f"""
                    SELECT COALESCE({COL_RAMO}, 'N/A') AS name,
                           COALESCE(SUM({COL_PRIMA}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY {COL_RAMO}
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_ramos_prima = [dict(r) for r in cur.fetchall()]

                # ─── Top Ramos por Comision Zyra ───
                cur.execute(f"""
                    SELECT COALESCE({COL_RAMO}, 'N/A') AS name,
                           COALESCE(SUM({COL_MC_ZYRA}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY {COL_RAMO}
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_ramos_mc_zyra = [dict(r) for r in cur.fetchall()]

                # ─── Estado de Pago ───
                cur.execute(f"""
                    SELECT COALESCE({COL_ESTADO}, 'N/A') AS estado,
                           COUNT(*) AS count,
                           COALESCE(SUM({COL_PRIMA}), 0) AS prima_neta,
                           COALESCE(SUM({COL_FEE}), 0) AS fee_neto
                    {base}
                    WHERE {where}
                    GROUP BY {COL_ESTADO}
                    ORDER BY prima_neta DESC
                """, params)
                estado_pago = [dict(r) for r in cur.fetchall()]

                # ─── Top Producers ───
                cur.execute(f"""
                    SELECT COALESCE({COL_PRODUCER}, 'N/A') AS producer,
                           COALESCE(SUM({COL_FEE}), 0) AS fee_neto,
                           COALESCE(SUM({COL_MC_PROD}), 0) AS mc_producer,
                           COALESCE(SUM({COL_PRIMA}), 0) AS prima_neta,
                           COUNT(*) AS count
                    {base}
                    WHERE {where}
                    GROUP BY {COL_PRODUCER}
                    ORDER BY fee_neto DESC
                    LIMIT 10
                """, params)
                top_producers = [dict(r) for r in cur.fetchall()]

                # ─── Timeline ───
                cur.execute(f"""
                    SELECT DATE_FORMAT({COL_FECHA}, '%%Y-%%m') AS month,
                           COALESCE(SUM({COL_PRIMA}), 0) AS prima_neta,
                           COALESCE(SUM({COL_FEE}), 0) AS fee_neto,
                           COALESCE(SUM({COL_MC_ZYRA}), 0) AS mc_zyra,
                           COUNT(*) AS count
                    {base}
                    WHERE {where}
                    GROUP BY DATE_FORMAT({COL_FECHA}, '%%Y-%%m')
                    ORDER BY month
                """, params)
                timeline = [dict(r) for r in cur.fetchall()]

                # ─── Cuotas (RamosPeruCuota) ───
                cuotas_summary = {}
                try:
                    cur.execute(f"""
                        SELECT
                            COUNT(*) AS total_cuotas,
                            COALESCE(SUM(q.RamoPeMonto), 0) AS monto_total,
                            SUM(CASE WHEN q.RamoPeEstadoPago LIKE '%%Pagad%%' THEN 1 ELSE 0 END) AS cuotas_pagadas,
                            SUM(CASE WHEN q.RamoPeEstadoPago LIKE '%%Pagad%%' THEN q.RamoPeMonto ELSE 0 END) AS monto_pagado
                        FROM `{T_DET}` q
                    """)
                    cuotas_summary = cur.fetchone() or {}
                except:
                    cuotas_summary = {}

                # Total records
                cur.execute(f"SELECT COUNT(*) AS total {base} WHERE {where}", params)
                total_records = cur.fetchone()['total']

        finally:
            conn.close()

        response = {
            'kpis': {k: float(v) if isinstance(v, Decimal) else v for k, v in kpis.items()},
            'top_contratantes_fee': top_contratantes_fee,
            'top_contratantes_mc_producer': top_contratantes_mc,
            'top_ramos_prima': top_ramos_prima,
            'top_ramos_mc_zyra': top_ramos_mc_zyra,
            'estado_pago': estado_pago,
            'top_producers': top_producers,
            'timeline': timeline,
            'cuotas_summary': {k: float(v) if isinstance(v, Decimal) else v for k, v in cuotas_summary.items()} if isinstance(cuotas_summary, dict) else {},
            'total_records': total_records
        }

        return json_response(response)

    except Exception as e:
        return json_response({
            'error': str(e),
            'traceback': traceback.format_exc()
        }, 500)


@app.route('/api/logo')
def api_logo():
    try:
        return send_from_directory(STATIC_DIR, 'logo.svg', mimetype='image/svg+xml')
    except:
        return '', 404


@app.route('/api/logo-white')
def api_logo_white():
    try:
        return send_from_directory(STATIC_DIR, 'logo-white.svg', mimetype='image/svg+xml')
    except:
        return '', 404


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    print(f"\n{'='*60}")
    print(f"  Dashboard Ramos Peru - Zyra")
    print(f"  http://localhost:{port}")
    print(f"  Diagnostico: http://localhost:{port}/api/test")
    print(f"{'='*60}\n")
    app.run(host='0.0.0.0', port=port, debug=debug)
