"""
Dashboard Ramos Peru - Zyra
Panel de control y analisis financiero
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

# ─── Nombres reales de tablas ───
T_CAB = 'RamosPeru'
T_DET = 'RamosPeruCuota'
T_ASEG = 'Aseguradora'
T_RAMO = 'Ramo'
T_PROD = 'Producer'
T_EJEC = 'Ejecutivo'
T_CLI = 'RamosPeruAsegurado'  # tabla de asegurados/clientes
T_EMP = 'Empresa'


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


# ─── Descubrir nombres de tablas de lookup ───
_lookup_cache = {}


def discover_lookups():
    """Descubre las tablas de lookup y sus columnas de nombre."""
    if _lookup_cache:
        return _lookup_cache

    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Ver que tablas existen
            cur.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cur.fetchall()]

            # Aseguradora
            if T_ASEG in tables:
                cur.execute(f"DESCRIBE `{T_ASEG}`")
                cols = [r['Field'] for r in cur.fetchall()]
                _lookup_cache['aseg_cols'] = cols
                # Buscar columna de nombre
                for c in cols:
                    if 'nombre' in c.lower() or 'name' in c.lower() or 'descripcion' in c.lower():
                        _lookup_cache['aseg_name'] = c
                        break
                # Buscar columna ID
                for c in cols:
                    if c.lower() == 'aseguradoraid' or c.lower() == 'id':
                        _lookup_cache['aseg_id'] = c
                        break

            # Ramo
            if T_RAMO in tables:
                cur.execute(f"DESCRIBE `{T_RAMO}`")
                cols = [r['Field'] for r in cur.fetchall()]
                _lookup_cache['ramo_cols'] = cols
                for c in cols:
                    if 'nombre' in c.lower() or 'name' in c.lower() or 'descripcion' in c.lower():
                        _lookup_cache['ramo_name'] = c
                        break
                for c in cols:
                    if c.lower() in ('ramoid', 'ramosid', 'id'):
                        _lookup_cache['ramo_id'] = c
                        break

            # Producer
            if T_PROD in tables:
                cur.execute(f"DESCRIBE `{T_PROD}`")
                cols = [r['Field'] for r in cur.fetchall()]
                _lookup_cache['prod_cols'] = cols
                for c in cols:
                    if 'nombre' in c.lower() or 'name' in c.lower():
                        _lookup_cache['prod_name'] = c
                        break
                for c in cols:
                    if c.lower() in ('producerid', 'id'):
                        _lookup_cache['prod_id'] = c
                        break

            # Ejecutivo
            if T_EJEC in tables:
                cur.execute(f"DESCRIBE `{T_EJEC}`")
                cols = [r['Field'] for r in cur.fetchall()]
                _lookup_cache['ejec_cols'] = cols
                for c in cols:
                    if 'nombre' in c.lower() or 'name' in c.lower():
                        _lookup_cache['ejec_name'] = c
                        break
                for c in cols:
                    if c.lower() in ('ejecutivoid', 'id'):
                        _lookup_cache['ejec_id'] = c
                        break

            # Cliente/Asegurado - buscar tabla que tenga razon social
            for tbl in ['RamosPeruAsegurado', 'Cliente', 'EmpresaAsegurado', 'Empresa']:
                if tbl in tables:
                    cur.execute(f"DESCRIBE `{tbl}`")
                    cols = [r['Field'] for r in cur.fetchall()]
                    _lookup_cache['cli_table'] = tbl
                    _lookup_cache['cli_cols'] = cols
                    for c in cols:
                        if 'razon' in c.lower() or 'nombre' in c.lower() or 'name' in c.lower():
                            _lookup_cache['cli_name'] = c
                            break
                    for c in cols:
                        if 'id' in c.lower():
                            _lookup_cache['cli_id'] = c
                            break
                    if 'cli_name' in _lookup_cache:
                        break

    finally:
        conn.close()

    return _lookup_cache


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

            # Cabecera
            cur.execute(f"DESCRIBE `{T_CAB}`")
            cab_cols = [r['Field'] for r in cur.fetchall()]
            result['cabecera_columns'] = cab_cols

            cur.execute(f"SELECT COUNT(*) AS total FROM `{T_CAB}`")
            result['cabecera_count'] = cur.fetchone()['total']

            # Detalle
            cur.execute(f"DESCRIBE `{T_DET}`")
            det_cols = [r['Field'] for r in cur.fetchall()]
            result['detalle_columns'] = det_cols

            cur.execute(f"SELECT COUNT(*) AS total FROM `{T_DET}`")
            result['detalle_count'] = cur.fetchone()['total']

            # Muestra de datos
            cur.execute(f"SELECT * FROM `{T_CAB}` LIMIT 1")
            sample = cur.fetchone()
            if sample:
                result['sample_row'] = {k: str(v)[:100] for k, v in sample.items()}

            # Lookups
            lookups = discover_lookups()
            result['lookups'] = {k: v for k, v in lookups.items() if not k.endswith('_cols')}

            # Lookup tables detail
            for prefix, table in [('aseg', T_ASEG), ('ramo', T_RAMO), ('prod', T_PROD), ('ejec', T_EJEC)]:
                if table in tables:
                    cur.execute(f"SELECT * FROM `{table}` LIMIT 2")
                    rows = cur.fetchall()
                    result[f'{prefix}_sample'] = [{k: str(v)[:80] for k, v in r.items()} for r in rows]

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


def build_base_query(lookups):
    """Construye el FROM + JOINs base."""
    joins = f"FROM `{T_CAB}` c"
    joins += f"\n LEFT JOIN `{T_ASEG}` a ON c.AseguradoraID = a.AseguradoraID"
    joins += f"\n LEFT JOIN `{T_RAMO}` r ON c.RamoId = r.RamoId"
    joins += f"\n LEFT JOIN `{T_PROD}` p ON c.ProducerId = p.ProducerId"
    joins += f"\n LEFT JOIN `{T_EJEC}` e ON c.EjecutivoId = e.EjecutivoId"
    joins += f"\n LEFT JOIN `{T_EMP}` em ON c.RamosPeEmpresaId = em.EmpresaID"
    return joins


def get_name_expr(lookups, key):
    """Devuelve la expresion SQL para obtener el nombre legible."""
    if key == 'aseguradora':
        return "a.AseguradoraNombre"
    elif key == 'ramo':
        return "r.RamoNombre"
    elif key == 'producer':
        return "CONCAT(COALESCE(p.ProducerPrimerNombre,''), ' ', COALESCE(p.ProducerApellidoPaterno,''))"
    elif key == 'ejecutivo':
        return "e.EjecutivoNombres"
    elif key == 'razon_social':
        return "COALESCE(em.EmpresaRazonSocial, c.RamosPeruBienAsegurado)"
    elif key == 'estado_pago':
        return "c.RamosPeruEstadoPago"
    return "'N/A'"


@app.route('/api/filters')
def api_filters():
    try:
        lookups = discover_lookups()
        conn = get_db()
        result = {}
        base = build_base_query(lookups)

        try:
            with conn.cursor() as cur:
                for key in ['producer', 'razon_social', 'aseguradora', 'ejecutivo', 'estado_pago']:
                    expr = get_name_expr(lookups, key)
                    try:
                        cur.execute(f"""
                            SELECT DISTINCT {expr} AS val
                            {base}
                            WHERE {expr} IS NOT NULL AND {expr} != ''
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
        lookups = discover_lookups()
        conn = get_db()
        base = build_base_query(lookups)

        # Expresiones para nombres legibles
        expr_razon = get_name_expr(lookups, 'razon_social')
        expr_ramo = get_name_expr(lookups, 'ramo')
        expr_producer = get_name_expr(lookups, 'producer')
        expr_aseg = get_name_expr(lookups, 'aseguradora')
        expr_ejec = get_name_expr(lookups, 'ejecutivo')
        expr_estado = get_name_expr(lookups, 'estado_pago')

        # ─── Filtros ───
        conditions = []
        params = []

        for key, expr in [('producer', expr_producer), ('razon_social', expr_razon),
                          ('aseguradora', expr_aseg), ('ejecutivo', expr_ejec),
                          ('estado_pago', expr_estado)]:
            val = request.args.get(key, '').strip()
            if val:
                values = val.split('||')
                placeholders = ','.join(['%s'] * len(values))
                conditions.append(f"{expr} IN ({placeholders})")
                params.extend(values)

        inicio_desde = request.args.get('inicio_desde', '').strip()
        inicio_hasta = request.args.get('inicio_hasta', '').strip()
        if inicio_desde:
            conditions.append("c.RamosPeruInicioVigencia >= %s")
            params.append(inicio_desde)
        if inicio_hasta:
            conditions.append("c.RamosPeruInicioVigencia <= %s")
            params.append(inicio_hasta)

        where = " AND ".join(conditions) if conditions else "1=1"

        # Columnas de valores numericos
        COL_PRIMA = "c.RamosPeruPrimaNeta"
        COL_MC_ZYRA = "c.RamosPeMontoComisionZeruk"
        COL_MC_PROD = "c.RamosPeMontoComisionProd"
        COL_FEE = f"(COALESCE({COL_MC_ZYRA}, 0) + COALESCE({COL_MC_PROD}, 0))"
        COL_FECHA = "c.RamosPeruInicioVigencia"

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
                    SELECT COALESCE({expr_razon}, 'N/A') AS name,
                           COALESCE(SUM({COL_FEE}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY c.RamosPeEmpresaId, em.EmpresaRazonSocial, c.RamosPeruBienAsegurado
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_contratantes_fee = [dict(r) for r in cur.fetchall()]

                # ─── Top Contratantes por Comision Producer ───
                cur.execute(f"""
                    SELECT COALESCE({expr_razon}, 'N/A') AS name,
                           COALESCE(SUM({COL_MC_PROD}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY c.RamosPeEmpresaId, em.EmpresaRazonSocial, c.RamosPeruBienAsegurado
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_contratantes_mc = [dict(r) for r in cur.fetchall()]

                # ─── Top Ramos por Prima Neta ───
                cur.execute(f"""
                    SELECT COALESCE({expr_ramo}, 'N/A') AS name,
                           COALESCE(SUM({COL_PRIMA}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY c.RamoId, r.RamoNombre
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_ramos_prima = [dict(r) for r in cur.fetchall()]

                # ─── Top Ramos por Comision Zyra ───
                cur.execute(f"""
                    SELECT COALESCE({expr_ramo}, 'N/A') AS name,
                           COALESCE(SUM({COL_MC_ZYRA}), 0) AS value
                    {base}
                    WHERE {where}
                    GROUP BY c.RamoId, r.RamoNombre
                    ORDER BY value DESC
                    LIMIT 10
                """, params)
                top_ramos_mc_zyra = [dict(r) for r in cur.fetchall()]

                # ─── Estado de Pago ───
                cur.execute(f"""
                    SELECT COALESCE({expr_estado}, 'N/A') AS estado,
                           COUNT(*) AS count,
                           COALESCE(SUM({COL_PRIMA}), 0) AS prima_neta,
                           COALESCE(SUM({COL_FEE}), 0) AS fee_neto
                    {base}
                    WHERE {where}
                    GROUP BY c.RamosPeruEstadoPago
                    ORDER BY prima_neta DESC
                """, params)
                estado_pago = [dict(r) for r in cur.fetchall()]

                # ─── Top Producers ───
                cur.execute(f"""
                    SELECT COALESCE({expr_producer}, 'N/A') AS producer,
                           COALESCE(SUM({COL_FEE}), 0) AS fee_neto,
                           COALESCE(SUM({COL_MC_PROD}), 0) AS mc_producer,
                           COALESCE(SUM({COL_PRIMA}), 0) AS prima_neta,
                           COUNT(*) AS count
                    {base}
                    WHERE {where}
                    GROUP BY c.ProducerId
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
                            COALESCE(SUM(d.RamoPeMonto), 0) AS monto_total,
                            SUM(CASE WHEN d.RamoPeEstadoPago LIKE '%%Pagad%%' THEN 1 ELSE 0 END) AS cuotas_pagadas,
                            SUM(CASE WHEN d.RamoPeEstadoPago LIKE '%%Pagad%%' THEN d.RamoPeMonto ELSE 0 END) AS monto_pagado
                        FROM `{T_DET}` d
                        INNER JOIN `{T_CAB}` c ON d.RamosPeruId = c.RamosPeruId
                        WHERE {where.replace(expr_producer, "'" + "N/A" + "'").replace(expr_razon, "COALESCE(c.RamosPeruBienAsegurado, '')").replace(expr_ramo, "c.RamoId").replace(expr_aseg, "c.AseguradoraID").replace(expr_ejec, "c.EjecutivoId") if 'LEFT JOIN' in build_base_query(lookups) else where}
                    """, params if not conditions else [])
                    cuotas_summary = cur.fetchone() or {}
                except Exception as e:
                    # Si falla el query de cuotas con filtros, intentar sin filtros
                    try:
                        cur.execute(f"""
                            SELECT
                                COUNT(*) AS total_cuotas,
                                COALESCE(SUM(d.RamoPeMonto), 0) AS monto_total,
                                SUM(CASE WHEN d.RamoPeEstadoPago LIKE '%%Pagad%%' THEN 1 ELSE 0 END) AS cuotas_pagadas,
                                SUM(CASE WHEN d.RamoPeEstadoPago LIKE '%%Pagad%%' THEN d.RamoPeMonto ELSE 0 END) AS monto_pagado
                            FROM `{T_DET}` d
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
