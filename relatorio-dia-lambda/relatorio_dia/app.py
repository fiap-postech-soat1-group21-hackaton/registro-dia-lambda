import json
import time
from os import getenv, putenv
from datetime import datetime, timedelta
import pymysql


putenv('TZ', 'America/Sao_Paulo')
time.tzset()

def lambda_handler(event, _):

    try:
        connection = None
        connection = get_db_connection()
        user_id = get_user_id(event)
        fmt_data = {}

        if event['queryStringParameters']['tipo'] == 'dia':
            fmt_data = current_day_balance(connection, user_id)

        if event['queryStringParameters']['tipo'] == 'mes':
            fmt_data['mensagem'] = 'pendente de implantação'
            # search = event['queryStringParameters'].get('ano-mes') 
            # datetime_object = datetime.strptime(search, '%Y-%m') if search else datetime.now()
            # fmt_data = monthly_balance(connection, user_id, datetime_object)

        return {
            'statusCode': 200,
            "body": json.dumps({
                "success": True,
                "data": fmt_data,
            }),
        }

    except Exception as e:
        return {
            'statusCode': 500,
            "body": json.dumps({
                "success": False,
                "message": str(e),
            }),
        }

    finally:
        if connection:
            connection.close()

def current_day_balance(connection, user_id):
    register_records = get_daily_registers(user_id, connection)
    closed_registers, open_registers = separete_registers(list(register_records))
    workload_balance_tuple = get_workload_balance(closed_registers)
    fmt_data = fmt_workload_balance(workload_balance_tuple, open_registers)
    return fmt_data

def monthly_balance(connection, user_id, range_search):

    result_month = []

    register_records = get_monthly_registers(user_id, connection, range_search)
    grouped_by_day = separete_registers_by_day(register_records) 

    for date, datetimes in grouped_by_day.items():
        closed_registers, open_registers = separete_registers(list(datetimes))
        workload_balance_tuple = get_workload_balance(closed_registers)
        fmt_data = fmt_workload_balance(workload_balance_tuple, open_registers)
        result_month.update({str(date):fmt_data })

    return result_month

def get_user_id(event):
    user_id = event['requestContext']['authorizer']['claims']['sub']
    return user_id

def get_db_connection():
    try:
        endpoint = getenv("DB_HOST")
        database = getenv("DB_NAME")
        port = int(getenv("DB_PORT"))
        password = getenv("DB_PASSWORD")
        username = getenv("DB_USER")

        connection = pymysql.connect(host=endpoint, port=port, user=username, passwd=password, db=database)

        if not connection:
            raise RuntimeError('Falha ao conectar no banco de dados. Verifique variaveis de ambiente')

        return connection

    except Exception as e:
        raise RuntimeError('Erro na conexão com o banco de dados:', e)

def get_daily_registers(user_id, connection):
    try:
        register_table = getenv("DB_TABLE")
        today = datetime.now().date()
        start_of_day = datetime.combine(today, datetime.min.time())
        end_of_day = datetime.combine(today, datetime.max.time())
        
        with connection.cursor() as cursor:
            sql = f"SELECT registro FROM {register_table} WHERE matricula = %s AND registro BETWEEN %s AND %s"
            cursor.execute(sql, (user_id, start_of_day, end_of_day))
            register_records = cursor.fetchall()
        
        return register_records

    except Exception as e:
        raise RuntimeError('Erro ao buscar registros diários:', e)

def get_monthly_registers(user_id, connection, range_search: datetime):
    try:
        register_table = getenv("DB_TABLE")
        today = range_search.date()
        first_day_of_month = today.replace(day=1)
        start_of_month = datetime.combine(first_day_of_month, datetime.min.time())
        end_of_month = start_of_month + timedelta(days=31)

        with connection.cursor() as cursor:
            sql = f"SELECT registro FROM {register_table} WHERE matricula = %s AND registro BETWEEN %s AND %s"
            cursor.execute(sql, (user_id, start_of_month, end_of_month))
            register_records = cursor.fetchall()

        return register_records

    except Exception as e:
        raise RuntimeError('Erro ao buscar registros mensais:', e)

def separete_registers(register_records: list[datetime]):
    open_register = []

    if not register_records:
        return [], []

    if len(register_records)%2:
        open_register.append(register_records.pop())
    
    return [r[0] for r in register_records], [r[0] for r in open_register]

def separete_registers_by_day(register_records):
    grouped_by_day = {}
    for tuple_item in register_records:
        datetime_object = tuple_item[0]
        date = datetime_object.date()
    
    if date in grouped_by_day:
        grouped_by_day[date].append(datetime_object)
    else:
        grouped_by_day[date] = [datetime_object]

    return grouped_by_day

def get_workload_balance(closed_registers:  list[datetime]):
    
    periods: list[list[datetime, datetime]] = []
    periods_to_interval: list[datetime] = []

    total_work: float = 0
    total_intervals: float = 0

    while closed_registers:
        work_in = closed_registers[0]
        work_out = closed_registers[1]

        diff = work_out - work_in
        total_work += diff.total_seconds()

        periods.append([work_in,work_out])
        periods_to_interval.insert(0, work_out)

        closed_registers.pop(0)
        closed_registers.pop(0)

    while periods_to_interval:
        work_out_higher: datetime = periods_to_interval[0]
        work_out_lower: datetime = periods_to_interval[1]

        diff = work_out_higher - work_out_lower
        total_intervals += diff.total_seconds()

        periods_to_interval.pop(0)
        periods_to_interval.pop(0)
        
    return periods, total_work, total_intervals

def fmt_workload_balance(workload_balance_tuple, open_registers):
    periods: list[list[datetime, datetime]] = workload_balance_tuple[0]
    total_work, total_intervals = workload_balance_tuple[-2:]

    fmt_result = {
        'total_horas_trabalhadas': 0.00,
        'total_horas_intervalo': 0.00,
        'registros': [],
        'registros_abertos': []
    }

    hours = lambda timedelta: int((timedelta/3600)*100)/100
    
    fmt_result['total_horas_trabalhadas'] = hours(total_work)
    fmt_result['total_horas_intervalo'] = hours(total_intervals)

    for work_in, work_out in periods:
        fmt_result['registros'].append([str(work_in),str(work_out)])

    for register in open_registers:
        fmt_result['registros_abertos'].append(str(register))


    return fmt_result
