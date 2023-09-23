import gspread
from google.oauth2 import service_account
import pandas as pd
import psycopg2
import json
import time
# Открыть нужную таблицу
def sheet_autorization(table_key):
    spreadsheet = client.open_by_key(table_key) # Открыть Google Таблицу по имени
    return spreadsheet

# Извлечь имя листа
def sheet_name(sheet_index):
    sheet = spreadsheet.get_worksheet(sheet_index) # Обращаемся к конкретному листу
    table_name = "tab_" + sheet.title # Извлечение имени таблицы
    print(table_name)
    time.sleep(2)
    return table_name

# Подготовить данные
def sheet_data(sheet_index):    
    sheet = spreadsheet.get_worksheet(sheet_index) # Обращаемся к конкретному листу
    data = sheet.get_all_records(value_render_option='UNFORMATTED_VALUE') # Забрать таблицу без преобразования форматов
    df = pd.DataFrame(data) # Преоразование в dataframe без пропущеных строк
    df = df.replace("", float("nan")).dropna(how='all')
    df = df.replace(float("nan"), 0)
    return df

# Отправка таблицы "Каталог исполнений EN" в pgsql
def sql_pull_catalouge(table_name, df): 
    # Установка подключения к PostgreSQL серверу
    conn = psycopg2.connect(
        host = credentials_db['host'],
        port = credentials_db['port'],
        database = credentials_db['database'],
        user = credentials_db['user'],
        password = credentials_db['password'])
    
    # Создание курсора для выполнения запросов
    cur = conn.cursor()
    
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,)) # Проверка наличия таблицы в PostgreSQL
    table_exists = cur.fetchone()[0]

    if table_exists:
        # Если таблица существует, удалите все записи из нее перед загрузкой новых данных
        cur.execute(f"TRUNCATE TABLE {table_name};")
    else:
        # Если таблица не существует, создайте новую таблицу
        cur.execute(f"""CREATE TABLE {table_name} (
                        mark VARCHAR,
                        sine VARCHAR,
                        name VARCHAR,
                        type VARCHAR,
                        diameter_class VARCHAR,
                        diameter_dn NUMERIC,
                        mass NUMERIC,
                        mass_podklad NUMERIC,
                        fz_minus NUMERIC,
                        fz NUMERIC,
                        fx NUMERIC,
                        fy NUMERIC,
                        mz NUMERIC,
                        mx NUMERIC,
                        my NUMERIC,
                        developer VARCHAR,
                        status VARCHAR,
                        plate VARCHAR,
                        stoper NUMERIC,
                        support_material NUMERIC,
                        support_type VARCHAR,
                        support_work VARCHAR,
                        support_loader VARCHAR,
                        support_demove VARCHAR,
                        pipeline_material VARCHAR,
                        support_length VARCHAR,
                        load_group VARCHAR,
                        A NUMERIC,
                        B NUMERIC,
                        C NUMERIC,
                        D NUMERIC,
                        d_ NUMERIC,
                        E NUMERIC,
                        F NUMERIC,
                        G NUMERIC,
                        H NUMERIC,
                        L NUMERIC,
                        M NUMERIC,
                        S NUMERIC,
                        X NUMERIC,
                        Y NUMERIC,
                        Z NUMERIC,
                        figure VARCHAR,
                        K NUMERIC,
                        K1 NUMERIC,
                        spring VARCHAR,
                        collection_max VARCHAR,
                        collection_min VARCHAR,
                        lisega_2020 VARCHAR,
                        lisega_2010ru VARCHAR,
                        aku VARCHAR,
                        isp NUMERIC,
                        A1 NUMERIC,
                        B1 NUMERIC,
                        H1 NUMERIC,
                        A2 NUMERIC,
                        B2 NUMERIC,
                        H2 NUMERIC);""")

    # Загрузка данных из датафрейма в PostgreSQL
    for _, row in df.iterrows():
        values = tuple(row)
        cur.execute(f"INSERT INTO {table_name} VALUES {values};")

    # Применение всех изменений в базе данных
    conn.commit()

    cur.close() # Закрытие соединения с PostgreSQL сервером
    conn.close()
    
def table_merge():
    # Установка подключения к PostgreSQL серверу
    conn = psycopg2.connect(
        host=credentials_db['host'],
        port=credentials_db['port'],
        database=credentials_db['database'],
        user=credentials_db['user'],
        password=credentials_db['password'])

    # Создайте курсор для выполнения SQL-запросов
    cur = conn.cursor()

    # Запуск процедуры merge_tables_as_one
    cur.execute('CALL merge_tables_as_one()')
    print('Процедура merge_tables_as_one применена')
    # Закоммитите изменения
    conn.commit()

    # Закрыть курсор и соединение с базой данных
    cur.close()
    conn.close()

def sheet_catalouge(index):    
    table_name = sheet_name(index).lower()

    # Установка подключения к PostgreSQL серверу
    conn = psycopg2.connect(
        host = credentials_db['host'],
        port = credentials_db['port'],
        database = credentials_db['database'],
        user = credentials_db['user'],
        password = credentials_db['password'])
    
    # Создание курсора для выполнения запросов
    cur = conn.cursor()

    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)", (table_name,)) # Проверка наличия таблицы в PostgreSQL
    table_exists = cur.fetchone()[0]
    print(f'Table {table_name} exists: {table_exists}')

    if table_exists:
        # Если таблица существует, удалите все записи из нее перед загрузкой новых данных
        cur.execute(f"TRUNCATE TABLE {table_name};")
        print('Table turnicated')
    else:
        # Если таблица не существует, создайте новую таблицу
        cur.execute(f"""CREATE TABLE {table_name} (
                        note VARCHAR,
                        mark VARCHAR);""")
        print('Table created')

    # Загрузка данных из датафрейма в PostgreSQL
    for _, row in df.iterrows():
        values = tuple(row)
        cur.execute(f"INSERT INTO {table_name} VALUES {values};")

    print(f'Data inserted to {table_name} table')

    # Применение всех изменений в базе данных
    conn.commit()

    cur.close() # Закрытие соединения с PostgreSQL сервером
    conn.close()




# Начало программы



############################################# Загрузка учетных данных из файла dbcredentials.json



with open('credentials_db.json') as f:
    credentials_db = json.load(f)

# Подключение к таблице google sheets
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials_gs = service_account.Credentials.from_service_account_file('credentials_gs.json', scopes=scope)
client = gspread.authorize(credentials_gs)



############################################## Загрузка таблицы "Каталог исполнений EN"



# Имя Google таблицы "Каталог исполнений EN"
table_key = '1XXqpF812VpcDxl8vKbdoOdzEPRkntHr78UikhM3QBEE'

spreadsheet = sheet_autorization(table_key)

worksheet_list = spreadsheet.worksheets() # Получить список всех листов
sheet_titles = [worksheet.title for worksheet in worksheet_list] # Получить список всех названий листов

# Выбор нужных листов из sheet_titles
selected_sheets = ['11', '12', '13', '14',
                   '21', '22',
                   '31', '32', '33', '34', '35', '36', '37', '38', '39',
                   '41', '42', '43', '44', '45',
                   '51', '52', '53', '54', '55', '56',
                   '61', '62', '63', '64', '65', '66', '67',
                   '71', '72', '73', '74',
                   '81', '82', '83', '84', '85', '86', '87', '88',
                   '92', '101']

selected_index = [] # Создание пустого списка selected_index

# Итерация по выбранным листам
for selected_sheet in selected_sheets:
    # Найдите индекс выбранного листа в списке
    index = sheet_titles.index(selected_sheet)
    # Добавление индекса выбранного листа в selected_index
    selected_index.append(index)

for index in selected_index:
    sql_pull_catalouge(sheet_name(index), sheet_data(index))

table_merge()



############################################## Загрузка таблицы "Каталог исполнений EN"



# Имя Google таблицы "База данных классификатора KT2"
table_key = '1IuvKFnJiJrreNc7r1Z0raRZ_2Jldb9stRviL29npjPw'

spreadsheet = sheet_autorization(table_key)

worksheet_list = spreadsheet.worksheets() # Получить список всех листов
sheet_titles = [worksheet.title for worksheet in worksheet_list] # Получить список всех названий листов

# Выбор нужных листов из sheet_titles
selected_sheets = ['CatAKU']

selected_index = [] # Создание пустого списка selected_index

# Итерация по выбранным листам
for selected_sheet in selected_sheets:
    # Найдите индекс выбранного листа в списке
    index = sheet_titles.index(selected_sheet)
    # Добавление индекса выбранного листа в selected_index
    selected_index.append(index)
    
selected_index[0]

sheet = spreadsheet.get_worksheet(selected_index[0]) # Обращаемся к конкретному листу
data = sheet.get_all_records(value_render_option='UNFORMATTED_VALUE') # Забрать таблицу без преобразования форматов
df = pd.DataFrame(data) # Преоразование в dataframe без пропущеных строк
df = df[['Note', 'kt2cat']]
df = df.replace("", float("nan")).dropna(how='all')
df = df.replace('....', float("nan")).dropna(how='all')

# Отправка таблицы "Каталог исполнений EN" в pgsql
sheet_catalouge(index)

# Выбор нужного листа из sheet_titles
selected_sheets = ['CatKT2']

selected_index = [] # Создание пустого списка selected_index

# Итерация по выбранным листам
for selected_sheet in selected_sheets:
    # Найдите индекс выбранного листа в списке
    index = sheet_titles.index(selected_sheet)
    # Добавление индекса выбранного листа в selected_index
    selected_index.append(index)
    
selected_index[0]

sheet = spreadsheet.get_worksheet(selected_index[0]) # Обращаемся к конкретному листу
data = sheet.get_all_records(value_render_option='UNFORMATTED_VALUE') # Забрать таблицу без преобразования форматов
df = pd.DataFrame(data) # Преоразование в dataframe без пропущеных строк
df = df[['Note', 'Маркировка_KT2']]
df = df.replace("", float("nan")).dropna(how='all')
df = df.replace('....', float("nan")).dropna(how='all')
df = df.dropna(subset=['Note'])
df = df.dropna(subset=['Маркировка_KT2'])

# Отправка таблицы "Каталог исполнений EN" в pgsql
sheet_catalouge(index)