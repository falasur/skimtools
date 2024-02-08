""" Set of functions that could be useful for 
    working with SKIM datamodels and statistical forms
"""

import pandas as pd
import difflib
import yaml
import re
from pandas import DataFrame

COLUMNS = ['ID', 'Наименование*', 'Родительский раздел/Путь*', 'Описание*', 'Тип*',
       'Дата ввода в действие',
       'Формула расчёта (если гбт расчётная и формула распространяется на все БТ)',
       'Ссылка на регламентирующий документ в Репозитории',
       'Код бизнес-процесса', 'Псевдоним', 'Международный глоссарий',
       'Глоссарий ОАО "РЖД"', 'Комментарий', 'Отв. Редактор', 'Отв. Контролер',
       'Контактные лица', 'Теги', 'ID БТ', 'Единица измерения*',
       'Дискретность*', 'Метрика*', 'Тип данных*', 'Тип значения*',
       'Ссылка на регламентирующий документ в Репозитории.1', 'Лэйбл ссылки',
       'Номер и дата регламентирующего документа*', 'Дата ввода в действие*',
       'Алгоритм расчёта*', 'Формула расчёта*',
       'Правила обработки для сравнительного анализа ', 'Бизнес-владелец*',
       'Руководство ОАО «РЖД»*', 'Правила контроля', 'Код бизнес-процесса.1',
       'Критерий оценки ', 'Отв. Редактор.1', 'Отв. Контролер.1',
       'Подразделение отв. Редактора и отв. Контролера', 'Контактные лица.1',
       'Тематики', 'Подразделение владельца формы отчётности',
       'Индекс/код и наименование формы отчётности*',
       '№Таблицы‚ №строки‚№ графы в форме*',
       'Индекс/код и наименование   связанной формы, справочно-аналитической формы*',
       'Подразделение технического владельца', 'Регламент формирования*',
       'Ответственный сотрудник ОАО «РЖД» за ввод  данных в систему мастер-данных*',
       'Система‚ источник учетной формы*',
       'Система‚ источник формирования показателя*',
       'Система‚ источник отчетной формы (публикация)*',
       'Справочник показателей (территориальный)*',
       'Справочник показателей (функциональный)*',
       'Указать справочник var/h-code или иное и указать код*', 'ID единый',
       'Комментарий.1', 'Принадлежность к виду отчетности']

COLUMN_MAPPER = {
    'ПОКАЗАТЕЛЬ 1 УРОВНЯ': 'Наименование*',
    'PLACE': 'Родительский раздел/Путь*',
    'ЕД. ИЗМЕРЕНИЯ': 'Единица измерения*',
    'ТИП ОТЧЕТНОГО ПЕРИОДА': 'Дискретность*',
    'ВЕРСИЯ ДАННЫХ': 'Метрика*',
    'VAL_TYPE': 'Тип данных*',
    'ТИП ЗНАЧЕНИЯ': 'Тип значения*',
    'НОРМАТИВНЫЙ ДОКУМЕНТ': 'Ссылка на регламентирующий документ в Репозитории',
    'НОРМАТИВНЫЙ ДОКУМЕНТ': 'Номер и дата регламентирующего документа*',
    'РЕГЛАМЕНТ ФОРМИРОВАНИЯ ПОКАЗАТЕЛЕЙ': 'Алгоритм расчёта*',
    'АЛГОРИТМ РАСЧЕТА': 'Формула расчёта*',
    'ОТВ. \nПОДР.': 'Бизнес-владелец*',
    'Ответственный заместитель генерального директора':'Руководство ОАО «РЖД»*',
    'ФОРМА СТАТИСТИЧЕСКОЙ ОТЧЕТНОСТИ':'Индекс/код и наименование формы отчётности*',
    'АЛГОРИТМ РАСЧЕТА':'Регламент формирования*',
    'ИСТОЧНИК МАСТЕР-ДАННЫХ':'Система‚ источник учетной формы*',
    'ИСТОЧНИК МАСТЕР-ДАННЫХ':'Система‚ источник формирования показателя*',
    'ИСТОЧНИК МАСТЕР-ДАННЫХ':'Система‚ источник отчетной формы (публикация)*',
    'ОРГСТРУКТУРА':'Справочник показателей (территориальный)*',
    'СТРУКТУРА':'Справочник показателей (функциональный)*',
    'VAR':'Указать справочник var/h-code или иное и указать код*'   
}

#### General ####


# Testing that module is imported
def test_import():
    print("Module sucessfully imported")


#### Data models opening and usage ####


# opening datamodel with specified params
# Example:
# data = open_md(filename, **guess_params(filename, open_md, options_set))
def open_md(filename, *args, **kvargs):
    return pd.read_csv(filename, *args, **kvargs).dropna(how='all')


# Guessing params (sep, encoding) in order to properly open datamodel
def guess_params(filename, opening_function=open_md, options_set=[
                                                          {}, 
                                                          {'sep':';'}, 
                                                          {'sep':';', 'encoding':'cp1251'}, 
                                                          {'encoding':'cp1251'}
                                                          ]):
                                                          # most of the params
                                                          # are included in the set
    for options in options_set:
        try:
            opening_function(filename, **options)
            return options
        except:
            pass


#### parsing and transforming datamodel ####



# Задача: в столбце могут быть значения с ошибками
# Необходимо смапить их на эталонный справочник
# Ищем похожие значения для справочника
def map_attribute(from_skim, from_dict):
    return {item: 
            difflib.get_close_matches(item, from_dict)[0] 
            if difflib.get_close_matches(item, from_dict) != [] 
            else 'not_found'
            for item in from_skim}


# Собираем все уникальные значения в столбце исходной модели данных
def get_set_attribute(dataframe, colname):
    return dataframe[colname].dropna().unique()

# Возвращает индекс формы отчетности (БЦ-БЦ) из строки
def get_form_names(x):
    if pd.isna(x):
        return ''
    return ', '.join(re.findall('[А-Яа-я0-9]+-[А-Яа-я0-9]+|[С|с]правка №*\d+', x))


# Чтобы не перезаписывать случайно файлы с конфигами, заведем декоратор, 
# который проверяет, что мы не вслепую деелаем
def doublecheck(function):
    def wrapper(*args, **kvargs):
        print('Это действие приведет к перезаписи файла. Продолжить? Y/n')
        respond = input()
        if respond == 'y' or respond == '':
            function(*args, **kvargs)
        else:
            raise KeyboardInterrupt
    return wrapper


# Запись файлов с параметрами
@doublecheck
# writing stuff in yaml
def write_yaml(filename, obj):
    with open(filename, 'w+') as f:
        yaml.dump(obj, 
                  f, 
                  allow_unicode=True, 
                  encoding='utf-8')


def read_yaml(filename):
    with open(filename, 'r') as f: # optional: encoding='utf-8'
        return yaml.safe_load(f)


# Write a mapper
def mapper(datamodel, column_name, plausible_mapper):
    return map_attribute(
        get_set_attribute(datamodel,
                          column_name),
        plausible_mapper)


# ИСправление колонки в соответствии с заданным маппером
def correct_column(df, column, na_filler, mapper):
    return df[column].fillna(na_filler).replace(mapper)


# Analyzing VAR postfixes in accordance with business logic
def fixing_deviations_based_on_var(data, var_column, metrics_column):
    for i in range(len(data[metrics_column])):
        if data[metrics_column][i] == 'Относительное отклонение (скорректировать)':
            if data[var_column][i].endswith('_fp'):
                data[metrics_column][i] = 'Относительное отклонение факта от плана'
            elif data[var_column][i].endswith('_ff'):
                data[metrics_column][i] = 'Относительное отклонение факта от факта прошлого года'
        if data[metrics_column][i] == 'Абсолютное отклонение (скорректировать)':
            if data[var_column][i].endswith('_afp'):
                data[metrics_column][i] = 'Абсолютное отклонение факта от плана'
            elif data[var_column][i].endswith('_aff'):
                data[metrics_column][i] = 'Абсолютное отклонение факта от факта прошлого года'


# проверка того, есть ли значения в эталонном справочнике.
# Справочник должен быть в формате csv, одна колонка
def dictionary_check(dataframe,
                     column,
                     dictionary_filename,
                     filename=None,
                     reader_options={'encoding': 'utf-8',
                                     'sep': ';'}):
    collection = get_set_attribute(dataframe, column)
    dic = list(pd.read_csv(dictionary_filename, **reader_options).squeeze())
    if filename:
        write_yaml(filename,
                   {i: True if i in dic else False for i in collection})
    return {i: True if i in dic else False for i in collection}

def split_rows_by_comma(dataframe, column):
    result = dataframe.copy()
    for i in range(len(dataframe)): 
        try:
            multiplier = result.loc[i,column].split(',')
        except AttributeError:
            print(result.loc[i,column])
            continue
        if len(multiplier) > 1:
            current_row = result.loc[i, :].copy()
            for case in multiplier:
                new_row = current_row.copy()
                new_row[column] = case
                result.loc[len(result)] = new_row
    result = result[~result[column].str.contains(',')]
    result.index = np.arange(len(result))
    return result

def fix_agg_result(dataframe, column):
    result = dataframe.copy()
    for i in range(len(result)):
        multiplier = result.loc[i, column].split('/')
        if len(multiplier) > 1:
            current_row = result.loc[i, :].copy()
            try:
                first_part = re.search('.+?(?=[а-яА-я]+$)', multiplier[0]).group(0)
            except AttributeError:
                continue
            new_row = current_row.copy()
            new_row[column] = multiplier[0]
            result.loc[len(result)] = new_row
            for period in multiplier[1:]:
                new_value = first_part + period
                new_row = current_row.copy()
                new_row[column] = new_value
                result.loc[len(result)] = new_row
    result = result[~result[column].str.contains('/')]
    return result

def compare_name(iterable, val, function):
    for i in iterable:
        if function(i) == val:
            return i
        


### Class definition###


class DataModel(DataFrame):
    def __init__(self, data, column_mapper=COLUMN_MAPPER, columns=COLUMNS):
        temp_data = data.rename(column_mapper)
        super().__init__(columns = columns)
        for i in self.columns:
            self[i] = pd.Series(['']*len(data))
        for i,j in column_mapper.items():
            self[i] = data[j]
# Дописать - чтобы шапка для импортера из верхних пяти строк была одинаковая

# Поправить Тип на расчетный
    def fix_type(self, type_col = 'Тип*'):
        return self.replace(
            to_replace={type_col: ''}, 
            value='расчетный')
    
# Поправить некорректные символы в справочниках
    def filter_functional_and_territorial_dicts(self, 
                                                orgstructure='Справочник показателей (территориальный)*', 
                                                structure='Справочник показателей (функциональный)*'):
        return self.replace(to_replace={orgstructure: ['\n', '\t'],
                                        structure: ['\n', '\t']},
                                        value=', ').\
                    replace(to_replace={orgstructure: ['∑'],
                                        structure: ['∑']},
                                        value='Сумма')

# Записать уникальные наборы значений в колонках    
    def write_sets_of_columns(self, 
                              column_set=['Единица измерения*', 'Дискретность*', 'Метрика*', 'Тип значения*'],
                              dictionary_folder="./skim_N_sets/"):
        for column in column_set:
            column_values = get_set_attribute(self, 
                                              column)
            column_dictionary = {i: i for i in column_values}
            temporal_name = re.sub(r'\W+', '', column)
            filename = dictionary_folder + temporal_name + ".yaml"
            print(column)
            write_yaml(filename, column_dictionary)

'''    def map_columns(self, mapper_folder):
        for file in os.listdir('./skim_N_sets/'):
           path = './skim_N_sets/' + file
           print(path)
           mapper = read_yaml(path)
        which_column = file[:-5]
    column_name = compare_name(target_dm.columns, 
                               which_column, 
                               lambda x: re.sub(r'\W+', '', x))
    
    new_column = correct_column(target_dm, 
                                column=column_name, 
                                na_filler='', 
                                mapper=mapper)
    new_column_name = column_name
    
    target_dm[new_column_name] = new_column
'''

# Актуально для СКИМ Н - поправить типы значений, записанные в формате "Итог, нарастающий итог по месяцам/суткам/неделям с начала месяца"
    def fix_val_type(self, 
                     valtype_column='Тип значения*'):
        return fix_agg_result(split_rows_by_comma(self, 'Тип значения*'), 'Тип значения*')