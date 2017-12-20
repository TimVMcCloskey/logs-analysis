#!/usr/bin/env python3
import psycopg2


def db_connect(database):
    '''Connects to a psql database'''
    try:
        connection = psycopg2.connect('dbname={}'.format(database))
        return (connection)
    except:
        print('Unable to connect to the database')


def db_disconnect(connection):
    cursor = connection.cursor()
    cursor.close()
    connection.close()


def get_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def print_query(result, type):
    for row in result:
        print('\t', row[0], '---', row[1], type)
    print('\n')


query1_title = 'What are the most popular three articles of all time?'
query1 = '''
    select a.title, count(*) as views
    from log as l, articles as a
    where l.path like concat('%', a.slug)
    group by a.title, l.path
    order by views desc
    limit 3;'''


query2_title = 'Who are the most popular article authors of all time?'
query2 = '''
    select au.name, cast(sum(num) as int) as views from
    (
        select ar.author, ar.title, count(*) as num
        from log as l, articles as ar
        where l.path like concat('%', ar.slug)
        group by ar.author, ar.title
        order by num desc
    ) as t, authors as au
    where au.id = t.author
    group by au.name
    order by views desc;'''


query3_title = 'On which days did more than 1% of requests lead to errors?'
query3 = '''
    select cast(day as text) as day, round(percent::numeric,2) as percent
    from (
        select cast(day as date) as day, total, errors,
        100.0 * (cast(errors as float) / cast(total as float)) as percent
        from (
            select date_trunc('day', time) as day,
            count(*) as total,
            count(case when status not like '200 OK' then 1 end) as errors
            from log
            group by day
        ) as t
    ) as t2
    where percent > 1.0
    order by day asc;'''


if __name__ == '__main__':
    connection = db_connect('news')
    print(query1_title)
    result = get_query(connection, query1)
    print_query(result, 'views')
    print(query2_title)
    result = get_query(connection, query2)
    print_query(result, 'views')
    print(query3_title)
    result = get_query(connection, query3)
    print_query(result, '%  errors')
