__author__ = 'cbell'
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select
from sqlalchemy.exc import InvalidRequestError
import argparse
import getpass


def login_prompt(credentials):
    user_name = raw_input('user [default - {}]:'.format(getpass.getuser()))
    if not user_name:
        user_name = getpass.getuser()
    password = getpass.getpass()
    credentials = '{0}:{1}'.format(user_name, password)
    return credentials


def get_mysql_connection(server, credentials=None):
    if not credentials:
        print 'Please enter user credentials for {}'.format(server)
        credentials = login_prompt(credentials)

    conn_string = 'mysql://{0}@{1}/mysql'.format(credentials, server)
    engine = create_engine(conn_string)
    connection = engine.connect()
    return connection


def main(args):
    orig_info = {}
    copy_info = {}
    if len(args.table.split('.')) == 2:
        orig_info = {'table': args.table, 'schema': args.table.split('.')[0], 'name': args.table.split('.')[1]}
        copy_info = orig_info
    else:
        exit('table must include source schema')

    # TODO implement rename
    source_conn = get_mysql_connection(server=args.source)
    dest_conn = get_mysql_connection(server=args.dest)
    source_meta = MetaData(bind=source_conn, schema=orig_info['schema'])
    dest_meta = MetaData(bind=dest_conn, schema=copy_info['schema'])
    orig_table = None
    copy_table = None
    try:
        source_meta.reflect(only=[orig_info['name']])
        orig_table = source_meta.tables[orig_info['table']]
    except InvalidRequestError:
        exit('{} doesn\'t exist on source server'.format(orig_info['table']))
    if not orig_table.exists(bind=dest_conn):
        copy_table = orig_table.tometadata(metadata=dest_meta, schema=copy_info['schema'])
        copy_table.create(bind=dest_conn, checkfirst=True)
    insert_stmt = copy_table.insert()
    select_orig = source_conn.execute(select([orig_table]))
    while not select_orig.closed:
        rows = select_orig.fetchmany(50000)
        if rows:
            dest_conn.execute(insert_stmt, rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Creates a copy of specified table from source server\'s database in destination server\'s '
                    'database.')
    parser.add_argument('--source', required=True, help='The server that the original table is on.')
    parser.add_argument('--dest', required=True, help='The server that the copy will be created on')
    parser.add_argument('--table', required=True, help='The table to be copied')
    # TODO implement
    # parser.add_argument('--rename', action='store', help='The name the copied table will have')
    arguments = parser.parse_args()
    main(arguments)
