#!/usr/bin/env python
"""
Pharmaceutical study management automatizer

(C) Copyright 2016
S. Sanjari

"""

import argparse
import sys
import csv
import sqlite3
import os
import re
import logging as log

__version_info__ = (0, 1, 0)
__version__ = '.'.join('%d' % d for d in __version_info__)


def sort_ctp_table(db_filename):
    db = sqlite3.connect(db_filename)
    cur = db.cursor()
    cur.execute("""SELECT sql FROM sqlite_master
    WHERE tbl_name = 'ctp_table' AND type = 'table';""")
    rows = cur.fetchall()
    col_names = rows[0][0].replace(' ', '').replace(')', '').replace('\n', '').replace('CREATETABLEctp_table(',
                                                                                       '').replace(
        'INTEGERPRIMARYKEYAUTOINCREMENT', '').split(',')
    col_names.sort()
    sql = 'CREATE TABLE IF NOT EXISTS temp_ctp_table (%s);' % ', '.join(
        a for a in col_names)
    cur.execute(sql)
    sql = 'INSERT INTO temp_ctp_table SELECT %s FROM ctp_table;' % ', '.join(
        a for a in col_names)
    cur.execute(sql)
    sql = 'DROP TABLE ctp_table;'
    cur.execute(sql)
    sql = 'ALTER TABLE temp_ctp_table RENAME TO ctp_table;'
    cur.execute(sql)
    db.commit()
    db.close()


def read_ctp_csv(db_filename, ctp_filename):
    to_db = []
    with open(ctp_filename, 'r') as f:
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(f)  # comma is default delimiter
        for i in dr:
            if i['visit_number']:
                visit_number = i['visit_number'].strip().upper()
            if i['day_number']:
                day_number = i['day_number'].strip().upper()

            if i['visit_description']:
                visit_description = i['visit_description'].strip().upper()

            if i['color']:
                color = i['color'].strip().upper()
            to_db.append((visit_number, day_number, visit_description, color))

    col0 = 'id'
    col1 = 'visit_number'
    col2 = 'day_number'
    col3 = 'visit_description'
    col4 = 'color'

    try:
        db = sqlite3.connect(db_filename)
        cur = db.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS ctp_table ({} INTEGER PRIMARY KEY AUTOINCREMENT,
            {}, {}, {}, {}
            );""".format(
                col0,
                col1,
                col2,
                col3,
                col4))

        cur.executemany("""INSERT INTO ctp_table (
                    {}, {}, {}, {}) VALUES (
                    ?, ?, ? ,?);""".format(
            col1,
            col2,
            col3,
            col4), to_db)
        db.commit()

    except Exception as e:
        # Roll back any change if something goes wrong
        db.rollback()
        raise e

    finally:
        # Close the db connection
        db.close()


def filename_sanitizer(filename):
    roster_base_filename = os.path.basename(filename)
    fn = os.path.splitext(roster_base_filename)[0].lower()
    if 'brown' in fn or 'brn' in fn:
        color = 'BRN'
    if 'blue' in fn or 'blu' in fn:
        color = 'BLU'
    if 'white' in fn or 'whi' in fn:
        color = 'WHI'
    if 'yellow' in fn or 'yel' in fn:
        color = 'YEL'
    if 'green' in fn or 'gre' in fn:
        color = 'GRE'
    if 'red' in fn:
        color = 'RED'
    fn = fn.replace(',', ' ').replace(
        '_', ' ').replace('.', ' ').replace('-', ' ')
    fn = fn.replace('jan', '01').replace('feb', '02').replace('mar', '03')
    fn = fn.replace('apr', '04').replace('may', '05').replace('jun', '06')
    fn = fn.replace('jul', '07').replace('aug', '08').replace('sep', '09')
    fn = fn.replace('oct', '10').replace('nov', '10').replace('dec', '12')
    fn = fn.split()
    ep_number = fn[3]

    roster_rcvd_date = ''.join(fn[0:3])
    roster_rcvd_date = re.sub("\D", "", roster_rcvd_date)
    roster_sent_date = ''.join(fn[-1:-4:-1])
    roster_sent_date = re.sub("\D", "", roster_sent_date)
    return roster_rcvd_date, roster_sent_date, ep_number, color, roster_base_filename


def get_clean_rosterfiles(filename):
    cnt = 0
    line_array = []
    with open(filename) as f:
        for i in f:
            if i.startswith('\"') or cnt == 0:
                line_array.append(i)
            cnt += 1
    return line_array


def read_roster_csv(db_filename, roster_filename):
    roster_rcvd_date, roster_sent_date, ep_number, color, roster_base_filename = filename_sanitizer(
        roster_filename)

    # csv.DictReader uses first line in file for column headings by default
    f = get_clean_rosterfiles(roster_filename)
    dr = csv.DictReader(f)  # comma is default delimiter

    to_db = []
    for i in dr:
        protocol_name = i['Protocol Name']
        accession_number = i['Accession #']
        patient_number = i['Patient Number']
        site_number = i['Site Number']
        visit_type = i['Visit Type']
        vn = i['Visit Name'].split()
        visit_number = vn[0]
        day_number = vn[1]
        visit_description = vn[2]
        if len(vn) == 4:
            visit_description = vn[2] + ' ' + vn[3]  # space in between!
        collection_date = i['Collection Date']
        collection_time = i['Collection Time']
        to_db.append((
            protocol_name, accession_number, patient_number, site_number, visit_type, visit_number, day_number,
            visit_description, collection_date, collection_time, roster_base_filename, roster_rcvd_date,
            ep_number, color, roster_sent_date))

    col0 = 'id'
    col1 = 'protocol_name'
    col2 = 'accession_number'
    col3 = 'patient_number'
    col4 = 'site_number'
    col5 = 'visit_type'
    col6 = 'visit_number'
    col7 = 'day_number'
    col8 = 'visit_description'
    col9 = 'collection_date'
    col10 = 'collection_time'
    col11 = 'roster_base_filename'
    col12 = 'roster_rcvd_date'
    col13 = 'ep_number'
    col14 = 'color'
    col15 = 'roster_sent_date'
    col16 = 'mismatch'

    try:
        db = sqlite3.connect(db_filename)

        # first check if the file has been processed before.
        cur = db.cursor()
        cur.execute(
            """SELECT name FROM sqlite_master WHERE type='table' AND name='roster_table';""")
        data = cur.fetchone()
        if data:
            # roster_table exists!
            cur.execute(
                'SELECT count(*) FROM roster_table WHERE roster_base_filename = ?', (roster_base_filename,))
            data = cur.fetchone()[0]
            if data > 0:
                log.info(
                    'This roster file has been processed before. Not importing.')
                db.close()
                return

        # inserting the csv file
        cur.execute(
            """CREATE TABLE IF NOT EXISTS roster_table ({} INTEGER PRIMARY KEY AUTOINCREMENT,
            {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
            );""".format(
                col0,
                col1,
                col2,
                col3,
                col4,
                col5,
                col6,
                col7,
                col8,
                col9,
                col10,
                col11,
                col12,
                col13,
                col14,
                col15,
                col16))

        cur.executemany("""INSERT INTO roster_table (
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} ) VALUES (
                    ?, ?, ? ,?, ?, ?, ? ,?,  ?, ?, ? ,?,  ?, ?, ?);""".format(
            col1,
            col2,
            col3,
            col4,
            col5,
            col6,
            col7,
            col8,
            col9,
            col10,
            col11,
            col12,
            col13,
            col14,
            col15,
            col16), to_db)

        # finish
        db.commit()

    except Exception as e:
        # Roll back any change if something goes wrong
        db.rollback()
        raise e

    finally:
        # Close the db connection
        db.close()


def print_rows(db_filename):
    try:
        db = sqlite3.connect(db_filename)
        cur = db.cursor()
        cur.execute('SELECT * FROM roster_table')
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        db.rollback()
        raise e

    finally:
        # Close the db connection
        db.close()


def match_tables(db_filename):
    log.info('Beginning match_tables procedure...')
    try:
        db = sqlite3.connect(db_filename)
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute('SELECT * FROM roster_table')
        roster_rows = cur.fetchall()
        for roster_row in roster_rows:
            # matching

            cur.execute(
                """SELECT * FROM ctp_table WHERE visit_number = ?
                and day_number = ? and visit_description = ? and color = ?""",
                (roster_row['visit_number'], roster_row['day_number'], roster_row['visit_description'],
                 roster_row['color'],))
            ctp_rows = cur.fetchall()
            if not ctp_rows:
                log.error(
                    'Sample mismatch with CTP table found! Flagging the roster table.')
                # marking as mismatch
                cur.execute('UPDATE roster_table SET {} = ? WHERE id = ?;'.format('mismatch'),
                            (1, roster_row['id']))
                # jump to next row
                continue

            elif len(ctp_rows) > 1:
                raise Exception(
                    'Multiple matches in CTP for one possible combination! CTP file has non-unique entries.')
            else:
                # get the rowid of the corresponding row in the CTP table
                ctp_rowid = ctp_rows[0]['id']
                entry_dupe = 0
                # we dont know how many duplicates there will be, so go for ever.
                while True:
                    col_name = 'z{}_m{}'.format(
                        roster_row['patient_number'], entry_dupe)
                    try:
                        log.info('Try to make a new column')
                        cur.execute(
                            """ALTER TABLE ctp_table ADD COLUMN {}""".format(col_name))
                    except:
                        # log.info('column already exists, never mind!')
                        pass
                    cur.execute(
                        'SELECT * from ctp_table WHERE id = ?;', (ctp_rowid,))
                    ddd = cur.fetchone()
                    if not ddd[col_name]:
                        log.info(
                            'Column exists and field is empty so fill it up.')
                        cur.execute('UPDATE ctp_table SET {} = ? WHERE id = ?;'.format(col_name),
                                    (roster_row['id'], ctp_rowid))
                        break
                    else:
                        log.info(
                            'Column exists but field is full, then we need a new column.')
                        entry_dupe += 1

        db.commit()

    except Exception as e:  # Roll back any change if something goes wrong
        db.rollback()
        raise e

    finally:  # Close the db connection
        db.close()


def read_sample_type_csv(db_filename, csv_filename):
    col0 = 'id'
    col1 = 'sample_type'
    col2 = 'color'

    with open(csv_filename, 'r') as f:
        # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(f)  # comma is default delimiter
        to_db = [(i['{}'.format(col1)].strip().upper(),
                  i['{}'.format(col2)].strip().upper()) for i in dr]

    try:
        db = sqlite3.connect(db_filename)
        cur = db.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS sample_type_table ({} INTEGER PRIMARY KEY AUTOINCREMENT, {}, {});""".format(
                col0,
                col1,
                col2))

        cur.executemany("INSERT INTO sample_type_table ({}, {}) VALUES (?, ?);".format(
            col1, col2), to_db)
        db.commit()

    except Exception as e:
        # Roll back any change if something goes wrong
        db.rollback()
        raise e

    finally:
        # Close the db connection
        db.close()


def check_db(db_filename):
    if not os.path.exists(db_filename):
        return False

    # check if the database has a ctp_table
    db = sqlite3.connect(db_filename)
    cur = db.cursor()
    cur.execute(
        """SELECT name FROM sqlite_master WHERE type='table' AND name='ctp_table';""")
    data = cur.fetchone()
    if not data:
        log.info('Database file does not conrain a ctp table.')
        return False
    return True


def main():
    parser = argparse.ArgumentParser(prog='samon')
    parser.add_argument('dbfilename', nargs=1, type=str,
                        help='Name of the database file.')
    parser.add_argument('--roster', nargs='+', type=str,
                        help='Input data files.')
    parser.add_argument('--version', action='version', version=__version__)
    parser.add_argument('--create', nargs=2, type=str,
                        help='Names of the files.')
    parser.add_argument('--match', action='store_true', help='Start matching.')
    parser.add_argument('--verbose', action='store_true',
                        help='Increase verbosity.')

    args = parser.parse_args()
    # check the first switches

    if args.verbose:
        log.basicConfig(level=log.DEBUG)

    db_filename = args.dbfilename[0]
    if not db_filename.lower().endswith('.sqlite'):
        db_filename += '.sqlite'

    log.info('Database file name: {}'.format(db_filename))

    # checking new database has prio
    if args.create:
        log.info('Creating a new study database...')
        log.info('Processing ctp file: {}'.format(args.create[0]))
        read_ctp_csv(db_filename, args.create[0])
        log.info('Sample type file: {}'.format(args.create[1]))
        read_sample_type_csv(db_filename, args.create[1])
        sys.exit()

    elif args.roster:
        if check_db(db_filename):
            log.info('Importing new roster...')
            for i in args.roster:
                log.info('Processing roster: {}'.format(i))
                read_roster_csv(db_filename, i)
                # print(filename_sanitizer(i))
                # print(get_clean_rosterfiles(i))
            sys.exit()
        else:
            log.info('Please create a database first.')
    elif args.match:
        print('Matching...')
        match_tables(db_filename)
        sort_ctp_table(db_filename)
        sys.exit()
    else:
        log.info('Nothing to do. Aborting ...')


# ----------------------------

if __name__ == '__main__':
    main()
