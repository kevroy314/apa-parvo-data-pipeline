import Queue
import logging
import os
import re
import time
import traceback
# noinspection PyUnresolvedReferences
import _strptime
from datetime import datetime, timedelta
from threading import Thread, Event

import numpy as np
import pandas as pd
from lxml import html
from pandas import DataFrame
from tinydb import *

os.remove('db.json')

db = TinyDB('db.json')

settings = {
            'directory': 'C:\\Users\\Kevin\\PycharmProjects\\ParvoCrawler\\data',
            'input_filename': 'C:\\Users\\Kevin\\PycharmProjects\\ParvoCrawler\\parvo.txt',
            'concurrency': 20,
            'log_level': 'DEBUG'
           }
###########################
# Setup Logging
###########################
if 'log_level' in settings:
    level = settings['log_level'].strip()
else:
    level = 'WARNING'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s, %(levelname)s: %(message)s',
                    filename='{0}.log'.format(time.strftime("%Y_%m_%d-%I_%M_%S")))
# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.getLevelName(level))
# tell the handler to use this format
console.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s: %(message)s'))
# add the handler to the root logger
logging.getLogger('').addHandler(console)

if not ('input_filename' in settings and os.path.exists(settings['input_filename'])):
    logging.error('input_filename not found.')
    exit()
# Read the input file
f = open(settings['input_filename'], 'r')
lines = f.readlines()
f.close()
a_nums = []
try:
    a_nums = [str(line[1:].strip()) for line in lines]
except Exception as e:
    logging.error('There was an error parsing the input file. '
                  'Ensure it is a proper format (newline separated A# list).')
    logging.debug(e)
if len(a_nums) == 0:
    logging.error('No ANums provided. Exiting.')
    exit()

pd.set_option("display.max_columns", 999)
pd.set_option("display.max_colwidth", 999)
pd.set_option('expand_frame_repr', False)
a_num_regex = re.compile('A\d\d\d\d\d\d\d\d')


def get_array_from_table(table, remove_empties=True):
    frame = []
    rows = table.cssselect("tr")
    for row in rows:
        frame.append(list())
        for td in row.cssselect("td"):
            contents = unicode(td.text_content().strip())
            if not remove_empties or contents:
                frame[-1].append(contents)
    return frame


def process_anum(a_number):
    file_path = os.path.join(settings['directory'], 'A' + a_number + '.htm')
    if not os.path.exists(file_path):
        return
    with open(file_path, 'r') as file_ptr:
        result = file_ptr.read()
    tree = html.fromstring(result)
    tables = tree.findall('.//table')
    # Table 1
    frame = get_array_from_table(tables[1])
    # Validate Shape
    if np.array(frame).shape != (2, 1):
        logging.debug('\n'+str(DataFrame(frame)))
    # Parse Elements
    a = frame[0][0].split(':')[1].strip()
    d = ':'.join(frame[1][0].split(':')[1:]).strip()
    # Validate Elements
    if a_num_regex.search(a) is None:
        logging.debug(a)
    try:
        print_date = datetime.strptime(d, '%m/%d/%Y %I:%M%p')
    except ValueError:
        print_date = None
        logging.debug(d)
    # Table 2
    frame = get_array_from_table(tables[2], remove_empties=False)
    # Validate Shape
    if np.array(frame).shape != (4L, 3L):
        logging.debug('\n' + str(DataFrame(frame)))
    # Parse Elements
    # noinspection PyBroadException
    a2 = frame[0][0].strip()
    a3 = frame[1][0].strip()
    gender = frame[2][1].strip()
    age_range = frame[3][1].strip()
    name = frame[0][1].strip()
    declawed = frame[2][2].strip().replace('Declawed:', '').strip()
    bite_history = frame[3][2].strip().replace('Bitten:', '').strip()
    physical_attributes = [att.strip() for att in frame[0][2].strip().split(',')]
    species = frame[1][1].strip()
    ads = [att.strip() for att in frame[1][2].strip().split(',')]
    if 'dob' not in frame[1][2].strip().lower():
        age = 'unk'
        dob = 'unk'
        spay_neuter = ads[0].replace('Spayed/Neutered:', '').strip()
    else:
        age = ads[0]
        dob = ads[1].replace('DOB:', '').strip()
        spay_neuter = ads[2].replace('Spayed/Neutered:', '').strip()
    # Validate Elements
    if a_num_regex.search(a2) is None:
        logging.debug('Bad Table 2 ANum {0}.'.format(a2))
    if not (gender.lower() == 'male' or gender.lower() == 'female' or gender.lower() == 'unknown'):
        logging.debug('Bad Table 2 Gender {0}.'.format(gender))
    if not (species.lower() == 'dog' or species.lower() == 'cat'):
        logging.debug('Bad Table 2 species {0}.'.format(species))
    try:
        if dob is not 'unk':
            dob = datetime.strptime(dob, '%m/%d/%Y')
    except ValueError:
        logging.debug('Bad DOB {0}.'.format(dob))
    if not (spay_neuter.lower() == 'yes' or spay_neuter.lower() == 'no', spay_neuter == 'unknown'):
        logging.debug('Bad Spay/Neuter {0}.'.format(spay_neuter))
    # Table 3
    # TODO: Continue table parsing... at least get the other important tables.
    # Send to DB
    if a and print_date:
        try:
            database_queue.put({'anum': a, 'confirmation_anum': a2, 'alt_anum': a3, 'print_date': print_date,
                                'gender': gender, 'age_range': age_range, 'name': name, 'declawed': declawed,
                                'bite_history': bite_history, 'physical_attributes': physical_attributes, 'age': age,
                                'dob': dob, 'spay_neuter': spay_neuter})
        except ValueError:
            logging.debug('Could not send {0} to DB.'.format({'anum': a, 'print_date': print_date}))


def process_next_anum():
    global q
    while True:
        next_a = None
        # noinspection PyBroadException
        try:
            next_a = q.get(True, 0)
            process_anum(next_a)
        except Queue.Empty:
            logging.debug('Closing thread gracefully after empty queue.')
            exit()
        except Exception:
            if next_a is not None:
                logging.error('There was an error processing the anum, {0}.'.format(next_a))
            else:
                logging.error('There was an error retreiving the next a number.')
            logging.error(traceback.print_exc())
            pass


def database_writer(database, queue, stop_signal):
    while not stop_signal.is_set():
        iters = queue.qsize()
        for index in range(iters):
            element = queue.get()
            database.insert(element)
    logging.info('Database writer received signal to end.')
    iters = queue.qsize()
    for index in range(iters):
        if index % 100 == 0:
            logging.info('{0} records left to write.'.format(iters-index))
        element = queue.get()
        database.insert(element)
    logging.info('Database writer exiting...')
    exit()

###########################
# Begin Parse
###########################
# Begin time logging
start_time = time.time()

logging.info('Initializing...')

if 'concurrency' in settings:
    logging.info('Setting up concurrency with {0} thread(s).'.format(settings['concurrency']))
    database_writer_stop_signal = Event()
    database_queue = Queue.Queue()
    db_thread = Thread(target=database_writer, args=(db, database_queue, database_writer_stop_signal))
    db_thread.start()
    # Construct Queue
    q = Queue.Queue(len(a_nums))
    for anum in a_nums:
        q.put(anum)
    # Spawn Threads
    thread_pool = []
    for i in range(int(settings['concurrency'])):
        t = Thread(target=process_next_anum)
        thread_pool.append(t)
        t.start()
    # Wait for Queue to complete
    prev_print = ''
    while any([t.isAlive() for t in thread_pool]):
        time.sleep(1)
        next_print = '{0}/{1}'.format(q.qsize(), len(a_nums))
        if prev_print != next_print:
            logging.info(next_print)
            prev_print = next_print
        else:
            logging.debug('.')
    database_writer_stop_signal.set()
    logging.info('All threads completed.')
    logging.info('Signaling database to wrap up writing...')
    while db_thread.isAlive():
        time.sleep(1)
else:
    logging.info('Running in serial mode.')
    # Parse all anums sequentially
    for idx, anum in enumerate(a_nums):
        if idx % 100 == 0:
            logging.info("{0}/{1}".format(idx, len(a_nums)))
        process_anum(anum)

num_files = sum(os.path.isfile(os.path.join(settings['directory'], f)) for f in os.listdir(settings['directory']))
logging.info('Process completed in {0}. {1} files saved out of {2} requested.'.format(
    str(timedelta(seconds=time.time()-start_time)), num_files, len(a_nums)))
