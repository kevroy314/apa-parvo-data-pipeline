import logging
import os
import time
import re

settings = {
    'directory': 'data',
    'input_filename': 'blacklist.txt',
    'concurrency': None,  # This script often runs faster w/o concurrency due the database and proc requirements
    'log_level': 'INFO'
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

for anum in a_nums:
    anum_file_string = 'A{0}'.format(anum)
    file_path = os.path.join(os.path.dirname(__file__), settings['directory'], anum_file_string + '.htm').replace("/", "\\")
    if not os.path.exists(file_path):
        logging.info("{0} skipped because file does not exist.".format(anum))
        continue
    os.system("start " + file_path)
    i = raw_input()
    if i == 'q':
        break
