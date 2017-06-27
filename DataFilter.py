import logging
import os
import time
import re

settings = {
    'directory': 'data',
    'input_filename': 'parvo.txt',
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

parvo_test_regex = re.compile("<td>\s*Parvo Test \(IDEXX\)</td><td>.*</td><td>Positive</td>")

whitelist = open(os.path.join(os.path.dirname(__file__), 'whitelist.txt'), 'w')
blacklist = open(os.path.join(os.path.dirname(__file__), 'blacklist.txt'), 'w')
missing = open(os.path.join(os.path.dirname(__file__), 'missing.txt'), 'w')

for anum in a_nums:
    anum_file_string = 'A{0}'.format(anum)
    file_path = os.path.join(os.path.dirname(__file__), settings['directory'], anum_file_string + '.htm').replace("/", "\\")
    if not os.path.exists(file_path):
        logging.info("{0} skipped because file does not exist.".format(anum))
        missing.write(anum_file_string + "\n")
        continue
    with open(file_path, 'r') as f:
        text = f.read()
        search_text = text.lower()
        is_whitelist = True
        if 'Parvo-Dog'.lower() in search_text or 'Parvo Ward'.lower() in search_text:
            logging.info('{0} whitelisted for containing containing non-case-sensitive location tags.'.format(anum))
        elif 'parvo treatment' in search_text:
            logging.info('{0} whitelisted for containing \'parvo treatment\' non-case-sensitive.'.format(anum))
        elif bool(parvo_test_regex.search(text)):
            logging.info('{0} whitelisted for containing parvo positive test regex, case-sensitive.'.format(anum))
        else:
            is_whitelist = False
            logging.info('{0} blacklisted for failing all tests.'.format(anum))
        if is_whitelist:
            whitelist.write(anum_file_string + "\n")
        else:
            blacklist.write(anum_file_string + "\n")

whitelist.close()
blacklist.close()
missing.close()