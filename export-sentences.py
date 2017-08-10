import ConfigParser
import logging
import logging.config
import re
import os
import json
import codecs
import datetime

import mediacloud

OUTPUT_DIR = "output"
base_dir = os.path.dirname(os.path.abspath(__file__))   # so nothing is relative paths

# set up logger
with open(os.path.join(base_dir,'logging.json'), 'r') as f:
    logging_config = json.load(f)
logging.config.dictConfig(logging_config)
log = logging.getLogger(__name__)
log.info("----------------------------------------------------------------------------")

# load config from file
config_file_path = os.path.join('settings.config')
settings = ConfigParser.ConfigParser()
settings.read(config_file_path)

# load settings
mc = mediacloud.api.AdminMediaCloud(settings.get('mediacloud', 'key'))
stories_per_fetch = int(settings.get('mediacloud', 'stories_per_fetch'))
query = settings.get('export', 'query')
filename = settings.get('export', 'filename')
log.info("Query: "+query)

media_cache = {}    # so we can build a lazy cache of media to get media_tags

# setup export file so we can write as we fetch
timestamp = datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
output_file_path = os.path.join(base_dir, OUTPUT_DIR, filename+'-'+timestamp+'.txt')
log.info("Writing output to "+output_file_path)
f = codecs.open(output_file_path, mode='w', encoding='utf-8')

# figure out how many total stories there are so we can output status well
story_count = mc.storyCount(query)['count']
log.info("Total of {} stories to fetch".format(story_count))
page_count = 1 + story_count / stories_per_fetch
log.info("  At {} per fetch that'll be {} pages".format(stories_per_fetch, page_count))

# page through stories using last_processed_stories_id value
last_processed_stories_id = 0
more_stories = True
page = 0
while more_stories:
    log.info("Page {} of {}".format(page, page_count))
    stories = mc.storyList(query, last_processed_stories_id=last_processed_stories_id, rows=stories_per_fetch,
                           sentences=True)
    more_stories = len(stories) > 0
    if len(stories) > 0:
        last_processed_stories_id = stories[-1]['processed_stories_id']
    for story in stories:
        for sentence in story['story_sentences']:
            f.write(re.sub(r'[^\w\s]', '', sentence['sentence']))
            f.write("\n")
    page += 1

log.info("Done!")
