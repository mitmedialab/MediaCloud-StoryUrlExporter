import logging
import os
import csv
import sys
import datetime as dt
from dotenv import load_dotenv
import mediacloud.api

load_dotenv()

OUTPUT_DIR = "output"
INCLUDE_MEDIA_METADATA = False

base_dir = os.path.dirname(os.path.abspath(__file__))   # so nothing is relative paths

logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
logger.info("----------------------------------------------------------------------------")
#sys.exit()

# load settings
mc = mediacloud.api.AdminMediaCloud(os.environ.get('MC_API_KEY'))
stories_per_fetch = int(os.environ.get('MC_STORIES_PER_PAGE'))
q = "(((mandatory and rehab*) or (civil* and commit*) or (coerc* and treat*) or (involuntar* and treat*) or (involuntar* and commit*) or (custod*)) and (opioid* or benzo* or alcohol or meth* or stimulant* or cocaine or substance* or drug* or addict*)) and tags_id_media:(34412234 38379429)"
fq = mc.dates_as_query_clause(dt.date(2017, 1, 1), dt.date(2020, 10, 1))
logger.info("Query: "+q)
logger.info("Fq: "+fq)
filename = 'hij'

media_cache = {}    # so we can build a lazy cache of media to get media_tags

# setup export file so we can write as we fetch
timestamp = dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')
output_file_path = os.path.join(base_dir, OUTPUT_DIR, filename+'-'+timestamp+'.csv')
logger.info("Writing output to "+output_file_path)
f = open(output_file_path, 'w')
csv_writer = csv.writer(f)
col_headers = ['stories_id', 'publish_data', 'url', 'title', 'language', 'ap_syndicated',
               'media_id', 'media_name', 'media_url']
if INCLUDE_MEDIA_METADATA:
    col_headers.append('media_tags')
csv_writer.writerow(col_headers)

# figure out how many total stories there are so we can output status well
story_count = mc.storyCount(q)['count']
logger.info("Total of {} stories to fetch".format(story_count))
page_count = 1 + story_count / stories_per_fetch
logger.info("  At {} per fetch that'll be {} pages".format(stories_per_fetch, page_count))

# page through stories using last_processed_stories_id value
last_processed_stories_id = 0
more_stories = True
page = 0
while more_stories:
    logger.info("Page {} of {}".format(page, page_count))
    stories = mc.storyList(q, last_processed_stories_id=last_processed_stories_id, rows=stories_per_fetch)
    more_stories = len(stories) > 0
    if len(stories) > 0:
        last_processed_stories_id = stories[-1]['processed_stories_id']
    for story in stories:
        if INCLUDE_MEDIA_METADATA:
            # grab media tags (building cache as we go)
            media = None
            if story['media_id'] not in media_cache:
                media_cache[story['media_id']] = mc.media(story['media_id'])
            media = media_cache[story['media_id']]
            media_collection_tags = [str(t['tags_id']) for t in media['media_source_tags'] if t['tag_sets_id'] == 5]
        # write info to csv
        story_row = [
            story['stories_id'],
            story['publish_date'],
            story['url'],
            story['title'],
            story['language'],
            story['ap_syndicated'],
            story['media_id'],
            story['media_name'],
            story['media_url'],
        ]
        if INCLUDE_MEDIA_METADATA:
            story_row.append(" ".join(media_collection_tags))
        csv_writer.writerow(story_row)
    page += 1

log.info("Done!")
