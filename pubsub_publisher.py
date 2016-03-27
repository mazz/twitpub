# encoding=utf8
from __future__ import print_function
import zmq
import time
import sys
import random
from  multiprocessing import Process
import msgpack
import binascii
import tweepy
from httplib import IncompleteRead
import re
import requests
import lxml
from lxml import html
import argparse
import logging
FORMAT = "%(asctime)s %(levelname)-5.5s [%(name)s][%(threadName)s] %(message)s"
logging.basicConfig(format=FORMAT)
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# from http://eon01.com/blog/extract-a-hashtag-the-regex/
tag_regex = re.compile("(?:^|\s)\s*(#[A-Za-z][A-Za-z0-9-_]+)",re.MULTILINE)
mention_regex = re.compile("(?:^|\s)\s*(@[A-Za-z_][A-Za-z0-9-_]+)",re.MULTILINE)

k_url_regex = r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"""

class TwitterListener(tweepy.StreamListener):
    def __init__(self, api, socket):
        self.api = api
        self.socket = socket
        super(tweepy.StreamListener, self).__init__()

    def on_status(self, status):
        data = {}
        data['text'] = status.text
        data['created_at'] = time.mktime(status.created_at.timetuple())
        data['geo'] = status.geo
        data['source'] = status.source
        data['screen_name'] = status._json['user']['screen_name']

        match_tags = [s.replace('#', '') for s in re.findall(tag_regex, status.text)]
        match_mentions = [s.replace('@', '') for s in re.findall(mention_regex, status.text)]
        match_urls = re.findall(k_url_regex, status.text)

        links = []
        link = {}
        """fetch_response = None
        for url in match_urls:
            try:
                fetch_response = requests.get(url)
            except requests.exceptions.ConnectionError as e:
                print(e)
            except requests.exceptions.ConnectTimeout as e:
                print(e)
            except requests.exceptions.ReadTimeout as e:
                print(e)
                """
            
        """if fetch_response is not None:
            parsed_body = html.fromstring(fetch_response.content)
            title = parsed_body.xpath('//title/text()')
            LOG.info('parsed_body.xpath ' + repr(title))

            link['title'] = title
            link['url'] = fetch_response.url

            links.append(link)
            print('links: %s' % repr(link))

            if len(links) > 0:
                send_dict['links'] = links       
                """
        send_dict = {}
        send_dict['tweet'] = data
        
        if len(match_urls) > 0:
            send_dict['match_urls'] = match_urls       
        
        if len(match_tags) > 0:
            send_dict['match_tags'] = match_tags       

        if len(match_mentions) > 0:
            send_dict['match_mentions'] = match_mentions       

        if len(send_dict.keys()) > 0:            
            LOG.info('send_dict: {0}'.format(repr(send_dict)))
            msg = msgpack.packb(send_dict)
    #         print('send unpacked message: %s' % repr(send_dict))
            LOG.info('send packed message hex: %s' % binascii.hexlify(msg))
            self.socket.send(msg)

    def on_error(self, status_code):
        LOG.info('Encountered error with status code: %s'.format(repr(status_code)))
        return True  # Don't kill the stream

    def on_timeout(self):
        LOG.info('Timeout...')
        return True  # Don't kill the stream

    def on_disconnect(self, notice):
        LOG.info('Encountered disconnect with notice: %s'.format(repr(notice)))
        """Called when twitter sends a disconnect notice
            
            Disconnect codes are listed here:
            https://dev.twitter.com/docs/streaming-apis/messages#Disconnect_messages_disconnect
            """
        return

def main():
    parser = argparse.ArgumentParser(prog='twitpub', description='publishes on a queue tweets in real-time filtered with a follow and/or track list. Also processes each tweet for http links')
    parser.add_argument('consumer_key', help='Your twitter consumer key')
    parser.add_argument('consumer_secret', help='Your twitter consumer secret')
    parser.add_argument('access_token', help='Your twitter access token')
    parser.add_argument('access_token_secret', help='Your twitter access token secret')
    parser.add_argument('--follow', nargs='*', action="store", dest="follow_list")
    parser.add_argument('--track', nargs='*', action="store", dest="track_list")

    args = parser.parse_args()

    if (args.follow_list is None or len(args.follow_list) == 0) and (args.track_list is None or len(args.track_list) == 0):
        LOG.info('You need to follow at least one id or track at least one keyword')
        sys.exit()

    ## initialize zeromq
    #
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://localhost:%s" % '5559')

    authdict = {
                'consumer_key': args.consumer_key,
                'consumer_secret': args.consumer_secret,
                'access_token': args.access_token,
                'access_token_secret': args.access_token_secret
    }

    while True:
        try:
            sapi = None
            auth = tweepy.OAuthHandler(authdict['consumer_key'], authdict['consumer_secret'])
            auth.set_access_token(authdict['access_token'], authdict['access_token_secret'])
#             api = tweepy.API(auth)
            api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
            sapi = tweepy.streaming.Stream(auth, TwitterListener(api, socket))
            sapi.filter(follow=args.follow_list, track=args.track_list, stall_warnings=True)
        except IncompleteRead:
            pass
        except requests.exceptions.MissingSchema:
            pass
        except IOError, ex:
            LOG.info('tweepy exception: %s' % ex)
            pass
        except lxml.etree.XMLSyntaxError:
            pass
        except tweepy.error.TweepError, ex:
            LOG.info('tweepy exception: %s' % ex)
            pass
        except IncompleteRead:
            # Oh well, reconnect and keep trucking
            continue
        except requests.packages.urllib3.exceptions.ProtocolError:
            continue
#             sapi.disconnect()
        except KeyboardInterrupt:
            # Or however you want to exit this loop
            sapi.disconnect()
            break

if __name__ == "__main__":
    main()