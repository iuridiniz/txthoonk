'''
Created on Jul 12, 2012

@author: iuri
'''
from txthoonk.types.base import FeedBaseType

class SortedFeed(FeedBaseType):
    '''
    classdocs
    '''

    def __init__(self, pub, name):
        '''
        Create a new Feed object for a given Thoonk feed name.

        @param pub: the ThoonkPub object
        @param name: the name of this feed
        '''
        super(SortedFeed, self).__init__(pub, name)

        self.feed_ids = 'feed.ids:%s' % self.name
        self.feed_items = 'feed.items:%s' % self.name
        self.feed_publishes = 'feed.publishes:%s' % self.name
        self.feed_config = 'feed.config:%s' % self.name
        self.feed_id_incr = 'feed.idincr:%s' % self.name

        self.channel_retract = 'feed.retract:%s' % self.name
        self.channel_position = 'feed.position:%s' % self.name
        self.channel_publish = 'feed.publish:%s' % self.name

    def get_schema(self):
        '''
        Get the redis keys used by this feed.
        '''
        return [self.feed_ids, self.feed_items,
                self.feed_publishes, self.feed_config, self.feed_id_incr]

    def get_channels(self):
        '''
        Get the redis channels used by this feed.
        '''
        return [self.channel_retract, self.channel_position,
                self.channel_publish]
