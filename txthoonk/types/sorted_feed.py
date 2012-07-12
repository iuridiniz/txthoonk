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

    def publish(self, item):
        redis = self.pub.redis

        def _check_exec(multi_result, id_):
            if multi_result:
                # Transaction done :D
                # assert number commands in transaction
                assert len(multi_result) == 5
                return id_

            # Transaction fail :(
            # repeat it
            return self.publish(item)

        def _got_id(id_):
            # begin transaction
            id_ = str(id_)
            d = redis.multi()
            d.addCallback(lambda x: redis.rpush(self.feed_ids, id_)) #0
            d.addCallback(lambda x: redis.hset(self.feed_items, id_, item)) #1
            d.addCallback(lambda x: redis.incr(self.feed_publishes)) #2
            d.addCallback(lambda x: \
                            self.pub.publish_channel(self.channel_publish,
                                                     id_, item)) #3
            d.addCallback(lambda x: \
                            self.pub.publish_channel(self.channel_position,
                                                     id_, ":end")) #4
            d.addCallback(lambda x: redis.execute())
            return d.addCallback(_check_exec, id_)


        return redis.incr(self.feed_id_incr).addCallback(_got_id)
