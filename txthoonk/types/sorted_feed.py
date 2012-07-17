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

    def _publish(self, item, where):
        assert where in (":end", ":begin")

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

            if where == ":end":
                push = redis.rpush
            elif where == ":begin":
                push = redis.lpush

            d.addCallback(lambda x: push(self.feed_ids, id_)) #0
            d.addCallback(lambda x: redis.hset(self.feed_items, id_, item)) #1
            d.addCallback(lambda x: redis.incr(self.feed_publishes)) #2
            d.addCallback(lambda x: \
                            self.pub.publish_channel(self.channel_publish,
                                                     id_, item)) #3
            d.addCallback(lambda x: \
                            self.pub.publish_channel(self.channel_position,
                                                     id_, where)) #4
            d.addCallback(lambda x: redis.execute())
            return d.addCallback(_check_exec, id_)

        return redis.incr(self.feed_id_incr).addCallback(_got_id)

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

    def edit(self, id_, item):
        redis = self.pub.redis
        def _check_exec(multi_result):
            if multi_result:
                # Transaction done :D
                # assert number commands in transaction
                assert len(multi_result) == 3
                return id_

            # Transaction fail :(
            # repeat it
            return self.publish(item)

        def _has_id(has_id):
            if not has_id:
                d = redis.unwatch()
                d.addCallback(lambda x: None)
                return d

            d = redis.multi()
            d.addCallback(lambda x: redis.hset(self.feed_items, id_, item)) #0
            d.addCallback(lambda x: redis.incr(self.feed_publishes)) #1
            d.addCallback(lambda x: \
                self.pub.publish_channel(self.channel_publish, id_, item)) #2

            d.addCallback(lambda x: redis.execute())
            d.addCallback(_check_exec)
            return d

        d = redis.watch(self.feed_items)
        d.addCallback(lambda x: self.has_id(id_))

        return d.addCallback(_has_id)

    def publish(self, item):
        return self.append(item)

    def append(self, item):
        return self._publish(item, where=":end")

    def prepend(self, item):
        return self._publish(item, where=":begin")

    def has_id(self, id_):
        return self.pub.redis.hexists(self.feed_items, id_)

    def get_ids(self):
        return self.pub.redis.lrange(self.feed_ids, 0, -1)

    def get_items(self):
        return self.pub.redis.hgetall(self.feed_items)

    def get_all(self):
        return self.get_items()

    def get_item(self, id_):
        return self.pub.redis.hget(self.feed_items, id_)

    def get_id(self, id_):
        return self.get_item(id_)
