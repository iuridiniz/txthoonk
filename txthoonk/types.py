'''
Created on Jul 6, 2012

@author: iuri
'''
from twisted.internet import defer
import uuid
import time

class Feed(object):
    '''
    classdocs
    '''
    def __init__(self, pub, name):
        '''
        Constructor
        '''
        self.pub = pub
        self.name = name

        self.feed_ids = 'feed.ids:%s' % name
        self.feed_items = 'feed.items:%s' % name
        self.feed_publishes = 'feed.publishes:%s' % name
        self.feed_config = 'feed.config:%s' % name

        self.channel_retract = 'feed.retract:%s' % name
        self.channel_edit = 'feed.edit:%s' % name
        self.channel_publish = 'feed.publish:%s' % name

    def get_config(self):
        return self.pub.get_config(self.name)

    def set_config(self, conf):
        return self.pub.set_config(self.name, conf)

    def publish(self, item, id_=uuid.uuid4().hex):
        pub = self.pub
        redis = pub.redis

        def _check_exec(bulk_result):
            # All defers must be succeed
            assert all([a[0] for a in bulk_result])

            multi_result = bulk_result[-1][1]
            if multi_result:
                # Transaction done :D
                # assert number commands in transaction
                assert len(multi_result) >= 3
                # check if id_ existed when added
                non_exists = multi_result[-1]
                if non_exists:
                    d = pub.publish_channel(self.channel_publish, id_, item)
                else:
                    d = pub.publish_channel(self.channel_edit, id_, item)

                # return the id
                d.addCallback(lambda x: id_)
                return d

            # Transaction fail :(
            # repeat it
            return self.publish(item, id_)

        def _do_publish(delete_ids, has_id):
            defers = []

            # begin transaction
            defers += [redis.multi()]

            # delete ids
            # id is already on feed, we don't need to delete one
            if has_id and len(delete_ids) > 0:
                try:
                    # try to choose itself if marked to be deleted
                    delete_ids.remove(id_)
                except ValueError:
                    # else remove the last
                    delete_ids.pop()

            for i in delete_ids:
                defers += [redis.zrem(self.feed_ids, i)]
                defers += [redis.hdel(self.feed_items, i)]
                defers += [pub.publish_channel(self.channel_retract, i)]

            defers += [redis.incr(self.feed_publishes)] # -3
            defers += [redis.hset(self.feed_items, id_, item)] # -2
            defers += [redis.zadd(self.feed_ids, id_, time.time())] # -1

            defers += [redis.execute()]

            return defer.DeferredList(defers).addCallback(_check_exec)

        def _got_config(bulk_result):
            # All defers must be succeed
            assert all([a[0] for a in bulk_result])
            # assert number of commands
            assert len(bulk_result) == 5

            has_id = bulk_result[-2][1]
            config = bulk_result[-1][1]
            max_ = config.get("max_length")
            if max_ is not None and max_.isdigit():
                max_ = int(max_)
                # get ids to be deleted
                d = redis.zrange(self.feed_ids, 0, -(max_))
            else:
                # no ids to be deleted
                d = defer.succeed([])

            return d.addCallback(_do_publish, has_id)

        defers = []
        defers.append(redis.watch(self.feed_config)) #0
        defers.append(redis.watch(self.feed_ids)) #1
        defers.append(redis.watch(self.feed_items)) #2
        defers.append(self.has_id(id_)) #3
        defers.append(self.get_config()) #4
        return defer.DeferredList(defers).addCallback(_got_config)

    def get_item(self, id_):
        return self.pub.redis.hget(self.feed_items, id_)
    get_id = get_item

    def has_id(self, id_):
        # ZRank has complexity O(log(n))
        #d = self.pub.redis.zrank(self.feed_ids, id_)
        #d.addCallback(lambda i: i is not None)

        # HExists has complexity O(1)
        d = self.pub.redis.hexists(self.feed_items, id_)
        return d

    def get_ids(self):
        return self.pub.redis.zrange(self.feed_ids, 0, -1)

    def get_all(self):
        return self.pub.redis.hgetall(self.feed_items)
