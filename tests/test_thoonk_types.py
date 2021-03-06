'''
Created on Jul 6, 2012

@author: iuri
'''
from tests.test_thoonk_pubsub import TestThoonkBase
from twisted.internet import defer


class TestThoonkFeedBaseType(TestThoonkBase):
    @defer.inlineCallbacks
    def setUp(self):
        yield TestThoonkBase.setUp(self)
        self.feed_name = "test_feed_base_type"
        self.config = {}
        yield self.pub.create_feed(self.feed_name, self.config)

        from txthoonk.types.base import FeedBaseType
        self.feed = FeedBaseType(pub=self.pub, name=self.feed_name)

        # check properties
        self.assertEqual(self.pub, self.feed.pub)
        self.assertEqual(self.feed_name, self.feed.name)

    ############################################################################
    #  Tests for config
    ############################################################################
    @defer.inlineCallbacks
    def testFeedSetGetConfig(self):
        # get an existing config
        ret = yield self.feed.get_config()
        self.assertEqual(ret, self.config)

        # set a config value
        new_conf = {"max_length": '20'}
        yield self.feed.set_config(new_conf)

        self.config.update(new_conf)

        ret = yield self.feed.get_config()
        self.assertEqual(ret, self.config)


class TestThoonkFeed(TestThoonkBase):
    @defer.inlineCallbacks
    def setUp(self):
        yield TestThoonkBase.setUp(self)

        self.feed_name = "teste"
        self.config = {'type': 'feed'}
        yield self.pub.create_feed(self.feed_name, self.config)

        from txthoonk.types import Feed
        self.feed = Feed(pub=self.pub, name=self.feed_name)

        self.assertEqual(self.feed.feed_ids,
                         "feed.ids:%s" % self.feed_name)
        self.assertEqual(self.feed.feed_items,
                         "feed.items:%s" % self.feed_name)
        self.assertEqual(self.feed.feed_publishes,
                         "feed.publishes:%s" % self.feed_name)
        self.assertEqual(self.feed.feed_config,
                         "feed.config:%s" % self.feed_name)

        self.assertEqual(self.feed.channel_retract,
                         "feed.retract:%s" % self.feed_name)
        self.assertEqual(self.feed.channel_edit,
                         "feed.edit:%s" % self.feed_name)
        self.assertEqual(self.feed.channel_publish,
                         "feed.publish:%s" % self.feed_name)

    ############################################################################
    #  Tests for publish
    ############################################################################
    @defer.inlineCallbacks
    def testFeedPublish(self):
        item = "my beautiful item"
        feed = self.feed

        # no publishes (check on redis)
        n = yield self.pub.redis.get(feed.feed_publishes)
        self.assertFalse(n)

        id_ = yield feed.publish(item)

        # check on redis for new id
        ret = yield self.pub.redis.zrange(feed.feed_ids, 0, -1)
        self.assertEqual(ret, [id_])

        # check on redis for publishes increment
        n = yield self.pub.redis.get(feed.feed_publishes)
        self.assertEqual(n, '1')

        # check on redis for new item
        ret = yield self.pub.redis.hget(feed.feed_items, id_)
        self.assertEqual(ret[id_], item)

    @defer.inlineCallbacks
    def testFeedPublishWithMaxLength(self):
        import string

        items_01 = string.printable[0:20]
        ids_01 = map(str, range(0, 20))
        items_02 = string.printable[20:40]
        ids_02 = map(str, range(20, 40))

        feed = self.feed

        # set max_length
        feed.set_config({'max_length': len(ids_02)})

        # full it
        for id_, item in zip(ids_01, items_01):
            yield feed.publish(item, id_)

        ret = yield feed.get_ids()
        self.assertEqual(set(ret), set(ids_01))
        self.assertEqual(len(ret), len(ids_01))

        # replace all
        for id_, item in zip(ids_02, items_02):
            yield feed.publish(item, id_)

        ret = yield feed.get_ids()
        self.assertEqual(set(ret), set(ids_02))
        self.assertEqual(len(ret), len(ids_02))

        # replace somes ids_
        yield feed.publish(items_02[-1], ids_02[-1])
        ret = yield feed.get_ids()
        self.assertEqual(len(ret), len(ids_02))
        self.assertEqual(set(ret), set(ids_02))

        yield feed.publish(items_02[0], ids_02[0])
        ret = yield feed.get_ids()
        self.assertEqual(len(ret), len(ids_02))
        self.assertEqual(set(ret), set(ids_02))

        yield feed.publish(items_02[-10], ids_02[-10])
        ret = yield feed.get_ids()
        self.assertEqual(len(ret), len(ids_02))
        self.assertEqual(set(ret), set(ids_02))

        # set a new max_length
        yield feed.set_config({'max_length': '10'})

        # force a publish
        yield feed.publish("non")
        ret = yield feed.get_ids()
        self.assertEqual(len(ret), 10)

        # another publish
        yield feed.publish("none")
        ret = yield feed.get_ids()
        self.assertEqual(len(ret), 10)

    @defer.inlineCallbacks
    def testPublishEvent(self):
        item = "my beautiful item"
        id_ = "myid"
        feed = self.feed
        @self.check_called
        def onPublish(*args):
            ret_id = args[0]
            ret_item = args[1]
            self.assertEqual(ret_id, id_)
            self.assertEqual(ret_item, item)

        yield self.sub.register_handler(feed.channel_publish, onPublish)

        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        yield self.feed.publish(item, id_)
        yield cb

    @defer.inlineCallbacks
    def testPublishEventEdit(self):
        item_original = "my beautiful item_original"
        item_edited = "my beautiful item_edited"
        id_ = "myid"
        feed = self.feed
        @self.check_called
        def onEdit(*args):
            ret_id = args[0]
            ret_item = args[1]
            self.assertEqual(ret_id, id_)
            self.assertEqual(ret_item, item_edited)

        yield self.sub.register_handler(feed.channel_edit, onEdit)

        yield self.feed.publish(item_original, id_)

        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        yield self.feed.publish(item_edited, id_)
        yield cb

    @defer.inlineCallbacks
    def testPublishEventRetract(self):
        my_id = "myid"
        feed = self.feed

        @self.check_called
        def onRetract(*args):
            ret_id = args[0]
            self.assertEqual(ret_id, my_id)

        yield feed.set_config({"max_length": "1"})
        yield self.sub.register_handler(feed.channel_retract, onRetract)
        yield self.feed.publish("blaw", my_id)

        # publish a new item
        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        yield self.feed.publish("blew")
        yield cb

    ############################################################################
    #  Tests for has_id
    ############################################################################
    @defer.inlineCallbacks
    def testFeedHasId(self):
        item = "my beautiful item"
        id_ = "myid"
        feed = self.feed

        yield feed.publish(item, id_)

        ret = yield feed.has_id(id_)
        self.assertTrue(ret)

        # non existing item
        ret = yield feed.has_id(id_ + "123")
        self.assertFalse(ret)

    ############################################################################
    #  Tests for get*
    ############################################################################
    @defer.inlineCallbacks
    def testFeedGetItem(self):
        item = "my beautiful item"
        id_ = "myid"
        feed = self.feed

        yield feed.publish(item, id_)

        ret = yield feed.get_item(id_)
        self.assertEqual(ret, {id_: item})

        # non existing item
        ret = yield feed.get_item(id_ + "123")
        self.assertIsNone(ret)

    @defer.inlineCallbacks
    def testFeedGetIds(self):
        item = "my beautiful item"
        id_ = "myid"
        feed = self.feed

        yield feed.publish(item, id_)

        ret = yield feed.get_ids()
        self.assertEqual(ret, [id_])

    @defer.inlineCallbacks
    def testFeedGetAll(self):
        import string
        items = string.printable
        ids = map(str, range(0, len(items)))
        feed = self.feed
        for id_, item in zip(ids, items):
            yield feed.publish(item, id_)

        ret = yield feed.get_ids()
        self.assertEqual(set(ret), set(ids))

        ret = yield feed.get_all()
        ret_ids = ret.keys()
        ret_items = ret.values()
        self.assertEqual(set(ret_ids), set(ids))
        self.assertEqual(set(ret_items), set(items))

    ############################################################################
    #  Tests for retract
    ############################################################################
    @defer.inlineCallbacks
    def testFeedRetractAndRetractEvent(self):
        item = "my beautiful item"
        id_ = "myid"
        feed = self.feed

        @self.check_called
        def onRetract(*args):
            ret_id = args[0]
            self.assertEqual(ret_id, id_)

        yield self.sub.register_handler(feed.channel_retract, onRetract)
        yield self.feed.publish(item, id_)

        yield feed.publish(item, id_)

        ret = yield feed.has_id(id_)
        self.assertTrue(ret)

        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        yield feed.retract(id_)
        yield cb

        # non existing item
        ret = yield feed.has_id(id_)
        self.assertFalse(ret)


class TestThoonkSortedFeed(TestThoonkBase):
    @defer.inlineCallbacks
    def setUp(self):
        yield TestThoonkBase.setUp(self)

        self.feed_name = "test_sorted_feed"
        self.config = {'type': 'sorted_feed'}
        yield self.pub.create_feed(self.feed_name, self.config)

        from txthoonk.types import SortedFeed
        self.feed = SortedFeed(pub=self.pub, name=self.feed_name)

        self.assertEqual(self.feed.feed_ids,
                         "feed.ids:%s" % self.feed_name)
        self.assertEqual(self.feed.feed_items,
                         "feed.items:%s" % self.feed_name)
        self.assertEqual(self.feed.feed_publishes,
                         "feed.publishes:%s" % self.feed_name)
        self.assertEqual(self.feed.feed_config,
                         "feed.config:%s" % self.feed_name)
        self.assertEqual(self.feed.feed_id_incr,
                         "feed.idincr:%s" % self.feed_name)

        self.assertEqual(self.feed.channel_retract,
                         "feed.retract:%s" % self.feed_name)
        self.assertEqual(self.feed.channel_position,
                         "feed.position:%s" % self.feed_name)
        self.assertEqual(self.feed.channel_publish,
                         "feed.publish:%s" % self.feed_name)

    ############################################################################
    #  Tests for publish (append/prepend)
    ############################################################################
    @defer.inlineCallbacks
    def testFeedPublish(self):
        item = "my beautiful item"
        feed = self.feed

        # 0 publishes (check on redis)
        n = yield self.pub.redis.get(feed.feed_publishes)
        self.assertFalse(n)

        # 0 ids on counter (check on redis)
        n = yield self.pub.redis.get(feed.feed_id_incr)
        self.assertFalse(n)

        id_ = yield feed.publish(item)

        # check on redis for new id
        ret = yield self.pub.redis.lrange(feed.feed_ids, 0, -1)
        self.assertEqual(ret, [id_])

        # check on redis for publishes increment
        n = yield self.pub.redis.get(feed.feed_publishes)
        self.assertEqual(n, '1')

        # check on redis for ids counter increment
        n = yield self.pub.redis.get(feed.feed_id_incr)
        self.assertEqual(n, '1')

        # check on redis for new item
        ret = yield self.pub.redis.hget(feed.feed_items, id_)
        self.assertEqual(ret[id_], item)

    @defer.inlineCallbacks
    def testPublishEvent(self):
        item = "my beautiful item"
        feed = self.feed
        @self.check_called
        def onPublish(*args):
            #ret_id = args[0]
            ret_item = args[1]
            self.assertEqual(ret_item, item)

        yield self.sub.register_handler(feed.channel_publish, onPublish)

        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        yield self.feed.publish(item)
        yield cb

    @defer.inlineCallbacks
    def testPositionEndEvent(self):
        item = "my beautiful item"
        feed = self.feed
        @self.check_called
        def onPosition(*args):
            #ret_id = args[0]
            ret_pos = args[1]
            self.assertEqual(ret_pos, ":end")

        yield self.sub.register_handler(feed.channel_position, onPosition)

        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        yield self.feed.append(item)
        yield cb

    @defer.inlineCallbacks
    def testFeedAppendPrepend(self):
        items = ["most important item",
                 "my beautiful item",
                 "another amazing item",
                 "ridiculous item"]
        items_ids = []
        feed = self.feed

        for item in items[1:]:
            id_ = yield feed.append(item)
            items_ids.append(id_)

        id_ = yield feed.prepend(item)
        items_ids.insert(0, id_)

        # check on redis for ids
        ret = yield self.pub.redis.lrange(feed.feed_ids, 0, -1)
        self.assertEqual(ret, items_ids)

    @defer.inlineCallbacks
    def testPositionBeginEvent(self):
        item = "my beautiful item"
        feed = self.feed
        @self.check_called
        def onPosition(*args):
            #ret_id = args[0]
            ret_pos = args[1]
            self.assertEqual(ret_pos, ":begin")

        yield self.sub.register_handler(feed.channel_position, onPosition)

        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        yield self.feed.prepend(item)
        yield cb

    ############################################################################
    #  Tests for get*
    ############################################################################
    @defer.inlineCallbacks
    def testFeedGetItem(self):
        item = "my beautiful item"
        feed = self.feed

        id_ = yield feed.append(item)

        ret = yield feed.get_item(id_)
        self.assertEqual(ret, {id_: item})

        # non existing item
        ret = yield feed.get_item(id_ + "123")
        self.assertIsNone(ret)

    @defer.inlineCallbacks
    def testFeedGetIds(self):
        item = "my beautiful item"
        feed = self.feed

        id_ = yield feed.append(item)

        ret = yield feed.get_ids()
        self.assertEqual(ret, [id_])

    @defer.inlineCallbacks
    def testFeedGetIdsGetItems(self):
        import string
        items = string.printable
        feed = self.feed
        ids = []
        for item in items:
            id_ = yield feed.prepend(item)
            ids.append(id_)

        ret = yield feed.get_ids()
        self.assertEqual(set(ret), set(ids))

        ret = yield feed.get_items()
        ret_ids = ret.keys()
        ret_items = ret.values()
        self.assertEqual(set(ret_ids), set(ids))
        self.assertEqual(set(ret_items), set(items))

    ############################################################################
    #  Tests for has_id
    ############################################################################
    @defer.inlineCallbacks
    def testFeedHasId(self):
        item = "my beautiful item"
        feed = self.feed

        id_ = yield feed.append(item)

        ret = yield feed.has_id(id_)
        self.assertTrue(ret)

        # non existing item
        ret = yield feed.has_id(id_ + "123")
        self.assertFalse(ret)

    ############################################################################
    #  Tests for edit
    ############################################################################
    @defer.inlineCallbacks
    def testFeedEdit(self):
        item = "my beautiful item"
        item2 = "replacement item"

        feed = self.feed

        id_ = yield feed.append(item)

        ids = yield feed.get_ids()
        self.assertEqual(len(ids), 1)

        # editing a id that does not exist.
        ret = yield feed.edit(id_ + "123", item)
        self.assertFalse(ret)

        ret = yield feed.get_id(id_)
        self.assertEqual(ret, {id_: item})

        @self.check_called
        def onEdit(*args):
            ret_id = args[0]
            ret_item = args[1]
            self.assertEqual(ret_id, id_)
            self.assertEqual(ret_item, item2)

        yield self.sub.register_handler(feed.channel_publish, onEdit)

        # Assuring that redis.messageReceived (sub) was called
        cb = self.msg_rcv
        ret = yield feed.edit(id_, item2)
        yield cb

        self.assertEquals(ret, id_)

        ids = yield feed.get_ids()
        self.assertEqual(len(ids), 1)

        ret = yield feed.get_id(id_)
        self.assertEqual(ret, {id_: item2})


if __name__ == "__main__":
    pass
