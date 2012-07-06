'''
Created on Jul 6, 2012

@author: iuri
'''
from tests.test_thoonk_pubsub import TestThoonkBase
from twisted.internet import defer


class TestThoonkFeed(TestThoonkBase):
    @defer.inlineCallbacks
    def setUp(self):
        yield TestThoonkBase.setUp(self)

        self.feed_name = "teste"
        self.config = {'type': 'feed'}
        yield self.pub.create_feed(self.feed_name, self.config)

        from txthoonk.types import Feed
        self.feed = Feed(pub=self.pub, name=self.feed_name)

        # check properties
        self.assertEqual(self.pub, self.feed.pub)
        self.assertEqual(self.feed_name, self.feed.name)

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

    def testFeedPublish(self):
        pass

if __name__ == "__main__":
    pass
