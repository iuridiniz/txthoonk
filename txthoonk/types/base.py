'''
Created on Jul 12, 2012

@author: iuri
'''

class FeedBaseType(object):
    def __init__(self, pub, name):
        '''
        Create a new Feed object for a given Thoonk feed name.

        @param pub: the ThoonkPub object
        @param name: the name of this feed
        '''
        self.pub = pub
        self.name = name

    def get_config(self):
        '''
        Get the configuration dictionary of this feed.
        '''
        return self.pub.get_config(self.name)

    def set_config(self, conf):
        '''
        Set configuration of this feed.

        @param conf: the configuration dictionary.
        '''
        return self.pub.set_config(self.name, conf)

    def get_schema(self):
        '''
        Get the redis keys used by this feed.
        '''
        return []

    def get_channels(self):
        '''
        Get the redis channels used by this channel.
        '''
        return []
