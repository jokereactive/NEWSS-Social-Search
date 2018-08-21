import dbaccess as dbaccess
import urllib
import urllib2
import re
from twython import Twython
import threading

TWITTER = 'twitter'

services = [TWITTER]

serviceUri = {
    TWITTER: 'http://twitter.com/'
}


defAvatar = 'http://careernetwork.msu.edu/wp-content/themes/cspMSU_v4.1/_images/twitter-logo.png'

class TwitterBot():
    '''
    Crawls twitter and updates social network and tagged resources to repository
    '''

    def __init__(self):
        self.service = TWITTER
        APP_KEY = "w7zjbW1qHtzX2GxbrHbeNgyPr"
        APP_SECRET = "qJFawkFxwYZX6U3DYNvJI3czWEjix2owU9wTXZ9BorMkhFU4Yb"
        self.twapi = Twython(APP_KEY, APP_SECRET)
        
        self.userBaseUri = 'http://twitter.com/%s'                   # % user screen_name
        self.tagBaseUri = 'http://twitter.com/#search?q=#%s'         # % trend key word

        self.tweetBaseUri = 'http://twitter.com/%s/status/%s'        # % (user screen_name, status id)

    @staticmethod
    def getname():
        return self.service
    
    @staticmethod
    def getDefaultAvatar():
        return defAvatar

    def setStartUserId(self, start_user_id):
        self.start_user_id = start_user_id

    def setMaxLevel(self, max_level):
        self.max_level = max_level

    def setLastAdded(self, last_added):
        self.last_added = last_added

    def updateDatabase(self, user_metadata, user_ntw, udate=None):
        raise NotImplementedError()

    def fetchUser(self, username):
        return (None, {'knows':[], 'fans':[]})

    def getUserUri(self, username):
        return self.userBaseUri % username
        
    @staticmethod
    def factory(user, params=None, depth=2, last_added=None, verbose=False):
	# Twitter crawler
        twcrawl = TwitterBot()
        twcrawl.setStartUserId(user)
        twcrawl.setMaxLevel(depth)
        twcrawl.setLastAdded(last_added)
        return twcrawl

    def updateDatabase(self, user_metadata, user_ntw, udate=None):
        username = user_metadata['username']
        userUri = self.userBaseUri % username
        resourceUri = None
        added = 0

        # add user to repository
        dbaccess.User.addToDB(userUri, self.service, username, 
            TwitterBot.getDefaultAvatar(), udate=udate)

        # add resources, tags and relations between them to repository
        for resource in user_metadata['data']:
            # add tweet resource to repository
            resourceId = resource['id']
            resourceTitle = resource['text']
            resourceUri = self.tweetBaseUri % (username, resourceId)

            dbaccess.Tweet.addToDB(resourceUri, resourceTitle, udate=udate)
            added += 1

            # add tag(trend) relations to repository
            for tag in self.parseTags(resourceTitle):
                tagUri = self.tagBaseUri % urllib.quote(tag.encode('utf-8'))
                # add tag to repository
                dbaccess.Tag.addToDB(tagUri, tag, udate=udate)
                # add user, resource, tag relation to repository
                dbaccess.UserResourceTag.addToDB(userUri, resourceUri, tagUri, udate=udate)

            # add url relations between the tweet and urls contained
            urls = map(self.getRealUrl, self.parseUrls(resourceTitle))
            for url in urls:
                dbaccess.Bookmark.addToDB(url, None)
                dbaccess.Tweet.addReferenceToDB(resourceUri, url)

        # add user social network relations to repository
        # add friends
        for u in user_ntw['knows']:
            knownUserUri = self.userBaseUri % u
            dbaccess.User.addToDB(knownUserUri, self.service, u, 
                TwitterBot.getDefaultAvatar(), udate=udate)
            dbaccess.User.addToDBUserKnowsUser(userUri, knownUserUri, udate=udate)

        # add followers
        for u in user_ntw['fans']:
            otherUserUri = self.userBaseUri % u
            dbaccess.User.addToDB(otherUserUri, self.service, u, 
                TwitterBot.getDefaultAvatar(), udate=udate)
            dbaccess.User.addToDBUserKnowsUser(otherUserUri, userUri, udate=udate)

        # return number of added resources and last added resource uri
        return added

    def fetchUserMetadata(self, username):
        # get user tweets (only last 20)
        data = self.twapi.get_user_timeline(screen_name=username)
        return { 'username': username, 'data': data}

    def fetchUserNetwork(self, username):
        chunk_size = 100	# max returned by bulkUserLookup
        # Get Friends
        
        fids, cursor = [], -1
        fchunks = [fids[i:i + chunk_size] for i in range(0, len(fids), chunk_size)]
        while True:
            f = self.twapi.get_friends_ids(screen_name=username, cursor=cursor)
            
            fids.extend(f['ids'])
            if f['next_cursor'] == cursor or len(f['ids']) == 0:
                break
            cursor = f['next_cursor']

            # get screen names from ids in chunks of 100
        screen_names = []
        for chunk in fchunks:
            screen_names = []
            
            screen_names.extend([userdata['screen_name'] 
                for userdata in self.twapi.lookup_user(user_id=chunk)])

        ntw_knows = screen_names
        print ntw_knows
        # Get Followers
        fids, cursor = [], -1
        while True:
            f = self.twapi.get_followers_ids(screen_name=username, cursor=cursor)
            fids.extend(f['ids'])
            if f['next_cursor'] == cursor or len(f['ids']) == 0:
                break
            cursor = f['next_cursor']

        fchunks = [fids[i:i + chunk_size] for i in range(0, len(fids), chunk_size)]
        fan_screen_names = []
        for chunk in fchunks:
            # get all data for all user ids
            fan_screen_names.extend([userdata['screen_name'] 
                for userdata in self.twapi.lookup_user(user_id=chunk)])
        
        ntw_fans = fan_screen_names

        return {'knows': ntw_knows, 'fans': ntw_fans}

    def fetchUser(self, username):
        return (self.fetchUserMetadata(username), self.fetchUserNetwork(username))

    def parseTags(self, text):
        '''
        Parses trends from tweet: '#trend #tre#nd ## #trend,two #tren%^& smth,#da #1dc #100 #1dc$ #100^',
        finds trends: trend, tre, trend, tren, da, 1dc, 100, 1dc, 100.
        Text parameter MUST be UNICODE encoded: u'some text'
        '''
        return re.findall('(?<=[\W^#]#)\w+', ' ' + text, re.UNICODE)

    def parseUrls(self, text):
        '''Returns a list of urls from the text.
        '''
        return re.findall('http://\S+', text)

    def getRealUrl(self, url):
        '''Returns the redirected url if *url* parameter is a shortened url (a redirect was produced), 
        or itself if not.
        '''
        try:
            u = urllib2.urlopen(url)
            realurl = u.geturl()
            if realurl == None: 
                raise urllib2.HTTPError('Could not fetch url')
        except:
            realurl = url

        return realurl

    def crawlUserNetwork(self, user_id=None, max_level=0, start_time=None):
        '''
        user_id - an id to uniquely identify a user (can be a username or userid)
        '''
        user_id = user_id or self.start_user_id
        max_level = max_level or self.max_level

        queue = [user_id]               # init queue
        visited = {user_id: 0}          # init visited nodes
        added = 0                       # set number of added resources

        while queue:
            v = queue.pop(0)            # pop next user node
            level = visited[v]          # get their level

            # get user metadata and social network
            user_metadata, user_ntw = self.fetchUser(v)

            # update database with user data, tagged resources and social network relations
            a = self.updateDatabase(user_metadata, user_ntw, udate=start_time)
            added += a

            # explore following nodes
            for w in user_ntw['knows']:
                if w not in visited and level < max_level:
                    queue.append(w)
                    visited[w] = level + 1

        return added



