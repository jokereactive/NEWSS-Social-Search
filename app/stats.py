import sys
from dbaccess import User, Tweet, Resource, Tag, db, UserResourceTag
import network

TWITTER = 'twitter'

services = [TWITTER]

serviceUri = {
    TWITTER: 'http://twitter.com/'
}

class stats:
    def get_total_no_users(self):
        return len(User().fetchFromDB().users)

    def get_total_no_resources(self):
        return len(Tweet().fetchFromDB().resources)

    def get_total_no_tags(self):
        return len(Tag().fetchFromDB().tags)

    def get_total_no_urt(self,rtype=None, urt=None):
        if not urt: 
            u, r, t = User(), Resource(), Tag()
            u.fetchFromDB()
            r.fetchFromDB()
            t.fetchFromDB()
            urt = UserResourceTag(User(), Resource(), Tag())
            
        q = urt.q_tw

        urt.fetchFromDB_query(q)
        return len(urt.urt)
    
    def get_stats(self):
        answer=[]
        answer.append(len(db))

        answer.append(self.get_total_no_users())

        answer.append(self.get_total_no_resources())

        answer.append(self.get_total_no_tags())

        # fetch data to use in all next calls
        u, r, t = User(), Resource(), Tag()
        u.fetchFromDB(), r.fetchFromDB(), t.fetchFromDB()
        urt = UserResourceTag(u, r, t)

        answer.append(self.get_total_no_urt(urt=urt))

        return answer
    
    def print_stats(self):
        print 'Total triples =', len(db)

        print 'Total no of users =', self.get_total_no_users()

        print 'Total no of resources =', self.get_total_no_resources()

        print '\tTotal no of Tweets(Twitter) =', self.get_total_no_resources()

        print 'Total no of tags =', self.get_total_no_tags()

        # fetch data to use in all next calls
        u, r, t = User(), Resource(), Tag()
        u.fetchFromDB(), r.fetchFromDB(), t.fetchFromDB()
        urt = UserResourceTag(u, r, t)

        print 'Total no of taggings =', self.get_total_no_urt(urt=urt)

        print '\tTotal no of taggings(Twitter) =', self.get_total_no_urt(rtype=TWITTER, urt=urt)

if __name__ == '__main__':
    s=stats()
    s.print_stats()
