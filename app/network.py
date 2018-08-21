import sys, time
from dbaccess import Resource
from twitterbot import TwitterBot
import threading

TWITTER = 'twitter'
services = [TWITTER]
serviceUri = {
    TWITTER: 'http://twitter.com/'
}


def parse_accounts(fin):
    accounts = {}
    for line in fin:
        acclist = line.strip().split(';')

        account = {}
        for acc in acclist:
            acc_type, acc_username = acc.strip().split('=')
            if acc_type == 'username':
                account_id = acc_username
            else:
                account[acc_type] = acc_username

        accounts[account_id] = account

    return accounts

def update_network(accounts, depth):
    for account_id in accounts.keys():
        print '\tupdating user:', account_id

        # start time of update
        sdate = time.time()

        acclist = accounts.get(account_id)
        crawlers, personUris = [], []
        for acc_type in acclist:
            acc_username = acclist[acc_type]
            print '\t\tupdating', acc_type, 'account:', acc_username

            if acc_username == '': continue

            elif acc_type == TWITTER:
                crawler = TwitterBot.factory(acc_username, depth=depth)
                personUris.append(crawler.getUserUri(acc_username))

        # create crawlers using user network accounts data
        crawler.crawlUserNetwork(start_time=sdate)

        # link resources to each other
        Resource.unify_all(personUris)



if __name__ == '__main__':  
    depth = int(2)
    fname = 'people.in'

    # parse accounts from file
    print 'Parsing accounts from input file...'
    accounts = parse_accounts(open(fname))

    # update network for accounts
    print 'Updating network...'
    update_network(accounts, depth)
    print 'Done updating!'
