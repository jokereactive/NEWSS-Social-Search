import dbaccess as db

from pysparse import spmatrix
import numpy as np

class FolkRank(object):

    def __init__(self, urt=None, nu=0, nr=0, nt=0):
        # number of users, resources and tags
        self.nu = nu
        self.nr = nr
        self.nt = nt
        if urt == None:
            urt = []

        # init the adjacency matrix
        self.initA(urt=urt, nu=nu, nr=nr, nt=nt)
        # normalize A
        self.normalizeA()

    def initA(self, urt=None, nu=0, nr=0, nt=0):
        # beginning positions for users, resources and tags in the matrix
        self.u_beg = 0
        self.r_beg = self.nu
        self.t_beg = self.nu + self.nr
        if urt == None:
            urt = []

        # total number of entities --> gives the size of the matrix
        self.n = self.nu + self.nr + self.nt

        # create the relation matrix A      
        self.A = spmatrix.ll_mat(self.n, self.n)

        # create adjacency matrix in the graph
        for u,r,t in urt:
             i = self.u_beg + u
             j = self.r_beg + r
             k = self.t_beg + t

             self.A[i,j] = self.A[j,i] = self.A[i,j] + 1
             self.A[i,k] = self.A[k,i] = self.A[i,k] + 1
             self.A[k,j] = self.A[j,k] = self.A[k,j] + 1

    def normalizeA(self):
        # compute sum of rows in sum vector
        v = np.ones(self.n)
        sum = np.zeros(self.n)
        self.A.matvec(v, sum)           # sum = A*v
        
        D = spmatrix.ll_mat(self.n, self.n)
        
        for i in range(self.n):
            if sum[i] != 0:
                D[i,i] = 1.0/sum[i]
            else: 
                D[i,i] = 1

        self.A = spmatrix.matrixmultiply(self.A, D)

    def computeP(self, tagid):
        '''
        Compute the preference vector
        '''
        high_w = 0.6
        low_w = (1.0 - high_w)/self.n

        p = np.multiply(np.ones(self.n), low_w)
        p[self.t_beg + tagid] = high_w

        return p

    def computeW(self, p, d, no_steps, Ap, eps=0):
        w = np.ones(self.n, '|f8')
        w = np.multiply(w, float(1.0 / self.n))       # w <- [1/n 1/n ... 1/n]
        w_tmp = np.zeros(self.n)

        f = open('weights%f.txt' % d, 'w')
        f.write('P %d, SUM = %f : %s\n' % (no_steps, np.sum(p), str(p)))
        f.write('%d, SUM = %f : %s\n' % (no_steps, np.sum(w), str(w)))

        while no_steps>0:
            Ap.matvec(w, w_tmp)                        # w_tmp <- A*w
            w = np.add(np.multiply(w_tmp, d), np.multiply(p, (1 - d)))    # w <- d*w_tmp + (1-d)*p
            no_steps = no_steps - 1

            f.write('%d, SUM = %f : %s\n' % (no_steps, np.sum(w), str(w)))

        return w

    def search(self, tagid):
        # compute preference vector
        p = self.computeP(tagid) 
        no_steps = 50

        d = 1
        w0 = self.computeW(p, d, no_steps, self.A.to_csr())   
        d = 0.75
        w1 = self.computeW(p, d, no_steps, self.A.to_csr())
        w = np.add(w1, np.multiply(w0, -1))

        bec_u = []
        bec_r = []
        bec_t = []
        bec = []

        for i in xrange(self.n):
            if i < self.r_beg:
                bec_u.append([w[i], i])
            elif i < self.t_beg:
                bec_r.append([w[i], i - self.r_beg])
            else:
                bec_t.append([w[i], i - self.t_beg])

        bec_u.sort(key=lambda a: -a[0])
        bec_r.sort(key=lambda a: -a[0])
        bec_t.sort(key=lambda a: -a[0])

        bec.append(bec_u)
        bec.append(bec_r)
        bec.append(bec_t)

        return bec

    def searchResourcesByTags(self, stags):
        return self.search(stags)

def writeResultsToFile(filename, res, users=None, resources=None, tags=None):
    f = file(filename, 'w')
    f.write('\n\n\nTOP USERS\n\n\n')
    print len(res)
    print ('users %d == %d' % (len(users.users), len(res[0])))
    print ('resources %d == %d' % (len(resources.resources), len(res[1])))
    print ('tags %d == %d' % (len(tags.tags), len(res[2])))
    for rat,id in res[0]:
        f.write('[%2.6f, %d, %s]\n' % (rat, id, users.users[id]))
    f.write('\n\n\nTOP RESOURCES\n\n\n')
    for rat,id in res[1]:
        f.write('[%2.6f, %d, %s]\n' % (rat, id, resources.resources[id]))
    f.write('\n\n\nTOP TAGS\n\n\n')
    for rat,id in res[2]:
        f.write('[%2.6f, %d, %s]\n' % (rat, id, tags.tags[id]))
    f.close()

if __name__ == '__main__':

    # create users, resources and tag objects
    users = db.User()
    resources = db.Resource()
    tags = db.Tag()

    # fetch users, resources and tags from repository
    users.fetchFromDB()
    resources.fetchFromDB()
    tags.fetchFromDB()

    # create user-tag-resource relations object
    urt = db.UserResourceTag(users, resources, tags)
    # fetch relations from db
    urt.fetchFromDB()

    # print data
    f = open('res_repr.txt', 'w')
    f.write('\n\n\nUSERS\n\n\n')
    f.write(repr(users))
    f.write('\n\n\nRESOURCES\n\n\n')
    f.write(repr(resources))
    f.write('\n\n\nTAGS\n\n\n')
    f.write(repr(tags))
    f.write('\n\n\nRELATIONS\n\n\n')
    for (u, r, t) in urt.urt:
        f.write('(%d, %d, %d)\n' % (u, r, t))
    f.close()

    # init folk rank algorithm
    fr = FolkRank(
        urt=urt.urt,
        nu=urt.getUsersNumber(),
        nr=urt.getResourcesNumber(),
        nt=urt.getTagsNumber()
    )
    fr.A.export_mtx('rez.txt', 2)

    # search tags list
    stags = []
    print 'Starting search for tags : %d - %s' % (3, urt.tags.tags[3])
    res = fr.searchResourcesByTags(3)
    writeResultsToFile('ratings3.txt', res, users=users, resources=resources, tags=tags)

    print 'Starting search for tags : %d - %s' % (6, urt.tags.tags[6])
    res = fr.searchResourcesByTags(6)
    writeResultsToFile('ratings6.txt', res, users=users, resources=resources, tags=tags)

    print 'Starting search for tags : %d - %s' % (7, urt.tags.tags[7])
    res = fr.searchResourcesByTags(7)
    writeResultsToFile('ratings7.txt', res, users=users, resources=resources, tags=tags)


    print 'All done !'
