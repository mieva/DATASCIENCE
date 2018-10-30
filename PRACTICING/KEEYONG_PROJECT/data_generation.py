from random import randint

for i in range(1, 50000):

    userid = randint(1,1000)

    courseid = randint(10,10000)

    pos = randint(0, 150) * 5

    print ("\t".join([str(userid), str(courseid), str(pos)]))
