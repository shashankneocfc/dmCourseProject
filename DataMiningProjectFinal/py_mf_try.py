import pymf
import numpy
import locale

n = 6
r = 3

U = [[0.7593, 0.705, 0.705, 0.67],
         [0.7593, 0.5, 0.7593, 0.2],
         [0.559, 0.575, 0.575, 0.56],
         [0.200, 0.05, 0.3, 0.7],
         [0.1207, 0.165, 0.165, 0.2],
         [0.05, 0.1, 0.3, 0.01],
         [0, 0, 0, 0],
         [0, 0, 0, 0],
         [0, 0, 0, 0],
         [0, 0, 0, 0],
         [0, 0, 0, 0],
         [0, 0, 0, 0]
        ]
U = numpy.array(U) 

print "U:"
print U

G = numpy.random.rand(n,r)

F = [[0.799, 0.65, 0.65, 0.6],
         [0.200, 0.30, 0.30, 0.3],
         [0.001, 0.05, 0.05, 0.1]]
F = numpy.array(F)

nmf_mdl = pymf.NMF(U, num_bases=3)
#nmf_mdl.initialization()
nmf_mdl.H = F

print "Initial"
print "H: "
print nmf_mdl.H
nmf_mdl.factorize( niter=1000, compute_h=False, show_progress=True)

print "Final"
print "W: "
print nmf_mdl.W
print "H: "
print nmf_mdl.H
