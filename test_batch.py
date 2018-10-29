import easygrid

class Test1:
	def __init__(self, outfile, index):
		self.outputs = outfile

		self.command = "echo 'hello %s' > %s" % (index, outfile)
		self.batch_size = 4

class Test2:
	def __init__(self, infile, outfile):
		self.outputs = outfile
		self.inputs = infile
		self.batch_size = 2
		self.command = 'cat %s > %s' % (infile, outfile)

pipeline = easygrid.JobManager()

for i in range(1, 10):
	pipeline.add_job(Test1('test1.%s.txt' % i, i))
	pipeline.add_job(Test2('test1.%s.txt' % i, 'test2.%s.txt' % i))
pipeline.run(dry=False)
