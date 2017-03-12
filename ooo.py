#
#March 12th, 2017, updated at 12 pm

import sys
from datetime import datetime
from itertools import islice

#Setting up basic logging information
time_start = datetime.now()
print time_start

#Setting up basic logging information
stockdata = {}
count = 0
chunk = 0
N = 10000
finish = False



#setting up functions for later

#function readfrom allows me to accept input in x line increments with
#n line increments with n being the number specified in N above

def readfrom(readfile, readrows):
	for line in itertools.islice(readfile, readrows):
		line = line.strip()
		return line


		
#function cleanfile allows me to clean out the datapoints that have
#negative price&volume and duplicates

def cleanfile(lines):
	
	clean_start = datetime.now()
	
	# remember before using this, I have to set: lines_seen = set()
	if lines not in lines_seen:
		noise_file.write(str(lines) + '\n')
	
	elif lines[1] <= 0:	#or if it's too large
		noise_file.write(str(lines) + '\n')
		
	elif lines[2] < 1:
		noise_file.write(str(lines) + '\n')
		
	else:
		signal_file.write(str(lines) + '\n')
		
	lines_seen.add(row)	
	
	clean_end = datetime.now()
	clean_time = clean_end - clean_start
	return clean_time
	
file_open_time = datetime.now()
print file_open_time


#fileopen = input("What file do you want to open? ")

try:
	
	signal_file = open("signal.txt","w")
	signal_file.close()
	signal_file = open("signal.txt","a")
	
	noise_file = open("noise.txt","w")
	noise_file.close()
	noise_file = open("noise.txt","a")
	
	log_file = open("log.txt","w")
	log_file.close()
	log_file = open("log.txt","a")
	
	normtest_file = open("normtest.txt","w")
	normtest_file.close()
	normtest_file = open("normtest.txt","a")
	
	with open(sys.argv[1]) as file_open:
		begin_time = datetime.now()
		cleantime = datetime.now()
		while finish == False:
			chunk += 1
			if count == 0:
				lines_to_read = readfrom(file_open, N)
				print "Now there are " + str(N) + "lines, and now the time is : " + str(begin_time)
				
			print "Now processing chunk :", str(chunk)
			
			for line in lines_to_read:
				try:
					stockdata[count] = line.split(",")
					stockdata[count][0] = datetime.strptime(stockdata[count][0],"%Y%m%d:%H:%M:%S.%f")
					stockdata[count][1] = float(stockdata[count][1])
					stockdata[count][2] = int(stockdata[count][2])
					
					cleantime += cleanfile(stockdata[count])
					count += 1
				
				except:
					pass
			
			normtest.write(str(sorted(stockdata.keys(), key = lambda s:stockdata[s][0])) + '\n')
			lines_to_read = readfrom(file_open, N)
			
			if  not lines_to_read:
				print "Finished!"
				timespent = cleantime - begin_time
				log_file.write("The total time spent on cleaning the data: ", str(timespent))
				
				signal_file.close()
				noise_file.close()
				log_file.close()
				normtest_file.close()
				finish = True
				
				
			
	#try:	let's use try and except later when I finish this part
	
except:
		
	print "something went wrong"
	file_open.close()
	signal_file.close()
	noise_file.close()
	log_file.close()
	normtest_file.close()
	sys.exit(1)
	