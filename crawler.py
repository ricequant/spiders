import requests
import pandas as pd
from bs4 import BeautifulSoup
import lxml
import csv
import sys
import threading

class myThread(threading.Thread):
	def __init__(self, thread_num, ticker_list_address):
		threading.Thread.__init__(self)
		self.thread_num = thread_num
		self.ticker_list_address = ticker_list_address
	def run(self):
		print "start: thread " + str(self.thread_num)
		multi_threads_crawl_and_save(self.thread_num, self.ticker_list_address)
		print "end: thread" + str(self.thread_num)



def crawl_and_save(symbol, out):
	count = 0 #the code keep printing a blank line: the 1st entry is empty. Use a counter to skip the first writing
	url = 'http://www.nasdaq.com/symbol/%s/dividend-history' % symbol
	res = requests.get(url)
	#print res.text
	soup = BeautifulSoup(res.text, "html.parser")
	#print(soup.get_text())
	if soup.find(id = 'quotes_content_left_dividendhistoryGrid') != None:
		entries = soup.find(id = 'quotes_content_left_dividendhistoryGrid').find_all('tr')
		for entry in entries:
			
			for item in entry.find_all('td'):
				out.write(item.get_text().strip() + ',')
			if count != 0:
				out.write(','+symbol)
				out.write('\n')
			count = count + 1
			

def multi_threads_crawl_and_save(thread_num, ticker_list_address):
	output = open('dividendData/dividend'+str(thread_num)+'.csv', 'w')
	f = open(ticker_list_address)
	try:
		reader = csv.reader(f)
		for row in reader:
			crawl_and_save(row[0], output)
			print row[0]
	finally:
		f.close()
		output.close()	


def main():
	thread_list = []
	for i in range(1,12):
		thread = myThread(i, str(i)+'.csv')
		thread_list.append(thread)
	
	for i in range(1,12):
		thread_list[i-1].start()
	
	
	
if  __name__ =='__main__':main()
