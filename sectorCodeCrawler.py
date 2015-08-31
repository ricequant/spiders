import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import shutil
import random


def getSectorCode(str_sectorName):
	if str_sectorName == 'ETF':
		return 'ETF'
	if str_sectorName == 'Finance':
		return 'Financials'
	if str_sectorName == 'Basic Industries':
		return 'Materials'
	if str_sectorName == 'Capital Goods':
		return 'Industrials'
	if str_sectorName == 'Consumer Durables':
		return 'ConsumerDiscretionary'
	if str_sectorName == 'Consumer Non-Durables':
		return 'consumerStaples'
	if str_sectorName == 'Consumer Services':
		return 'consumerDiscretionary'
	if str_sectorName == 'Energy':
		return 'Energy'
	if str_sectorName == 'Health Care':
		return 'HealthCare'
	if str_sectorName == 'Miscellaneous':
		return 'Unknown'
	if str_sectorName == 'Public Utilities':
		return 'Utilities'
	if str_sectorName == 'Technology':
		return 'InformationTechnology'
	if str_sectorName == 'Transportation':
		return 'Industrials'
	
def crawl_sector_code(ticker, list_dict_sectorCode):
	time.sleep(random.random()*3)
	url = "http://www.nasdaq.com/symbol/%s" %ticker
	res = None

	try:
		res = requests.get(url, timeout = 10)
		if res.status_code != requests.codes.ok:
			print "ERROR: Cannot download: " + ticker + " successfully. status_code: ", res.status_code
	except:
		print ticker + " exception"
		list_dict_sectorCode.append({'AbbrevSymbol':ticker, 'SectorCodeName':'Unknown', 'SectorCode': 'Unknown'})
		return None
	
	soup = BeautifulSoup(res.text, "html.parser")
	if soup.find(id = 'qbar_sectorLabel') != None:
		entries = soup.find(id = 'qbar_sectorLabel').find_all('a')
		if entries == []:
			list_dict_sectorCode.append({'AbbrevSymbol':ticker, 'SectorCodeName':'ETF', 'SectorCode': 'ETF'})
		if entries != []:
			for entry in entries:
				str_sectorName = entry.get_text()
				str_sectorCode = getSectorCode(str_sectorName)
				list_dict_sectorCode.append({'AbbrevSymbol': ticker, 'SectorCodeName': str_sectorName, 'SectorCode': str_sectorCode})
	print "sector code for %s has been processed." %ticker
		 
		
def crawl_all_stocks():
	print "sector code crawler starts processing..."
	if not os.path.exists('sector_code'):
		os.mkdir('sector_code')
	
	int_numOfProcess = 20
	list_int_children = []
	list_dict_sectorCode = []
	
	#use a local directory for debugging purpose only
	df_metadata = pd.read_csv("/etc/rq/hd/Instruments/latest/us/US_Instruments.csv")
	#df_metadata = pd.read_csv("US_Instruments.csv")
	int_len_of_meta = len(df_metadata.AbbrevSymbol) 
	print "length of metadata is %d." %int_len_of_meta
	
	
	#multi-processing starts here
	for i in range(int_numOfProcess):
		pid = os.fork()
		
		if pid:
			print "child pid is %d" %pid
			#time.sleep(1)
			list_int_children.append(pid)
		
		else:
			list_dict_sectorCode = []
			print "sub_process %d starts." %i
			for j in range(int_len_of_meta):
				if j % int_numOfProcess == i:
					crawl_sector_code(df_metadata['AbbrevSymbol'].iloc[j], list_dict_sectorCode)
					print "%d stocks has been processed." %j
			df = pd.DataFrame(list_dict_sectorCode)
			df.to_csv('sector_code/%d.csv' %i, index = False)
				
			os._exit(0)  
	
	for i,child in enumerate(list_int_children):
		os.waitpid(child, 0)
	
	list_df_sectorCode = []
	for file_ in os.listdir('sector_code'):
		df_temp = pd.read_csv('sector_code/' + file_)
		list_df_sectorCode.append(df_temp)
	
	df_out = pd.concat(list_df_sectorCode)
	df_out.to_csv("sectorCode.csv", index = False)
	
	shutil.rmtree('sector_code')
	print "all sector codes are crawled."
	
if __name__ == '__main__':
	main()
