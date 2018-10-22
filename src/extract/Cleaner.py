import csv
import sys


def splitfile(path):
	outpath = "/home/ubuntu/datasets/stocks/all/"
	with open(path) as f:
		f.readline()
		tickers = {}
		lines = f.readlines()[500000:]
		buf = []
		for line in lines:
			linearr = line.split(",")
			ticker = str(linearr[0])
			#print(tickers)
			#print(linearr)
			#print(linearr)
			#print()
			if ticker in tickers:
				#print (ticker, " exists")
				buf.append(linearr[1:-1])
			else:
				outfile = outpath + ticker + ".csv"
				if (len(buf)):
					with open(outfile, "w") as output:
						writer = csv.writer(output, delimiter=',', lineterminator="\n", quotechar= '"')
						writer.writerows(buf)
				buf = []
				#print("here")
				tickers[ticker] = outfile
				buf.append(str(linearr[1:]))
			#print(len(buf))
			#print(len(tickers))

stock_path = "/home/ubuntu/datasets/stocks/huge.txt"
splitfile(stock_path)

