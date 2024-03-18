import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

config_file = "config1.txt"
cfile = open(config_file, "r")
val = cfile.read()

configs = val.split('\n')
total_nodes = int(configs[0])

node_names = []

for i in range(1, len(configs)):
    node_names.append(configs[i].split(' ')[0])

# files = []
    
# max_length = 0
dfs = []
for i in range(0, len(node_names)):
    # fp = open("transaction_timings_" + node_names[i] + ".csv")
    # files.append(fp)
    df = pd.read_csv("transaction_timings_" + node_names[i] + ".csv", header=None)
    # if len(df) > max_length:
    #     max_length = len(df) 
    # print(df)
    dfs.append(df)
        

transactions = {}
for i in range(0, len(dfs)):
    for j in range(0, len(dfs[i])):
        # split = dfs[i][j][0].split("\t")
        split = dfs[i].iloc[j, 0].split("\t")
        transaction = ""
        if split[0] == "Process":
            transaction = split[1] + "\t" + split[2] + "\t" + split[3]
        else:
            transaction = split[0] + "\t" + split[1] + "\t" + split[2]
        if transactions.get(transaction) == None:
            transactions[transaction] = []
            transactions[transaction].append(float(dfs[i].iloc[j, 1])) # start time
            transactions[transaction].append(float(dfs[i].iloc[j, 1])) # last processing node
        
        if split[0] == "Process":
            _, end_time = transactions[transaction]
            if float(dfs[i].iloc[j, 1]) > end_time:
                transactions[transaction][1] = float(dfs[i].iloc[j, 1])
        else:
            transactions[transaction][0] = float(dfs[i].iloc[j, 1])

differences = []
for key in transactions:
    differences.append(transactions[key][1] - transactions[key][0]) # calculate difference

differences = pd.Series(differences)
cdf = np.linspace(0, 1, len(differences))
sorted = np.sort(differences)
cdf = np.cumsum(sorted) / np.sum(sorted)

plt.plot(sorted, cdf)
plt.xlabel("Transaction Processing Time in Seconds")
plt.ylabel("Percentile")
plt.title("CDF of Transaction Processing Time")

ax = plt.gca()
# ax = ax.twinx()
# ax.set_ylim(0.01, 0.99) # 1-99%
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

plt.show()