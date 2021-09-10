import pandas
import numpy as np
# def refineData():
#     data = pandas.read_csv('data.csv')
#     data.to_csv('refined.csv')

# def filter():
#     refinedData = pandas.read_csv('data.csv')
#     refinedData = pandas.DataFrame(refinedData,columns=['pin','name','registration','kra status'])
#     validPins = refinedData['pin']
#     allPins = pandas.read_pickle('sample.pickle')
#     allPins = pandas.DataFrame(allPins,columns=['pin'])
#     excludedPins = pandas.merge(allPins,validPins,how='outer',on='pin',indicator=True)
#     print(len(excludedPins))

# filter()

def groupData():
    data = pandas.read_pickle('sample.pickle')
    data = data[:1000]
    groups = []
    for g,df in data.groupby(np.arange(len(data))// (len(data)//9)):
        groups.append(df.values.tolist())

    return groups


# def

