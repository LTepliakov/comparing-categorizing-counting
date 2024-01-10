if __name__ == "__main__": 
    import sys
    import os
    from pathlib import Path
#    os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask
    import argparse
    import pandas as pd
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
#   import modin.pandas as pd
    import pyxlsb
    import string
    import time
    from datetime import datetime
    import difflib
    import numpy as np
    import re
    import gc
 #   import objgraph

    from rapidfuzz import fuzz,utils
#    from thefuzz import fuzz
#    from thefuzz import process

 #   import fuzzywuzzy
 #   from fuzzywuzzy import fuzz
 #   from fuzzywuzzy import process

    parser= argparse.ArgumentParser('Categorize input excel')
    parser.add_argument('inputFile',metavar='1',type=str,nargs='?',help='input file name')
    parser.add_argument('--output',help='output file name, if not specified, input file name suffixed with _parsed')
    parser.add_argument('--matchPcntName',help='match percent for name and address comparison. 80% by default')
    parser.add_argument('--matchPcntAddress',help='match percent for name and address comparison. 80% by default')
    parser.add_argument('--limitInputRows',help='limit number of rows read from input file. No limit by default')
    parser.add_argument('--limitUniqueClients',help='limit number of unique clients taken from input file . No limit by default')
    parser.add_argument('--skipFirstRows',help='Number of rows to skip before start processing')
    parser.add_argument('--exemptionsFile',help='Name of file with exemptions. exemptions.txt by default')
    args=parser.parse_args()

    permittedOutputExts=('.xlsx')
    permittedInputExts=('.xlsb','.xlsx')

    inputFileName,inputFileExt=os.path.splitext(args.inputFile)

    print("Input file name:",inputFileName)
    print("Input fle extension:",inputFileExt)

    if inputFileExt not in permittedInputExts:
        print("Only ", permittedInputExts, " input files are supported for now. Exiting...")
        sys.exit()


    if args.matchPcntName :
        matchPercentName=float(args.matchPcntName)
    else:
        matchPercentName=80.

    print ("Name match percent: ", matchPercentName)   

    if args.matchPcntAddress :
        matchPercentAddress=float(args.matchPcntAddress)
    else:
        matchPercentAddress=80.

    print ("Address match percent: ", matchPercentAddress)   

    if args.exemptionsFile:
        exemptionsFileName=args.exemptionsFile
    else:
        exemptionsFileName="exemptions.txt"

    if os.path.isfile(exemptionsFileName.strip()):
        useExemptions=True
    else:
        print("Exemptions file '",exemptionsFileName,"' not found.") # \nContinue without exemptions? (y/n)")
        join=input('Continue without exemptions? (y/n)\n')
        if join.lower() =='yes' or join.lower() == 'y':
            useExemptions=False
        else:
            sys.exit()

    if args.output :
        outputFileName,outputFileExt=os.path.splitext(args.output)

        if outputFileExt :
            if outputFileExt not in permittedOutputExts:
                print("Only ", permittedOutputExts, " output files are supported for now. Exiting...")
                sys.exit()
        else:
            outputFileExt='.xlsx'

    else:
        outputFileName=inputFileName+'-out-pan-fuz1-sort-2-npcnt'+str(matchPercentName)+'-apcnt'+str(matchPercentAddress)
        if useExemptions:
            outputFileName=outputFileName+'-ex1'
        if args.limitInputRows:
            outputFileName=outputFileName+'-IL'+args.limitInputRows
        if args.limitUniqueClients:
            outputFileName=outputFileName+'-TL'+args.limitUniqueClients
        if args.skipFirstRows:
            outputFileName=outputFileName+'-skip'+args.skipFirstRows
        outputFileExt='.xlsx'

    outfile=outputFileName+outputFileExt
    print("Output file: ",outfile)


    if useExemptions:
        print ("Loading exemptions file: ", exemptionsFileName)
        exemptions=pd.read_csv(exemptionsFileName,header=None)
        print("Done.\n Preparing exemptions.")
        exemptions.drop(exemptions[exemptions[0].isna()].index,inplace=True) #dropping nan values just to be on safe side
        exemptions[1]=exemptions[0].apply(lambda x : len(x.split()))         # prepare columns for sorting
        exemptions[2]=exemptions[0].apply(lambda x : len(x))                 # for regex patterns are to be applied longest first
        exemptions=exemptions.sort_values(by = [1,2], ascending = [False,False])

        # build regexp and function for exemptions cleanup

        r="("
        for x in exemptions[0].tolist():
            x=x.strip()
            x=x.replace(".","\.")
            r=r+r'\s'+x+r'\s|'
        r=r[:-1]+r')'

        print("***\n",r)
        rEx=re.compile(r)

        def removeExemptions(s):
            s=s.upper()
            i=0
            while i<100:
                result=rEx.sub(' ',' '+s+' ').strip()
        #        print('\ns=',s,'\nr=',result,'\n----')
                i=i+1
                if result==s:
                    break
                s=result
            return result

    else:
        print("OK. Continuing without exemptions.")

# Loading input file to dataframe    

    file_load_start_time=datetime.now()
    print("\nLoading input file: "+inputFileName+inputFileExt)
    if args.limitInputRows:
        print("Limit number or rows read from input file to ", args.limitInputRows)
        df=pd.read_excel(inputFileName+inputFileExt,keep_default_na=False,nrows=int(args.limitInputRows))
    else:
        df=pd.read_excel(inputFileName+inputFileExt,keep_default_na=False)
    print('Loded input file successfully. Duration: {}'.format(datetime.now() -file_load_start_time))

# Start porcessing input

    process_start_time=datetime.now()

# Change column names for convenience

    origColumnList=list(df.columns)
    newColMapping={\
            df.columns[0]:"PARCEL ID - APPRAISER",\
            df.columns[1]:"Code",\
            df.columns[3]:"D",\
            df.columns[4]:"E",\
            df.columns[5]:"F",\
            df.columns[6]:"Client_Name",\
            df.columns[7]:"H",\
            df.columns[8]:"A_Addr",\
            df.columns[9]:"T_Addr",\
            df.columns[10]:"DTP_TDA"
            }
    df=df.rename(columns=newColMapping)

# dataframe cleanup and preparation

    # Skip second header line which appears on the first row of dataframe 
    df.drop(index=df.index[0], axis=0,inplace=True)
    df.reset_index(inplace=True)
#    print(df.info())
 
    # Skip first rows if kipFirstRows parameter is set
    if args.skipFirstRows :
        iskip=int(args.skipFirstRows)
        print('Skipping ',iskip,' input rows before start processing')
        df.drop(index=df.index[:iskip], axis=0,inplace=True)
        df.reset_index(inplace=True)
       #df=df.drop(index=df.index[:iskip], axis=0).reset_index()


    # Bring all values in name columns to string type
    df['A_Addr'] = df['A_Addr'].astype(str)
    df['T_Addr'] = df['T_Addr'].astype(str)
    df['Client_Name'] = df['Client_Name'].astype(str)
    #df['Code	'] = df['Code'].astype(str)

    #print(df.info())

    # Replace all empty strings in address columns with special value %E% (Hope we do not have such real client names or addresses)
    # We need that to significantly simplify name comparison function, which is heavily used and critical for script performance.

    # This may be rudiment from initial version of comparison function, but requires additional testing to be removed.

    #df.loc[df['A_Addr'].apply(lambda x: x.translate({ord(c): None for c in string.whitespace})).apply(len)==0, ['A_Addr']] = '%E%'
    #df.loc[df['T_Addr'].apply(lambda x: x.translate({ord(c): None for c in string.whitespace})).apply(len)==0, ['T_Addr']] = '%E%'
    #df.loc[df['Client_Name'].apply(lambda x: x.translate({ord(c): None for c in string.whitespace})).apply(len)==0, ['Client_Name']] = '%E%'

    df['H']='0'
    df = df.astype({'H':'int'})

# define some functions to be used in the process

    def compareNames(name1, name2):        
#         return fuzz.token_sort_ratio(name1,name2)
        return fuzz.token_set_ratio(name1,name2)

    def sort_words(s): # sorts words in a string alphabetically and converts to uppercase: "january february march" - 'FEBRUARY JANUARY MARCH'
        l=s.split()
        l.sort()
        return ' '.join(l).upper()  
    
    squizeSparsed=re.compile(r'(?:^|\s)(?:[A-z](?:\s|$)){2,}')
    def compressSparsed(s):  # .compreses sparsed text : "s p a r s e d text" -> "sparsed text"
#        wordList=re.findall(r'(?:^|\s)(?:[A-z](?:\s|$)){2,}',s)
        wordList=squizeSparsed.findall(s)
        for w in wordList:
            s=s.replace(w.strip(),w.replace(' ',''))
        return s

    # compress sparsed text in client name 
    df['Client_Name_s']=df['Client_Name'].apply(lambda x: compressSparsed(x))

    # create work Client_Name_woe column for client names with exemptions removed
    if useExemptions:
        print("Removing exemptions form name column.")
        t=datetime.now()
#        df['Client_Name_woe']=df['Client_Name_s'].apply(lambda x :removeExemptions.sub(' ',' '+x)) # added ' ' before x for regex to capture exemptions, started at beginning of name
        df['Client_Name_woe']=df['Client_Name_s'].apply(lambda x :removeExemptions(x)) 
        print("Done. Duration: ",datetime.now()-t)

    print("Prepare sorted name and address columns before comparison.")
    t=datetime.now()


    if useExemptions:
        df['Client_Name_s']=df.Client_Name_woe.apply(lambda x : sort_words(x))
    else:
        df['Client_Name_s']=df['Client_Name_s'].apply(lambda x : sort_words(x))
    df['A_Addr_s']=df.A_Addr.apply(lambda x : sort_words(x))
    df['T_Addr_s']=df.T_Addr.apply(lambda x : sort_words(x))
    print("Sorting done. Duration:",datetime.now()-t)


    print("Clean away non alphanumeric characters before comparison")
    t=datetime.now()
    df['Client_Name_s']=df['Client_Name_s'].apply(lambda x : utils.default_process(x))
    df['A_Addr_s']=df.A_Addr.apply(lambda x : utils.default_process(x))
    df['T_Addr_s']=df.T_Addr.apply(lambda x : utils.default_process(x))
    print("Cleaning done. Duration:",datetime.now()-t)
   


    print(df.head(100))

    #sameClientGroup=pd.DataFrame()  
    sameClientGroup=df.iloc[:0]
    out_df_temp=df.iloc[:0]

    #sameClientGroup["client_id"]=0
    #sameClientGroup.astype({"client_id":int})
    client_id=0
    iteration_number=0
    # group set of similar name clients by groups of same client
    #print('@@@ 3',df.loc[0])
    print("Starting process to group same clients.")
    if args.limitUniqueClients:
        print("Limiting to ",args.limitUniqueClients," unique clients only")
    while df.shape[0] > 0 and (not args.limitUniqueClients or client_id < int(args.limitUniqueClients)):

        collected = gc.collect()
        print("Garbage collector: collected %d objects." % (collected))

#        objgraph.show_growth
#        client.run(trim_memory)

        iteration_start_time=datetime.now()  
        print("\n\nClient # ",client_id, "  df lengh ",df.shape[0])

        ser=df.iloc[0].copy(deep=True)
        ser["ClientId"]=str(client_id)
        group_addresses_set=set(ser['A_Addr_s':'T_Addr_s'].dropna().tolist())
        sameClientGroup=pd.concat([sameClientGroup, ser.to_frame().T], ignore_index=True)
        name=ser["Client_Name_s"]
        print("Client Name:",ser["Client_Name"],"index:",ser["index"]," Address set:", group_addresses_set)
        df.drop(index=df.index[0], axis=0,inplace=True) # first client is in the group, we do not need it on input any more

        sameNameGroup=df[df.Client_Name_s.apply(lambda x: compareNames(name,x)>=matchPercentName)] # looking for same name clients on the rest of input
        print("sameNameGroup len: ",sameNameGroup.shape[0], "Search time: ",datetime.now()- iteration_start_time,'\n',sameNameGroup)
        more=True

        while sameNameGroup.shape[0] > 0  and more:
            sameNameIndexes=sameNameGroup.index.values.tolist()
            #    sameNameIndexes.reverse()
            more=False
            for j in sameNameIndexes:
                ser=sameNameGroup.loc[j].copy(deep=True)
                print("Candidate # ", j, " Name:",ser.loc["Client_Name"])
                candidate_address_list=ser.loc['A_Addr_s':'T_Addr_s'].dropna().tolist()
                print("***5 Candidate addresses:",candidate_address_list)
                print("***6 Group addresses:",group_addresses_set)
                match=False
                for group_addr in group_addresses_set:
                    print("***7 group_addr=",group_addr)
                    for candidate_addr in candidate_address_list:
                        if compareNames(group_addr,candidate_addr)>=matchPercentAddress :
                            match=True
                            break
                print("match=",match)

                if match:
                    ser["ClientId"]=str(client_id)
    #                    ser["ClientId"]=client_id
                    group_addresses_set.update(candidate_address_list)
                    print("**12 group_addres changed to=",group_addresses_set)
                    print("**10 Adding parcel ",ser['PARCEL ID - APPRAISER'],"as client_id=",client_id)
    #                    sameClientGroup=sameClientGroup.append(ser,ignore_index=True)
                    sameClientGroup=pd.concat([sameClientGroup, ser.to_frame().T], ignore_index=True)
                    sameNameGroup=sameNameGroup.drop(j)
                    more=True

    #    out_df_temp=out_df_temp.append(sameClientGroup,ignore_index=True)
        print("sameClientGroup len: ",sameClientGroup.shape[0],'\n',sameClientGroup)
        t=datetime.now()
        out_df_temp=pd.concat([out_df_temp,sameClientGroup], ignore_index=True)
        print('**20 Concat time: ', datetime.now()-t)
                    
        same_client_index_list=sameClientGroup["index"].tolist() 
        df.drop(df[df["index"].isin(same_client_index_list)].index,inplace=True) 
    
        del [[sameClientGroup,sameNameGroup]]
        gc.collect()
        sameClientGroup=df.iloc[:0]
 
        client_id=client_id+1
        print('Iteration complete. Duration: {}'.format(datetime.now() - iteration_start_time))
        print('Elapsed time: {}'.format(datetime.now() - process_start_time))

    print('Process to group same clients complete. Duration: {}'.format(datetime.now() - process_start_time))
    
    out_df_temp.to_pickle(outputFileName+".pkl") 

    # Filling columns D,E,F

    print('\nStarted to fill counters...')
    countersStartTime=datetime.now()
    out_df_temp['Addresses']=''  
    out_df=out_df_temp[:0]
    n=1
    codeOrderList=['C','A','B','M','F','11','12','21','22','??']
    codeOrderDict={codeOrderList[i]:i for i in range(0,len(codeOrderList))}
    DTP_TDA_OrderList=['TDA','DTP','??']
    DTP_TDA_OrderDict={DTP_TDA_OrderList[i]:i for i in range(0,len(DTP_TDA_OrderList))}
    for i in out_df_temp['ClientId'].unique():
        print('Client:',i,end="\r")
        oneClientdf=out_df_temp[out_df_temp['ClientId']==str(i)].reset_index(drop=True)
#        print(oneClientdf)
        oneClientdf.insert(len(oneClientdf.columns),'C_order',"")
        oneClientdf.insert(len(oneClientdf.columns),'K_order',"")
    #    oneClientdf=oneClientdf.join(pd.DataFrame({'C_order':object(),'K_order':object()},index=[]))
    #    print("*** 1 i=",i," Shape: ", oneClientdf.shape )
        if oneClientdf.shape[0]==1:
            oneClientdf.at[0,"D"]=1
    #        print("*** 2", oneClientdf)
        for k in oneClientdf.index:
    #        print("*** 3 k=",k)
            oneClientdf.at[k,"E"]=k+1
            oneClientdf.at[k,"F"]=oneClientdf.shape[0]
    #        print("*** 4 D_ord_name=",oneClientdf.at[k,"Code"][:2],type(oneClientdf.at[k,"Code"]))
            code=oneClientdf.at[k,"Code"]
            if len(code)==0:
                oneClientdf.at[k,"C_order"]=codeOrderDict['??']
            elif code[0] in ['C','A','B','M','F']:
                oneClientdf.at[k,"C_order"]=codeOrderDict[code[0]]            
            elif len(code)>1 and code[:2] in ['11','12','21','22']:
                oneClientdf.at[k,"C_order"]=codeOrderDict[code[:2]]
            else:
                oneClientdf.at[k,"C_order"]=codeOrderDict['??']
            
            dTP_TDA=oneClientdf.at[k,"DTP_TDA"]
            if len(dTP_TDA)==0:
                oneClientdf.at[k,"K_order"]=DTP_TDA_OrderDict['??']
            elif dTP_TDA in ['TDA','DTP']: 
                oneClientdf.at[k,"K_order"] = DTP_TDA_OrderDict[dTP_TDA]
            else:
                oneClientdf.at[k,"K_order"]=DTP_TDA_OrderDict['??']
        oneClientdf=oneClientdf.sort_values(by = ['K_order','C_order','VALUE'], ascending = [True, True,False], na_position = 'first')
        oneClientdf=oneClientdf.reset_index(drop=True)
        oneClientdf['E']=oneClientdf.index+1
    #    oneClientdf=oneClientdf.sort_values(by = ['index'], ascending = [False], na_position = 'first')
        oneClientdf.at[0,"D"]='1'   

            # filling counters in column H

        addresses=oneClientdf[['index','A_Addr','A_Addr_s']]
        while addresses.shape[0]>0:
        #    print(addresses.shape[0])
            sa=addresses.iloc[0]
            addrSet=set([addresses.iloc[0]['A_Addr']])
            addresses=addresses.drop(index=addresses.index[0],axis=0)
            sameAddresses=addresses[addresses.A_Addr_s.apply(lambda x: compareNames(sa['A_Addr_s'],x)>=matchPercentAddress)]
        #    print(sameAddresses)
            if sameAddresses.shape[0]>0:
                addrSet.update(sameAddresses['A_Addr'].tolist())
            same_addr=','.join(list(addrSet))
            oneClientdf.update(pd.DataFrame({'H':[sameAddresses.shape[0]+1],'Addresses':same_addr},index=oneClientdf.index[oneClientdf['index']==sa['index']].tolist()))
            if addresses.shape[0]>0:
                oneClientdf.update(sameAddresses.assign(H=sameAddresses.shape[0]+1,Addresses=same_addr)[['index','H','Addresses']])
            addresses=addresses.drop(sameAddresses.index,axis=0)


    #    out_df=out_df.append(oneClientdf,ignore_index=True)
        out_df=pd.concat([out_df,oneClientdf], ignore_index=True)

    print('\nEnded filling counters. Duration: {}'.format(datetime.now() - countersStartTime))

    print("\nCleaning ...")
    cleaning_start_time=datetime.now()
    #print("Original list:",origColumnList)
    if useExemptions:
        out_df.drop(['Client_Name_woe'],axis=1, inplace=True)
    out_df.drop(['C_order','K_order','Client_Name_s','A_Addr_s','T_Addr_s'], axis=1, inplace=True)
    #out_df=out_df.drop(['index','ClientId','C_order','K_order'], axis=1)
    #print("Current list:",list(out_df.columns))

    # Replace back %E% with blank values
    df.loc[df['A_Addr']=='%E%',['A_Addr']]=''
    df.loc[df['T_Addr']=='%E%',['T_Addr']]=''
    df.loc[df['Client_Name']=='%E%',['Client_Name']]=''

    #old_new={origColumnList[i]:list(out_df.columns)[i] for i in range(len(origColumnList))}
    #print("\n Dictionary:",old_new)

    newColMapping={df.columns[4]:'1st ocurrence with a \nDTP/TDA and Highets Value',\
                df.columns[5]:'Number of occurences',\
                df.columns[6]:'Total Count of Clients',\
                df.columns[7]:"CLIENT'S NAME 1\n\n\n\n\n\n\n\n35",\
                df.columns[8]:"APPRAISER counter",\
                df.columns[9]:'APPRAISER Address 1\n\n\n\n\n\n\n\n25',\
                df.columns[10]:'Tax Collector Property Address 1\n\n\n\n\n\n\n25',\
                df.columns[11]:'DTP - TDA'
                }
    out_df=out_df.rename(columns=newColMapping)


    #df.set_axis(origColumnList,axis=1)
    print('Cleaning done. Duration: {}'.format(datetime.now() - cleaning_start_time))

    print("\nUnloading result to file: ",outfile)
    file_unload_start_time=datetime.now()
    out_df.to_excel(outfile,index=False)
    #sameClientGroup.to_excel(outfile)  
    print('Unload finished. Duration: {}'.format(datetime.now() -file_unload_start_time))

    print('All done. Total duration: {}'.format(datetime.now() - process_start_time))


