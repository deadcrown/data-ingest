''' code to run MDFU as an executable
perform necessary manipulations as required for offline and custom audiences uploads
all the directory locations now point to network drives
'''

#import relevant facebook modules
from facebookads import FacebookSession
from facebookads import FacebookAdsApi
from facebookads import adobjects
from facebookads.adobjects.user import User
from facebookads.adobjects.FBAdAccount import FBAdAccount
from facebookads.adobjects.customaudience import CustomAudience

import argparse
import os
import json
import yaml
import subprocess
import datetime
import pandas
import csv

#store all global variables to external files to be used for uploads
#MDFU executable path
exec_path = r'C:\Users\axf8912\EXE-marketing-data-file-uploader-master'

serv_path = "\\\\at1a1\\"
#file path for audience_id_map.json
ca_map_path = r'vol4\DEPTS\HD\CRM\MuSigma\Agile Marketing\MOVE\Agile Marketing\05. Users\axf8912\mdfu_test\CustomAudiences\ca_id_map.xlsx'
ca_map_file = os.path.join(serv_path, ca_map_path)

#use FB Marketing API to check for custom audience id in case of custom audience uploads
config_path = r'C:\Users\axf8912\api_proj_py3\facebook-python-ads-sdk'
config_file = str(os.path.join(config_path, 'config_devAcct.json'))

#create argument parser variables
parser = argparse.ArgumentParser(description="Upload custom audiences and offline transactions files")
group = parser.add_mutually_exclusive_group()

group.add_argument("--customaudiences", help="Upload custom audiences")
group.add_argument("--offlineconversions", help="Upload transaction files")
parser.add_argument("--new", help="Defines a new custom audience to upload")
parser.add_argument("--existing", help="Defines an already uploaded custom audience. Uses id stored in customer_id mapping")
parser.add_argument("--retention", type=int, help="defines the retention period for the uploaded custom audience. Optional for existing auudiences")

args = parser.parse_args()

###########################################################################
#############check validity of parsed arguments
###########################################################################
'''
if args.offlineconversions:
    ##check for any wrongly provided prameter with oca
    if args.new or args.existing or args.retention:
        print("Invalid parameters for offline conversions. Exit")
        exit()

elif args.offlineconversions and args.customaudiences is None:
    print("****************Help Text*****************")

else:
    print("Invalid options. Exit")
    exit()
'''
###########################################################################

#get config file to be used for FB Marketing API
with open(config_file, 'r') as config:
    configs = json.load(config)

#setup parameters for FB session
session = FacebookSession(configs['app_id'], configs['app_secret'], configs['access_token'])
api = FacebookAdsApi(session)
FacebookAdsApi.set_default_api(api)

#create ad_account instance using the connection established
ad_acct = FBAdAccount(configs['act_id'])

#Function for uploading offline conversions
#disable temporary to avoid uploads to test event data set told by Carlos 
def offline_conversions():
    '''this function provides methods to execute offline transactions upload
    various options are defined in the command line help
    '''    
    del_pth_main_pth = r'vol4\DEPTS\HD\CRM\MuSigma\Agile Marketing\MOVE\Agile Marketing\05. Users\axf8912\mdfu_test\OfflineTransactions'
    del_pth_arc_pth = r'vol4\DEPTS\HD\CRM\MuSigma\Agile Marketing\MOVE\Agile Marketing\05. Users\axf8912\mdfu_test\OfflineTransactions\archive'
    del_pth_main = os.path.join(serv_path, del_pth_main_pth)
    del_pth_arc = os.path.join(serv_path, del_pth_arc_pth)
    for files in os.listdir(del_pth_main):
        if files.endswith(".csv"):
            txn_nm = files[:-4]
            #create a dated copy of the file in archive folder
            f = open(str(os.path.join(del_pth_main,files)), 'r')
            df_csv = pandas.read_csv(f)
            main_data = csv.reader(f)
            now = datetime.datetime.now()
            file_nm = txn_nm + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + ".csv" 
            arc_file = str(os.path.join(del_pth_arc, file_nm))
            df_csv.to_csv(arc_file, index=False)
            f.close()

            ###############manipulate config file(.yaml)################
            #edit oca.config.yaml inputFilePath 
            with open(str(os.path.join(exec_path, 'oca_file_uploader.conf.yml')), 'r') as oca_read:
                oca_conf_load = yaml.load(oca_read)
                oca_conf_load['inputFilePath'] = str(os.path.join(del_pth_main, files))
            with open(str(os.path.join(exec_path, 'oca_file_uploader.conf.yml')), 'w') as oca_write:
                yaml.dump(oca_conf_load, oca_write, default_flow_style=False)
            ############################################################
                
            #call MDFU executable with arguments for offline conversions
            oca_args = ['marketing-data-file-uploader.exe', 'offline-conversions']
            #subprocess.call(oca_args, shell=True) #temporary disable call of the executable

#Function for custom audience uploads
def custom_audi(mode):
    '''
    function defined for uploading custom audiences.
    can be used in 2 cases: new & existing audiences
    '''
    del_path_pth = r'vol4\DEPTS\HD\CRM\MuSigma\Agile Marketing\MOVE\Agile Marketing\05. Users\axf8912\mdfu_test\CustomAudiences'
    del_path = os.path.join(serv_path, del_path_pth)
    #separate process handling for new vs existing custom audiences
    if mode == 'new':
        del_pth_main = str(os.path.join(del_path, 'new'))
        del_pth_arc = str(os.path.join(del_pth_main, 'archive'))
        for files in os.listdir(del_pth_main):
            if files.endswith(".csv"):
                audi_nm = str(files[:-4])
                
                ###############manipulate delivery files(.csv)#################
                #extract retention parameter, clean the csv, and create a dated copy in archive
                f = open(str(os.path.join(del_pth_main,files)), 'r')
                df_csv = pandas.read_csv(f)
                main_data = csv.reader(f)
                now = datetime.datetime.now()
                file_nm = audi_nm + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + ".csv" 
                arc_file = str(os.path.join(del_pth_arc, file_nm))
                df_csv.to_csv(arc_file, index=False)
                f.close()
                f = open(str(os.path.join(del_pth_main,files)), 'r')
                ret_days = next(f)
                days = ret_days.split(sep=',', maxsplit=2)
                days1 = days[1:2]
                ret_time = str(days1[0]) #retention period saved
                #write back the csv in the original location without the retention parameter
                new_df = pandas.read_csv(f)
                os.chdir(del_pth_main)
                new_df.to_csv(str(files), index=False) #wrote back the csv file without the retention period                
                f.close()                                
                ############################################################                                

                ###############manipulate config file(.yaml)################
                #edit ca.config.yaml inputFilePath 
                with open(str(os.path.join(exec_path, 'ca_file_uploader.conf.yml')), 'r') as ca_read:
                    ca_conf_load = yaml.load(ca_read)
                    ca_conf_load['inputFilePath'] = str(os.path.join(del_pth_main, files))
                with open(str(os.path.join(exec_path, 'ca_file_uploader.conf.yml')), 'w') as ca_write:
                    yaml.dump(ca_conf_load, ca_write, default_flow_style=False)
                ############################################################

                #call MDFU executable passing new as one of the parameters
                ret = '--retention ' + str(ret_time)
                ca_args = ['marketing-data-file-uploader.exe', 'custom-audiences', ret]
                now_dir = os.chdir(exec_path)
                print(now_dir, '\t', ca_args)
                subprocess.call(ca_args, shell=True)

                ###############manipulate audi_id_map file(.xlsx)###########
                #store as a excel instead of csv to avoid precision loss in numbers(audience id)
                #read custom audience upload details to insert the id of the new audience currently uploaded
                audi_read = ad_acct.get_custom_audiences(fields=[CustomAudience.Field.name,
                                                                    CustomAudience.Field.id,
                                                                    CustomAudience.Field.retention_days,
                                                                    CustomAudience.Field.time_updated])            

                audi_read_list = [x for x in audi_read]
                df_audi = pandas.DataFrame(audi_read_list)
                df_list = df_audi.to_dict(orient='records')            
                #list to hold the last updated time for each audience upload
                time_updated = []
                for rec in df_list:
                    last_upd = int(rec['time_updated'])
                    time_updated.append(last_upd)
                time_updated.sort(reverse=True)
                t1 = time_updated[0]
                df_data = pandas.DataFrame(columns=['id', 'name', 'retention_days', 'time_updated'])
                #save the data read from the API to a csv file for audience name id map reference
                for rec in df_list:
                    if int(rec['time_updated']) == t1:
                        #df_data = pandas.DataFrame(columns=['id', 'name', 'retention_days', 'time_updated'])
                        df_data.loc[0] = str(rec['id'])
                        df_data.loc[0][1] = str(rec['name'])
                        df_data.loc[0][2] = str(rec['retention_days'])
                        df_data.loc[0][3] = str(rec['time_updated'])                        
                        df_csv = pandas.read_excel(ca_map_file, index_col=False)                        
                        df_final_data = pandas.concat([df_csv, df_data], axis=0)                        
                        print(df_data)
                        df_final_data.to_excel(ca_map_file, index=False)
                ########################################################
                                         
                #print relevant details to console/or to a log file
                print("Audience ID: %s \n Audience Name: %s \n Audience last updated: %s\n Audience retention days: %s\n Complete mapping details of all audiences present in file location:\n%s" % (df_data['id'], df_data['name'], df_data['time_updated'], df_data['retention_days'], ca_map_file))
                    
                
    if mode == 'existing':
        #move to existing delivery file path
        del_pth_main = str(os.path.join(del_path, 'existing'))
        del_pth_arc = str(os.path.join(del_pth_main, 'archive'))
        for files in os.listdir(del_pth_main):
            if files.endswith(".csv"):
                audi_nm = str(files[:-4])
                
                ###############manipulate delivery files(.csv)#################
                #extract retention parameter, clean the csv, and create a dated copy in archive
                f = open(str(os.path.join(del_pth_main,files)), 'r')
                df_csv = pandas.read_csv(f)
                main_data = csv.reader(f)
                now = datetime.datetime.now()
                file_nm = files[:-4] + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + ".csv" 
                arc_file = str(os.path.join(del_pth_arc, file_nm))
                df_csv.to_csv(arc_file, index=False)
                f.close()
                f = open(str(os.path.join(del_pth_main,files)), 'r')
                ret_days = next(f)
                days = ret_days.split(sep=',', maxsplit=2)
                days1 = days[1:2]
                ret_time = str(days1[0]) #retention period saved
                #write back the csv in the original location without the retention parameter
                new_df = pandas.read_csv(f)
                os.chdir(del_pth_main)
                new_df.to_csv(str(files), index=False) #wrote back the csv file without the retention period                
                f.close()                                
                ############################################################                

                ###############manipulate config file(.yaml)################
                #edit ca.config.yaml inputFilePath 
                with open(str(os.path.join(exec_path, 'ca_file_uploader.conf.yml')), 'r') as ca_read:
                    ca_conf_load = yaml.load(ca_read)
                    ca_conf_load['inputFilePath'] = str(os.path.join(del_pth_main, files))
                with open(str(os.path.join(exec_path, 'ca_file_uploader.conf.yml')), 'w') as ca_write:
                    yaml.dump(ca_conf_load, ca_write, default_flow_style=False)
                ############################################################

                #########lookup audience id based on the file name in ca_audi_map.xlsx##########                
                ca_audi_map = pandas.read_excel(ca_map_file, index_col=False)
                df_rec = ca_audi_map[ca_audi_map.name == audi_nm]
                audi_id = str(df_rec['id'])
                name = str(df_rec['name'])
                time_updated = int(df_rec['time_updated'])
                retn = '--retention ' + ret_time
                audience_id = '--customAudienceId ' + audi_id
                ca_args = ['marketing-data-file-uploader.exe', 'custom-audiences', audience_id, retn]
                os.chdir(exec_path)
                #print(str(os.getcwd()), '\t', ca_args)
                #call MDFU executable with the required parameters
                subprocess.call(ca_args, shell=True)
                #############upload complete with the stored audience id##############
                
                ############################################################
                #check the details of the last updated audience
                #compare the last updated audience id with the audience id stored corresponding to the file name
                audi_read = ad_acct.get_custom_audiences(fields=[CustomAudience.Field.name,
                                                                 CustomAudience.Field.id,
                                                                 CustomAudience.Field.time_updated,
                                                                 CustomAudience.Field.retention_days])
                audi_read_list = [x for x in audi_read]
                audi_df = pandas.DataFrame(audi_read_list)
                audi_df_det = audi_df[audi_df.name == audi_nm]
                #compare the last updated time with the previously stored valyue for the same audience name
                t1 = int(audi_df_det['time_updated'])
                if time_updated > t1:
                    print('Error. Upload the existing file again.\nFile Details %s \n %s' % (files, str(os.path.join(del_pth_main, files))))
                else:
                    pass
        
                #update the audience_id_map.xlsx with the new values for the selected audience
                dict_data = df_rec.to_dict(orient='records')
                for rec in dict_data:
                    if rec['name'] == audi_nm:
                        rec['retention_days'] = retn
                        rec['time_updated'] = str(time_updated)
                        rec['id'] = audi_id
                    
                new_df_data = pandas.DataFrame(dict_data)
                new_df_data.to_excel(ca_map_file, index=False)
                
if __name__ == "__main__":
    '''main function call to the THD MDFU application.
    Calls the required function based on parsed arguments from command line
    '''
    if args.customaudiences:
        print('Custom Audiences')
        if args.new:
            print("Upload new Custom Audience")
            custom_audi(mode="new")
            print('Upload Suceessful')
        elif args.existing:
            print("Upload existing Custom Audience")
            custom_audi(mode="existing")
            print("Upload Successful")
    elif args.offlineconversions:
        print('Offline conversions')
        offline_conversions()
        print("Upload Successful")
    else:
        print('Invalid arguments')
        exit()
    
