from datetime import timedelta, timezone, datetime
from discord.ext import commands
import asyncio
# import openpyxl
import pandas as pd
import numpy as np
import time

#Need version 1.7.3 of discord.py

#!Make it so that you can only measure channels in specific categories using Guild.category
#!Use multiprocessing/threading (multiprocessessing might be faster) to speed up indexing times
#!and find a way to evenly split up the channels in the multiprocessessing
#!Fix ending time print values
#!USE OOP!
#!Make it so that dropped connections do not completely stop the bot
#!make it so that if someone double-texts (or triple texts or whatever), do not count it as multiple messages
#!include optional value to append strings in a list to check how many times each person in the server has included those strings in their messages
#!currently does not scan vc text channels

# function getToken() {
#   let a = [];
#   webpackChunkdiscord_app.push([[0],,e=>Object.keys(e.c).find(t=>(t=e(t)?.default?.getToken?.())&&a.push(t))]);
#   console.log(`${a}`);
#   return a[0];
# }
# getToken();

# df['author_id'] = df['author_id'].apply('="{}"'.format)
# df['author_name'] = df['author_name'].apply('="{}"'.format)

#*Add options to see the following properties of individual users (might want to use user class instead of member class when applicable because it is faster):
#*public_flags, created_at, bot, nick, messages per day, and add option to render their avatar in the spreadsheet

#?let us create a dictionary, where the keys are author_id's, and the values to each key is a number of dictionaries where each
#?dictionary corresponds to the variables of that user in a specific channel.

ID_OF_SERVER = 540980541810540551 #741441440021741579
channels_to_measure = [] #leave empty to measure all channels 741441440021741582
keys_to_remove = ['author', 'author_user_profile', 'message_dates_sent'] #list of attributes of Author objects to remove from the final spreadsheets
message_capture_limit = None #Input None to measure the entire channel
replace_zeroes_with_empty_cells = False
measure_by_people = True
measure_dates_per_person = True
measure_by_dates = False
file_save_location = r'D:\Programs\Python\Discord Spreadsheets'
token = '' #alt

now = datetime.now().date()
now_utc = datetime.now(timezone.utc).date()
current_time = now.strftime("%H:%M:%S")
print(current_time)

def is_none_checker(attribute, self, other):
    try:
        self_attr = getattr(self, attribute)
    except:
        self_attr = None
    try:
        other_attr = getattr(other, attribute)
    except:
        other_attr = None
    
    if self_attr is not None:
        return self_attr
    elif other_attr is not None:
        return other_attr
    else:
        return None

class Author:
    def __init__(self, message=None, DATE_DICT_ZEROES=None, input_dict=None):
        if message is not None:
            self.author = message.author
            self.author_id = self.author.id
            self.author_name = str(self.author)
            self.message_total = 0
            if DATE_DICT_ZEROES is not None:
                for date in DATE_DICT_ZEROES:
                    setattr(self, str(date), 0)
                self.message_dates_sent = []
        else:
            for attribute in input_dict:
                setattr(self, attribute, input_dict[attribute])

    def __add__(self, other):
        '''
        This will be how values are output in the "Combined Channels" spreadsheet.
        You must specify how each attribute from one author_id in channel_1 will combine with each attribute from the same author_id in channel_2.
        The output will be a dictionary of the same type as the output from the output_dataframe_values method, where the keys of the output for
        the built-in add function will be all of the attributes for an Author object and all of the values will be the corresponding value for each attribute.
        '''
        output_dict = {}
        output_dict['author_id'] = self.author_id
        output_dict['author_name'] = self.author_name
        output_dict['message_total'] = self.message_total + other.message_total
        output_dict['in_server'] = is_none_checker('in_server', self, other)
        
        if hasattr(self, 'message_dates_sent'):
            for date in list(DATE_DICT_ZEROES):
                output_dict[str(date)] = getattr(self, str(date)) + getattr(other, str(date))
            self.message_dates_sent.extend(other.message_dates_sent)
            output_dict['message_dates_sent'] = self.message_dates_sent
        return Author(input_dict=output_dict)
    
    async def get_user_profile(self, user_profile_cached):
        try:
            output = await client.fetch_user_profile(self.author_id)
            self.author_user_profile = output
        except:
            self.author_user_profile = None
        user_profile_cached.set()
    
    async def in_server_check(self):
        '''
        Checks to see if the user is still in the Discord server being read.
        '''
        def contains(list, filter):
            for x in list:
                if filter(x):
                    return True
            return False
        
        user_profile_cached = asyncio.Event()
        asyncio.create_task(self.get_user_profile(user_profile_cached))
        await user_profile_cached.wait()
        user_profile_cached.clear()
        if self.author_user_profile is not None:
            if contains(self.author_user_profile.mutual_guilds, lambda x: x.id == ID_OF_SERVER):
                self.in_server = True
            else:
                self.in_server = False
        else:
            self.in_server = False

    
    def update_message_totals(self, message):
        '''
        Updates message counters for both the author's total messages in the channel and their total messages in a specific day.
        Will only update message counter for a specific day if measure_dates_per_person is set to True.
        '''
        if hasattr(self, 'message_dates_sent'):
            message_date = message.created_at.date()
            message_date_total = getattr(self, str(message_date))
            setattr(self, str(message_date), message_date_total + 1)
            self.message_dates_sent.append(message_date)
        self.message_total += 1
    
    def get_min_max_dates(self):
        '''
        Sets attributes for earliest message date, latest message date, days since last message, and days since first message.
        Delete temporary attribute self.message_dates_sent since it is simply a list of all the dates the author has sent a message.
        This method will be called right before you convert the data into a DataFrame only if measure_dates_per_person is set to True.
        '''
        self.earliest_date = min(self.message_dates_sent)
        self.latest_date = max(self.message_dates_sent)
        self.days_since_first_message = (now_utc - self.earliest_date).days
        self.days_since_last_message = (now_utc - self.latest_date).days
    
    def make_values_nan(self):
        '''
        Converts all zero values in the dictionary containing the amount of messages the author sent per day into np.nan values.
        This will result in an output .csv file that will have empty cells instead of cells with the integer 0.
        This method is called when replace_zeroes_with_empty_cells is set to True.
        '''
        if hasattr(self, 'message_dates_dict'):
            for date in self.message_dates_dict:
                if self.message_dates_dict[date] == 0:
                    self.message_dates_dict[date] = np.nan

class Date:
    def __init__(self, date):
        self.date = date
        self.message_count = 0

    def update_message_totals(self):
        self.message_count += 1

start_time = time.time()
df_list_by_people = []
df_list_by_dates = []
day_counter_list_for_days = []
new_df_list = []
list_of_columns = []
aggregation_functions = {}
aggregation_functions_by_dates = {}

def common_list_members(list1, list2):
    '''
    Outputs a list of all of the elements that are in both list1 and list2 (outputs the intersection of the two sets).
    '''
    set1, set2 = set(list1), set(list2)
    return set1.intersection(set2)

def channel_list_creator(guild, self_user, bounding_channel_list):
    '''
    Takes as input a guild, your user, and the channels to include and outputs a list of all of the channels that your account has access to.
    This is to ensure that you have permissions to view the message history of all of the channels that are being analyzed.
    If bounding_channel_list is not an empty list, then the output will not contain any channels that are not in the bounding channel list.
    '''
    id_of_channels = []
    for channel in guild.text_channels:
        if channel.permissions_for(self_user).read_message_history == True:
            id_of_channels.append(channel.id)
    
    if len(bounding_channel_list) != 0:
        id_of_channels = common_list_members(id_of_channels, bounding_channel_list)
    return id_of_channels

def empty_date_dict_generator(guild):
    '''
    Outputs a dictionary where keys are datetime.date objects that go from the date the guild was created to the current date in UTC,
    and all values are innitialized at 0.
    '''
    output = {}
    date_list = pd.date_range(guild.created_at.date(), now_utc, freq='D')
    date_list = [pd.to_datetime(i).date() for i in date_list]
    for date in date_list:
        output[date] = 0
    return output

def global_dict_updater(global_dict, local_dict):
    '''
    Outputs the updated global dictionary for either authors or dates that will be used at the end of the program to
    form the "Combined Channels" spreadsheet(s) that will combine all of the message values from each of the channels,
    where the inputs are the global dictionary to be updated and the local dictionary to update it with.
    In an essence, this will create a dictionary where the keys are author_id's (which are unique to each author)
    or datetime's of specific dates (depending on whether or not you input a global dictionary for authors or dates respectively),
    and the values for each key will be a list of dictionaries, where each dictionary in that list will correspond to the
    output for each individual author or date in a specific text channel (in which by "output for each individual" I mean
    the output dictionary consisting of all of the attribute/value pairs from each author/date in an individual text channel).
    '''
    for key in local_dict:
        if key in global_dict:
            global_dict[key].append(local_dict[key])
        else:
            global_dict[key] = [local_dict[key]]
    return global_dict

def remove_list_elements(input_list, elements_to_remove):
    for element in elements_to_remove:
        try:
            input_list.remove(element)
        except:
            pass
    return input_list

global client
client = commands.Bot(command_prefix='>', self_bot=True)
@client.event
async def on_ready():
    global DATE_DICT_ZEROES, guild
    guild = client.get_guild(ID_OF_SERVER)
    id_of_channels = channel_list_creator(guild, guild.me, channels_to_measure)
    
    cached_user_profiles = []
    global_author_dict, global_date_dict = {}, {}
    if measure_by_people and measure_dates_per_person:
        DATE_DICT_ZEROES = empty_date_dict_generator(guild)
    elif measure_by_people and not measure_dates_per_person:
        DATE_DICT_ZEROES = None
    if measure_by_dates:
        date_list = list(empty_date_dict_generator(guild).keys())
    
    for channel_id in id_of_channels:
        local_author_dict, local_date_dict = {}, {}
        channel = client.get_channel(channel_id)
        
        if measure_by_dates:
            for date in date_list:
                local_date_dict[date] = Date(date)
        
        total_messages = 0
        async for message in channel.history(limit=message_capture_limit):
            total_messages += 1
            author = message.author

            if measure_by_people:
                if not author.id in local_author_dict:
                    local_author_dict[author.id] = Author(message=message, DATE_DICT_ZEROES=DATE_DICT_ZEROES)
                local_author_dict[author.id].update_message_totals(message)

            if measure_by_dates:
                local_date_dict[message.created_at.date()].update_message_totals()
                
            if total_messages % 10000 == 0:
                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")
                print(f'{current_time} - {total_messages // 1000}k - {channel}')
        
        if total_messages != 0:
            if measure_by_people:
                for author in local_author_dict:
                    if author not in cached_user_profiles:
                        await local_author_dict[author].in_server_check()
                        cached_user_profiles.append(author)
                global_author_dict = global_dict_updater(global_author_dict, local_author_dict)
                
                for author in local_author_dict:
                    if measure_dates_per_person:
                        local_author_dict[author].get_min_max_dates()
                    local_author_dict[author] = vars(local_author_dict[author])
                
                if author is not None:
                    columns_list = list(local_author_dict[author].keys())
                    columns_list = remove_list_elements(columns_list, keys_to_remove)
                    df = pd.DataFrame(data=local_author_dict.values(), columns=columns_list)
                    df.to_csv(f'{file_save_location}\\{channel} by Users, {guild}.csv', index=False, encoding='utf-8-sig')
                    print(df)
            
            if measure_by_dates:
                global_date_dict = global_dict_updater(global_date_dict, local_date_dict)
                for date in local_date_dict:
                    local_date_dict[date] = vars(local_date_dict[date])
                
                if date is not None:
                    columns_list = local_date_dict[date].keys()
                    columns_list = remove_list_elements(columns_list, keys_to_remove)
                    df = pd.DataFrame(data=local_date_dict.values(), columns=columns_list)
                    df.to_csv(f'{file_save_location}\\{channel} by Dates, {guild}.csv', index=False, encoding='utf-8-sig')
                    print(df)
    
    if len(global_author_dict.keys()) != 0:
        output_total_dict = {}
        for author_id in global_author_dict:
            output_value = None
            for channel_information in global_author_dict[author_id]:
                if output_value is None:
                    output_value = channel_information
                else:
                    output_value += channel_information
            output_total_dict[author_id] = output_value

        for author in output_total_dict:
            if measure_dates_per_person:
                output_total_dict[author].get_min_max_dates()
            output_total_dict[author] = vars(output_total_dict[author])
        
        columns_list = list(output_total_dict[author_id].keys())
        columns_list = remove_list_elements(columns_list, keys_to_remove)
        df = pd.DataFrame(data=output_total_dict.values(), columns=columns_list)
        df.to_csv(f'{file_save_location}\\Combined by Users, {guild}.csv', index=False, encoding='utf-8-sig')
        print(df)

    if len(global_date_dict.keys()) != 0:
        if measure_by_dates:
            pass
    
    await client.close()
client.run(token)

end_time = int(time.time() - start_time)
try:
    print(f'Program took {end_time} to complete, indexing {3 + 3} messages across {len(id_of_channels)} channels')
except:
    print('finished')