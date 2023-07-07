from datetime import timedelta, timezone, datetime
from discord.ext import commands
import discord
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

ID_OF_SERVER = 1091116862236012686
channels_to_measure = [] #leave empty to measure all channels
message_capture_limit = None #Input None to measure the entire channel
replace_zeroes_with_empty_cells = False
measure_by_people = True
measure_dates_per_person = True
measure_by_dates = True
file_save_location = r'D:\Programs\Python\Discord Spreadsheets'
token = 'NzEyMTQ3NjM4NDU5MzAxOTcw.GxGygN.gTZ28LjxpshSiVWm94VPcH82ou5u_AcBRsdsOE'

now = datetime.now()
now_utc = datetime.utcnow()
current_time = now.strftime("%H:%M:%S")
print(current_time)

class Author:
    def __init__(self, message, DATE_DICT_ZEROES):
        self.author = message.author
        self.author_id = self.author.id
        self.author_name = self.get_author_name()
        # self.author_avatar = self.author.display_icon
        self.message_total = 0
        self.in_server = self.in_server_check()
        if DATE_DICT_ZEROES is not None:
            self.message_dates_dict = DATE_DICT_ZEROES.copy()
            self.message_dates_sent = []
        del self.author

    def in_server_check(self):
        '''
        Checks to see if the user is still in the Discord server being read.
        '''
        if type(self.author) == discord.Member:
            return True
        elif type(self.author) == discord.User:
            return False
    
    def get_author_name(self):
        '''
        Checks to see if the Discord user still has their tag/discriminator (the numbers at the end of their name, e.g. Chris#9928).
        If they still have their discriminator, their name will be displayed with the tag and discriminator, e.g. Chris#1251.
        If they do not have a discriminator, their username will be displayed normally, e.g. Chris.
        '''
        if self.author.discriminator != '0':
            return f'{self.author.name}#{self.author.discriminator}'
        else:
            return self.author.name
    
    def update_message_totals(self, message):
        '''
        Updates message counters for both the author's total messages in the channel and their total messages in a specific day.
        Will only update message counter for a specific day if measure_dates_per_person is set to True.
        '''
        if hasattr(self, 'message_dates_dict'):
            self.message_dates_dict[message.created_at.date()] += 1
            self.message_dates_sent.append(message.created_at.date())
        self.message_total += 1
    
    def get_min_max_dates(self):
        '''
        Sets attributes for earliest message date, latest message date, days since last message, and days since first message.
        Delete temporary attribute self.message_dates_sent since it is simply a list of all the dates the author has sent a message.
        This method will be called right before you convert the data into a DataFrame only if measure_dates_per_person is set to True.
        '''
        self.earliest_date = min(self.message_dates_sent)
        self.latest_date = max(self.message_dates_sent)
        self.days_since_first_message = (now_utc.date() - self.earliest_date).days
        self.days_since_last_message = (now_utc.date() - self.latest_date).days
        del self.message_dates_sent
    
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
    
    def output_dataframe_values(self):
        '''
        Will unpack the message_dates_dict so that it is not a nested dictionary. This allows Pandas to read the attributes of each author correctly.
        Hence, this method will output a dictionary of all attributes of an Author object, where each date key in the message_dates_dict attribute will
        be its own key in the output of this method so that each Author object can properly be converted into rows of a DataFrame.
        This method will be called right before you convert the data into a DataFrame.
        '''
        if hasattr(self, 'message_dates_dict'):
            output = {}
            temp_variables = vars(self)
            output.update(temp_variables['message_dates_dict'])
            del temp_variables['message_dates_dict']
            output.update(temp_variables)
            return output
        else:
            return vars(self)

class Date:
    def __init__(self, date):
        self.date = date
        self.message_count = 0

    def update_message_totals(self):
        self.message_count += 1
    
    def output_dataframe_values(self):
        return vars(self)
    
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
    date_list = pd.date_range(guild.created_at.date(), datetime.now(timezone.utc).date(), freq='D')
    date_list = [pd.to_datetime(i).date() for i in date_list]
    for date in date_list:
        output[date] = 0
    return output

client = commands.Bot(command_prefix='>', self_bot=True)
@client.event
async def on_ready():
    global id_of_channels
    guild = client.get_guild(ID_OF_SERVER)
    id_of_channels = channel_list_creator(guild, guild.me, channels_to_measure)
    
    if measure_by_people and measure_dates_per_person:
        DATE_DICT_ZEROES = empty_date_dict_generator(guild)
    elif measure_by_people and not measure_dates_per_person:
        DATE_DICT_ZEROES = None
    if measure_by_dates:
        date_list = empty_date_dict_generator(guild).keys()
        
    for channel_id in id_of_channels:
        author_dict, date_dict = {}, {}
        channel = client.get_channel(channel_id)
        
        if measure_by_dates:
            for date in date_list:
                date_dict[date] = Date(date)
                example_date_index = date
        
        total_messages = 0
        async for message in channel.history(limit=message_capture_limit):
            total_messages += 1
            author = message.author
            
            if measure_by_people:
                if not author.id in author_dict:
                    author_dict[author.id] = Author(message, DATE_DICT_ZEROES)
                author_dict[author.id].update_message_totals(message)

            if measure_by_dates:
                date_dict[message.created_at.date()].update_message_totals()
                
            if total_messages % 10000 == 0:
                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")
                print(f'{current_time} - {total_messages // 1000}k - {channel}')
        
        if measure_by_people:
            for author in author_dict:
                if measure_dates_per_person:
                    author_dict[author].get_min_max_dates()
                author_dict[author] = author_dict[author].output_dataframe_values()
                example_author_index = author
            
            if example_author_index is not None:
                columns_list = author_dict[example_author_index].keys()
                df = pd.DataFrame(data=author_dict.values(), columns=columns_list)
                df.to_csv(f'{file_save_location}\\{channel} by Users, {guild}.csv', index=False, encoding='utf-8-sig')
                df_list_by_people.append(df)
                print(df)
        
        if measure_by_dates:
            for date in date_dict:
                date_dict[date] = date_dict[date].output_dataframe_values()
                example_date_index = date
            
            if example_date_index is not None:
                columns_list = date_dict[example_date_index].keys()
                df = pd.DataFrame(data=date_dict.values(), columns=columns_list)
                df.to_csv(f'{file_save_location}\\{channel} by Dates, {guild}.csv', index=False, encoding='utf-8-sig')
                df_list_by_dates.append(df)
                print(df)
        
    await client.close()
client.run(token)

if measure_by_people:
    for df in df_list_by_people:
        df = df.rename(columns={df.columns[2]: 'Message Total'})
        new_df_list.append(df)
    new_df_temp = pd.concat(new_df_list)
    aggregation_functions['Name'] = 'first'
    aggregation_functions['Message Total'] = 'sum'
    aggregation_functions['First Message Date'] = 'min'
    aggregation_functions['Days Since First Message'] = 'max'
    aggregation_functions['Last Message Date'] = 'max'
    aggregation_functions['Days Since Last Message'] = 'min'
    aggregation_functions['Still in Server?'] = 'first'
    aggregation_functions['User ID'] = 'first'
    
    new_df_temp['First Message Date'] = new_df_temp['First Message Date'].apply(lambda element: f'{str(element)[0:4]}-{str(element)[4:6]}-{str(element)[6:]}')
    new_df_temp['Last Message Date'] = new_df_temp['Last Message Date'].apply(lambda element: f'{str(element)[0:4]}-{str(element)[4:6]}-{str(element)[6:]}')
        
    if len(date_list) != 0:
        for date in date_list:
            aggregation_functions[date] = 'sum'

    new_df = new_df_temp.groupby(new_df_temp['User ID']).aggregate(aggregation_functions)
    new_df = new_df.rename(columns={'Message Total': f'({total_messages}) Message Total'})
    if replace_zeroes_with_empty_cells:
        counter2 = 0
        for column in new_df:
            if counter2 < 8:
                pass
            else:
                new_df[column] = new_df[column].replace(0, np.nan)
            counter2 += 1
    new_df.to_csv(f'{file_save_location}\\Combined Channels by Users, {client.get_guild(ID_OF_SERVER)}.csv', index=False, encoding='utf-8-sig')
    print(new_df)

if measure_by_dates:
    for df in df_list_by_dates:
        df = df.rename(columns={df.columns[0]: 'Day Counter'})
        df = df.rename(columns={df.columns[2]: 'Message Total'})
        new_df_list.append(df)
    new_df_temp_by_dates = pd.concat(new_df_list)
    
    aggregation_functions_by_dates['Day Counter'] = 'first'
    aggregation_functions_by_dates['Date'] = 'first'
    aggregation_functions_by_dates['Message Total'] = 'sum'
    new_df_by_dates = new_df_temp_by_dates.groupby(new_df_temp_by_dates['Date']).aggregate(aggregation_functions_by_dates)
    new_df_by_dates = new_df_by_dates.rename(columns={'Message Total': f'({total_messages_by_dates}) Message Total'})
    new_df_by_dates = new_df_by_dates.rename(columns={'Day Counter': f'({day_counter_list_for_days[-1]}) Day Counter'})
    new_df_by_dates.to_csv(f'{file_save_location}\\Combined Channels by Dates, {client.get_guild(ID_OF_SERVER)}.csv', index=False, encoding='utf-8-sig')
    print(new_df_by_dates)

end_time = int(time.time() - start_time)
try:
    print(f'Program took {end_time} to complete, indexing {final_message_count} messages across {len(id_of_channels)} channels')
except:
    print('finished')