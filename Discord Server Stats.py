from datetime import timedelta, timezone, datetime
from discord.ext import commands
# import openpyxl
import discord
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

#*Add options to see the following properties of individual users (might want to use user class instead of member class when applicable because it is faster):
#*public_flags, created_at, bot, nick, messages per day, and add option to render their avatar in the spreadsheet

id_of_server = 
id_of_channels = []
message_capture_limit = None #Input None to measure the entire channel
ids_to_measure_dates = []
MeasureAllChannels = False
ReplaceZeroesWithEmptyCells = False
MeasureByPeople = True
MeasureAllDatesPeople = True
MeasureByDates = False
file_save_location = r'D:\Programs\Python\Discord Spreadsheets'
token = ''

start_time = time.time()
df_list = []
guild_list = []
df_list_by_dates = []
day_counter_list_for_days = []
new_df_list = []
total_messages_list = []
total_messages_list_by_dates = []
see_if_dates_checker = []
list_of_columns = []
aggregation_functions = {}
aggregation_functions_by_dates = {}
total_messages = 0
total_messages_by_dates = 0
counter = 0

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print(current_time)

for key, element in enumerate(ids_to_measure_dates):
    ids_to_measure_dates[key] = str(element)

def daterange(date1, date2):
    for n in range(int((date2 - date1).days)+1):
        yield date1 + timedelta(n)

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

client = commands.Bot(command_prefix='>', self_bot=True)
@client.event
async def on_ready():
    global id_of_channels
    guild = client.get_guild(id_of_server)
    permissions_list = []
    self_user = guild.me
    
    if MeasureAllChannels:
        id_of_channels = []
        for channel in guild.text_channels:
            for value in self_user.permissions_in(channel):
                permissions_list.append(value)
            if permissions_list[10][-1] == True:
                id_of_channels.append(channel.id)
                permissions_list = []

    for id_of_channel in id_of_channels:
        date_list = []
        empty_list = []
        list_counter_list = []
        zeros_list = []
        unique_author_id_list = []
        author_dict = {}
        counter = 0
        total_messages = 0
        counter_dates = 1
        day_counter_list = []
        date_dict = {}
        channel = client.get_channel(id_of_channel)
        creation_date = datetime.strptime(str(guild.created_at).split(' ')[0], '%Y-%m-%d').date()
        
        for dt in daterange(creation_date, datetime.now(timezone.utc).date()):
            date_list.append(str(dt.strftime("%Y-%m-%d")))
            list_of_columns.append(date_list[-1])
            zeros_list.append(0)
            empty_list.append(np.nan)
            
        for date in date_list:
            date_dict[date] = [counter_dates, date, 0]
            day_counter_list.append(counter_dates)
            counter_dates += 1
            
        async for message in channel.history(limit=message_capture_limit):
            total_messages += 1
            member = message.author
            author_id = member.id
            author_name = member.name
            author_tag = '#' + member.discriminator
            message_date = str(message.created_at).split(' ')[0]
            message_date_int =  to_integer(datetime.strptime(message_date, "%Y-%m-%d"))
            
            if MeasureByPeople:
                if author_id in unique_author_id_list:
                    author_dict[author_id][4] = (datetime.now(timezone.utc).date() - datetime.strptime(message_date, "%Y-%m-%d").date()).days
                    author_dict[author_id][3] = to_integer(datetime.strptime(message_date, "%Y-%m-%d"))
                    author_dict[author_id][2] += 1
                    if not MeasureAllDatesPeople:
                        if author_id in ids_to_measure_dates:
                            date_counter = date_list.index(message_date)
                            author_dict[author_id][9 + date_counter] += 1 #added columns
                            list_counter_list.append(0)
                            see_if_dates_checker.append(0)
                    else:
                        date_counter = date_list.index(message_date)
                        author_dict[author_id][9 + date_counter] += 1 #added columns
                        list_counter_list.append(0)
                        see_if_dates_checker.append(0)
                        
                else:
                    try:
                        await guild.fetch_member(author_id)
                        user_in_server = 'Yes'
                    except:
                        user_in_server = 'No'
                    author_dict[author_id] = [author_tag, author_name, 1, message_date_int, (datetime.now(timezone.utc).date() - datetime.strptime(message_date, "%Y-%m-%d").date()).days, message_date_int, (datetime.now(timezone.utc).date() - datetime.strptime(message_date, "%Y-%m-%d").date()).days, user_in_server, int(author_id)]
                    unique_author_id_list.append(author_id)
                    if not MeasureAllDatesPeople:
                        if author_id in ids_to_measure_dates:
                            author_dict[author_id].extend(zeros_list)
                            date_counter = date_list.index(message_date)
                            author_dict[author_id][9 + date_counter] += 1 #added columns
                            list_counter_list.append(0)
                            see_if_dates_checker.append(0)
                        else:
                            author_dict[author_id].extend(empty_list)
                    else:
                        author_dict[author_id].extend(zeros_list)
                        date_counter = date_list.index(message_date)
                        author_dict[author_id][9 + date_counter] += 1 #added columns
                        list_counter_list.append(0)
                        see_if_dates_checker.append(0)

            if MeasureByDates:
                date_dict[message_date][-1] += 1
                
            if total_messages % 10000 == 0:
                now = datetime.now()
                current_time = now.strftime("%H:%M:%S")
                print(f'{current_time} - {total_messages // 1000}k - {channel}')
                
        if MeasureByPeople:
            for id in unique_author_id_list:
                if len(list_counter_list) == 0:
                    author_dict[id] = author_dict[id][0:5]
                author_dict[counter] = author_dict.pop(id)
                if author_dict[counter][0] == '#0000':
                    del author_dict[counter]
                counter += 1
            
            columns_list = ['Tag', 'Name', f'({total_messages}) Message Total', 'First Message Date', 'Days Since First Message', 'Last Message Date', 'Days Since Last Message', 'Still in Server?', 'User ID']
            if len(list_counter_list) != 0:
                columns_list.extend(date_list)
            df = pd.DataFrame(author_dict.values(), index=author_dict.keys(), columns=columns_list)
            df['User ID'] = df['User ID'].apply('="{}"'.format)
            df['Name'] = df['Name'].apply('="{}"'.format)
            if ReplaceZeroesWithEmptyCells:
                counter3 = 0
                for column in df:
                    if counter3 < 8:
                        pass
                    else:
                        df[column] = df[column].replace(0, np.nan)
                    counter3 += 1
            df_export = df.copy()
            df_export['First Message Date'] = df_export['First Message Date'].apply(lambda element: f'{str(element)[0:4]}-{str(element)[4:6]}-{str(element)[6:]}')
            df_export['Last Message Date'] = df_export['Last Message Date'].apply(lambda element: f'{str(element)[0:4]}-{str(element)[4:6]}-{str(element)[6:]}')
            df_export.to_csv(f'{file_save_location}\\{channel} by Users, {guild}.csv', index=False, encoding='utf-8-sig')   
            df_list.append(df)
            total_messages_list.append(total_messages)
            print(df_export)
        
        if MeasureByDates:
            columns_list_by_dates = [f'({counter_dates}) Day Counter', 'Date', f'({total_messages}) Message Total']
            df = pd.DataFrame(date_dict.values(), index=day_counter_list, columns=columns_list_by_dates)
            df.to_csv(f'{file_save_location}\\{channel} by Dates, {guild}.csv', index=False, encoding='utf-8-sig')
            df_list_by_dates.append(df)
            total_messages_list_by_dates.append(total_messages)
            day_counter_list_for_days.append(counter)
            print(df)
    await client.close()
client.run(token)

if MeasureByPeople:
    for number in total_messages_list:
        total_messages += number
    final_message_count = total_messages

if MeasureByDates:
    for number in total_messages_list_by_dates:
        total_messages_by_dates += number
    final_message_count = total_messages_by_dates

if MeasureByPeople:
    for df in df_list:
        df = df.rename(columns={df.columns[2]: 'Message Total'})
        new_df_list.append(df)
    new_df_temp = pd.concat(new_df_list)
    aggregation_functions['Tag'] = 'first'
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
        
    if len(see_if_dates_checker) != 0:
        for date in list_of_columns:
            aggregation_functions[date] = 'sum'

    new_df = new_df_temp.groupby(new_df_temp['User ID']).aggregate(aggregation_functions)
    new_df = new_df.rename(columns={'Message Total': f'({total_messages}) Message Total'})
    if ReplaceZeroesWithEmptyCells:
        counter2 = 0
        for column in new_df:
            if counter2 < 8:
                pass
            else:
                new_df[column] = new_df[column].replace(0, np.nan)
            counter2 += 1
    new_df.to_csv(f'{file_save_location}\\Combined Channels by Users, {client.get_guild(id_of_server)}.csv', index=False, encoding='utf-8-sig')
    print(new_df)

if MeasureByDates:
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
    new_df_by_dates.to_csv(f'{file_save_location}\\Combined Channels by Dates, {client.get_guild(id_of_server)}.csv', index=False, encoding='utf-8-sig')
    print(new_df_by_dates)

end_time = int(time.time() - start_time)
try:
    print(f'Program took {end_time} to complete, indexing {final_message_count} messages across {len(id_of_channels)} channels')
except:
    print('finished')