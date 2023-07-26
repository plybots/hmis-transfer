import csv
import os
from datetime import datetime, timedelta
import pandas as pd

import requests
from requests.auth import HTTPBasicAuth

''' POST CREDENTIALS '''
base_post_url = "https://ug.sk-engine.cloud"
post_username = 'admin'
post_password = 'Nomisr123$'

''' GET CREDENTIALS '''
base_get_url = 'https://hmis.health.go.ug'
get_username = 'moh-rch.dmurokora'
get_password = 'Dhis@2022'

error_date = '1970-01-01 00:00:00.00'

cert_elms = [
    'sfpqAeqKeyQ',
    'zD0E77W4rFs',
    'cSDJ9kSJkFP',
    'WkXxkKEJLsg',
    'Ylht9kCLSRW',
    'zb7uTuBCPrN',
    'tuMMQsGtE69',
    'uckvenVFnwf',
    'fleGy9CvHYh',
    'myydnkmLfhp',
    'QGFYJK00ES7',
    'C8n6hBilwsX',
    'ZFdJRT3PaUd',
    'hO8No9fHVd2',
    'aC64sB86ThG',
    'CnPGhOcERFF',
    'IeS8V8Yf40N',
    'Op5pSvgHo1M',
    'eCVDO6lt4go',
    'cmZrrHfTxW3',
    'QTKk2Xt8KDu',
    'dTd7txVzhgY',
    'xeE5TQLvucB',
    'ctbKSNV2cg7',
    'mI0UjQioE7E',
    'krhrEBwJeNC',
    'u5ebhwtAmpU',
    'ZKtS7L49Poo',
    'OxJgcwH15L7',
    'fJDDc9mlubU',
    'Zrn8LD3LoKY',
    'z89Wr84V2G6'

]


def filter_by_week(
        data,
        not_approved=False,
        seven_days=False,
        notifications=False,
        certfications=False
):
    approved_key = 'twVlVWM3ffz'
    approved_position = None
    date_of_death_position = None
    __count = 0
    for o in data['headers']:
        if o.get('name') == 'i8rrl8YWxLF':
            date_of_death_position = __count
            break
        __count += 1

    if not_approved:
        _count = 0
        for o in data['headers']:
            if o.get('name') == approved_key:
                approved_position = _count
                break
            _count += 1

    notification_positions = []
    notification_elms = ['ZKBE8Xm9DJG', 'MOstDqSY0gO', 'ZYKmQ9GPOaF', 'zwKo51BEayZ', 'Z41di0TRjIu',
                         'dsiwvNQLe5n', 'RbrUuKFSqkZ', 'q7e7FOXKnOf', 'e96GB4CXyd3', 'i8rrl8YWxLF']

    if notifications:
        count = 0
        for o in data['headers']:
            if o.get('name') in notification_elms:
                notification_positions.append(count)
            count += 1

    cert_positions = []
    if certfications:
        count = 0
        for o in data['headers']:
            if o.get('name') in cert_elms:
                cert_positions.append(count)
            count += 1

    _filtered_data = {}
    for entry in data.get('rows'):
        if not_approved and approved_position:
            if not str(entry[approved_position]).strip().lower() == 'not approved':
                continue
        date_str = entry[date_of_death_position] if (notifications or certfications) else entry[2]
        if date_str:
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f%z' if (
                        notifications or certfications) else "%Y-%m-%d %H:%M:%S.%f").date()
            except Exception:
                date_obj = datetime.strptime(error_date, '%Y-%m-%d %H:%M:%S.%f')
            date = date_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
            is_notification = False
            is_certification = False
            if seven_days:
                if date_obj < datetime.now() - timedelta(days=7):
                    continue
            elif notifications:
                for np in notification_positions:
                    if str(entry[np]).strip():
                        is_notification = True
                        break
            elif certfications:
                for cp in cert_positions:
                    if str(entry[cp]).strip():
                        is_certification = True
                        break

            if notifications:
                if not is_notification:
                    continue

            elif certfications:
                if not is_certification:
                    continue

            if _filtered_data.get(date):
                _filtered_data[date].append(entry)
            else:
                _filtered_data[date] = [entry, ]
    org_data = {}
    for key, value in _filtered_data.items():
        org_data[key] = {}
        for entry in value:
            if org_data[key].get(entry[11]):
                org_data[key].get(entry[11]).append(entry)
            else:
                org_data[key][entry[11]] = [entry, ]

    count_data = {}
    for key, _value in org_data.items():
        count_data[key] = {}
        for entry, elms in _value.items():
            count_data[key][entry] = len(elms)
    return count_data


def retrieve_data_with_basic_auth(url):
    response = requests.get(url, auth=(get_username, get_password))
    print(response.status_code)
    if response.status_code == 200:
        return response.json()

    return []


def extract_names(data):
    names = [header["name"] for header in data["headers"]]
    return names


def write_data_to_csv(headers, data, filename):
    if os.path.exists(filename):
        os.remove(filename)
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)


def export_data(filtered_data, indicator, label):
    names_list = ["dataelement", "period", "orgunit", "category", "attributeoptioncombo", "value",
                  "storedby", "lastupdateds", "comment", "followup", "deleted"]
    # label = "created"
    # if indicator == 'DVj4areqLLK':
    #     label = "not_approved"

    for day, value in filtered_data.items():
        day_str = day.split(" ")[0]
        filename = f'data_{day_str}_{label}.csv'
        csv_data = []
        for org, count in value.items():
            _data = [
                indicator,
                day_str, org,
                "HllvX50cXC0",
                "HllvX50cXC0",
                count,
                "admin",
                "",
                "",
                "FALSE",
                "null"
            ]
            csv_data.append(_data)
        write_data_to_csv(names_list, csv_data, filename)


def post_csv_data(filename):
    # if '1970' in filename:
    #     print(filename)
    #     continue
    # Post the CSV file to the specified URL
    post_url = f"{base_post_url}/hmis/api/dataValueSets?async=true&dryRun=false&" \
               "strategy=NEW_AND_UPDATES&preheatCache=false&skipAudit=false&dataElementIdScheme=UID&" \
               "orgUnitIdScheme=UID&idScheme=UID&skipExistingCheck=true&format=csv&firstRowIsHeader=true"

    with open(filename, 'rb') as file:
        headers = {
            'Content-Type': 'application/csv'
        }

        response = requests.post(post_url, files={"file": file}, headers=headers,
                                 auth=HTTPBasicAuth(post_username, post_password))

    if response.status_code == 200:
        # os.remove(filename)
        print(f"CSV file '{filename}' posted successfully.")
        print("Server response:", response.text)
    else:
        print(f"Failed to post CSV file '{filename}'. Error: {response.text}")


def merge_csv_files_in_folder(folder_path, output_file_name='merged_file.csv', delete_after_merge=True):
    """
        Merge all CSV files in the given folder into one.

        Parameters:
            folder_path (str): The path to the folder containing the CSV files.
            output_file_name (str, optional): The name of the output merged CSV file. Default is 'merged_file.csv'.
            delete_after_merge (bool, optional): Whether to delete the original CSV files after merging. Default is True.
    """
    # Get a list of all CSV files in the folder
    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv') and '1970' not in file]

    # Initialize an empty DataFrame to store the merged data
    merged_df = pd.DataFrame()

    # Iterate through each CSV file and merge it with the existing data
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        merged_df = pd.concat([merged_df, df], ignore_index=True)

    # Save the merged data to a new CSV file
    output_file_path = os.path.join(folder_path, output_file_name)
    merged_df.to_csv(output_file_path, index=False)

    print(f"All CSV files in the folder have been merged into {output_file_path}.")

    # Delete the original CSV files if specified
    if delete_after_merge:
        for file in csv_files:
            file_path = os.path.join(folder_path, file)
            os.remove(file_path)
    post_csv_data(output_file_path)


def get_url(start, end, last7=False):
    return f'{base_get_url}/api/37/events/query.json?programStage=aKclf7Yl1PE&page=1&pageSize=100&' \
           f'totalPages=true&order=created&includeAllDataElements=true&attributeCc=UjXPudXlraY&' \
           f'attributeCos=l4UMmqvSBe5&startDate={start}&endDate={end}' if not last7 else f'{base_get_url}/api/37/events/query.json?programStage=aKclf7Yl1PE&paging=false&' \
                                                                                         f'&startDate={start}&endDate={end}'


def get_last_month_dates():
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_last_month = last_day_of_last_month.replace(day=1)
    return first_day_of_last_month.strftime('%Y-%m-%d'), last_day_of_last_month.strftime('%Y-%m-%d')


def run(
        last_seven_days=None,
        not_approved=None,
        notifications_all_months=False,
        notifications_last_month=False,
        notifications_today=False,
        certifications_last_month=False
):
    today = datetime.now()
    date_today = today.date().strftime("%Y-%m-%d")
    url = get_url(date_today, date_today)
    several_years_ago = (today - timedelta(days=3285)).date().strftime("%Y-%m-%d")
    if last_seven_days is not None or not_approved is not None:
        get_all = True
        create_start = date_today
        if get_all:
            create_start = several_years_ago
        date_last_7_days = (today - timedelta(days=8)).date().strftime("%Y-%m-%d")

        url = get_url(create_start, date_today)

        if last_seven_days:
            url = get_url(date_last_7_days, date_today)
    else:
        if notifications_all_months:
            url = get_url(several_years_ago, date_today)
        elif notifications_today:
            url = get_url(date_today, date_today)
        elif notifications_last_month or certifications_last_month:
            start, end = get_last_month_dates()
            url = get_url(start, end)

    data = retrieve_data_with_basic_auth(url)
    print("Data received:")
    log_text = ''
    label_text = 'created'
    filtered_data = {}
    indicator = "DVj4areqLLK"
    if not_approved is not None and last_seven_days is not None:
        filtered_data = filter_by_week(data, not_approved)
        if not_approved:
            indicator = 'BDLKmLCokSH'
            label_text = 'not_approved'
        log_text = f"Export {'created' if not not_approved else 'not approved'} records"
    elif notifications_all_months or notifications_today or notifications_last_month:
        filtered_data = filter_by_week(
            data,
            not_approved=False,
            seven_days=False,
            notifications=True
        )
        indicator = 'FIxCRqpxU9Y' if notifications_today else 'kV2JnJUsooE'
        log_text = \
            f"Export " \
            f"{'last month' if notifications_last_month else 'all months' if notifications_all_months else 'today'} " \
            f"notification records"
        label_text = \
            f"notifications" \
            f"_{'last_month' if notifications_last_month else 'all_months' if notifications_all_months else 'today'}"

    elif certifications_last_month:
        filtered_data = filter_by_week(
            data,
            not_approved=False,
            seven_days=False,
            notifications=False,
            certfications=True
        )
        indicator = 'KUsyTsNCDkl'
        log_text = f"Export last month certifications records"
        label_text = "certifications_last_month"
    print(log_text)
    export_data(filtered_data, indicator, label_text)


if __name__ == '__main__':
    print(
        f"Using {base_get_url} for GET requests"
    )
    print(
        f"Using {base_post_url} for POST requests"
    )
    # get records created today
    run()
    # get records not approved in the last seven days
    run(last_seven_days=True, not_approved=True)
    # get notifications for all months
    run(notifications_all_months=True)
    # get notifications for today
    run(notifications_today=True)
    # get notifications for last month
    run(notifications_last_month=True)
    # get certifications for last month
    run(certifications_last_month=True)

    folder_path = os.path.dirname(os.path.abspath(__file__))  # Set the current directory as the folder path
    merge_csv_files_in_folder(folder_path)
