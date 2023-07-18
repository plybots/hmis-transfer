import csv
import os
from datetime import datetime, timedelta

import requests
from requests.auth import HTTPBasicAuth


def filter_by_week(data, not_approved=False, seven_days=False):
    approved_key = 'twVlVWM3ffz'
    approved_position = None
    if not_approved:
        _count = 0
        for o in data['headers']:
            if o.get('name') == approved_key:
                approved_position = _count
                break
            _count += 1

    _filtered_data = {}
    for entry in data.get('rows'):
        if not_approved and approved_position:
            if not str(entry[approved_position]).strip().lower() == 'not approved':
                continue
        date_str = entry[2]
        if date_str:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f").date()
            if seven_days:
                if date_obj < datetime.now() - timedelta(days=7):
                    continue
            date = date_obj.strftime("%Y-%m-%d %H:%M:%S.%f")
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


def retrieve_data_with_basic_auth(url, username, password):
    response = requests.get(url, auth=(username, password))

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


def export_data(filtered_data, indicator):
    names_list = ["dataelement", "period", "orgunit", "category", "attributeoptioncombo", "value",
                  "storedby", "lastupdateds", "comment", "followup", "deleted"]
    label = "created" if indicator == 'DVj4areqLLK' else "not_approved"
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

        # Post the CSV file to the specified URL
        post_url = "https://ug.sk-engine.cloud/hmis/api/dataValueSets?async=true&dryRun=false&" \
                   "strategy=NEW_AND_UPDATES&preheatCache=false&skipAudit=false&dataElementIdScheme=UID&" \
                   "orgUnitIdScheme=UID&idScheme=UID&skipExistingCheck=true&format=csv&firstRowIsHeader=true"

        with open(filename, 'rb') as file:
            headers = {
                'Content-Type': 'application/csv'
            }
            response = requests.post(post_url, files={"file": file}, headers=headers,
                                     auth=HTTPBasicAuth('admin', 'district'))

        if response.status_code == 200:
            # os.remove(filename)
            print(f"CSV file '{filename}' posted successfully.")
            print("Server response:", response.text)
        else:
            print(f"Failed to post CSV file '{filename}'. Error: {response.text}")


def run(last_seven_days=False, not_approved=False, base_url='https://ug.sk-engine.cloud/hmis'):
    get_all = True
    today = datetime.now()
    date_today = today.date().strftime("%Y-%m-%d")
    create_start = date_today
    if get_all:
        create_start = (today - timedelta(days=3285)).date().strftime("%Y-%m-%d")
    date_last_7_days = (today - timedelta(days=8)).date().strftime("%Y-%m-%d")
    url = f'{base_url}/api/37/events/query.json?programStage=aKclf7Yl1PE&page=1&pageSize=100&' \
          f'totalPages=true&order=created&includeAllDataElements=true&attributeCc=UjXPudXlraY&' \
          f'attributeCos=l4UMmqvSBe5&startDate={create_start}&endDate={date_today}'
    if last_seven_days:
        url = f'{base_url}/api/37/events/query.json?programStage=aKclf7Yl1PE&paging=false&' \
              f'order=created&includeAllDataElements=true&attributeCc=UjXPudXlraY&attributeCos=l4UMmqvSBe5' \
              f'&startDate={date_last_7_days}&endDate={date_today}'

    data = retrieve_data_with_basic_auth(url, username, password)
    print("Data received:")
    filtered_data = filter_by_week(data, not_approved)
    print(f"Export {'created' if not not_approved else 'not approved'} records")
    export_data(filtered_data, "DVj4areqLLK" if not not_approved else "BDLKmLCokSH")


# Access Credentials
username = 'admin'
password = 'Nomisr123$'

if __name__ == '__main__':
    # get records created today
    run()
    # get records not approved in the last seven days
    run(last_seven_days=True, not_approved=True)
