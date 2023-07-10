import json
import os
from datetime import datetime, timedelta
import requests
import csv


def filter_by_week(data, approved=False, seven_days=False):
    approved_key = 'twVlVWM3ffz'
    approved_position = None
    if approved:
        _count = 0
        for o in data['headers']:
            if o.get('name') == approved_key:
                approved_position = _count
                break
            _count += 1

    _filtered_data = {}
    for entry in data.get('rows'):
        if approved and approved_position:
            if str(entry[approved_position]).strip().lower() != 'approved':
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
    for key, value in org_data.items():
        count_data[key] = {}
        for entry in value:
            count_data[key][entry] = len(value)
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    filter_by_approved = False
    last_seven_days = True
    today = datetime.now()
    date_today = today.date().strftime("%Y-%m-%d")
    date_last_7_days = (today - timedelta(days=8)).date().strftime("%Y-%m-%d")
    url = f'https://hmis.health.go.ug/api/37/events/query.json?programStage=aKclf7Yl1PE&page=1&pageSize=100&totalPages=true&order=created&includeAllDataElements=true&attributeCc=UjXPudXlraY&attributeCos=l4UMmqvSBe5&startDate={date_today}&endDate={date_today}'
    if last_seven_days:
        url = f'https://hmis.health.go.ug/api/37/events/query.json?programStage=aKclf7Yl1PE&paging=false&order=created&includeAllDataElements=true&attributeCc=UjXPudXlraY&attributeCos=l4UMmqvSBe5&startDate={date_last_7_days}&endDate={date_today}'
    username = 'moh-rch.dmurokora'
    password = 'Dhis@2022'
    data = retrieve_data_with_basic_auth(url, username, password)
    filtered_data = filter_by_week(data, approved=filter_by_approved)
    names_list = ["dataelement", "period", "orgunit", "category", "attributeoptioncombo", "value",
                  "storedby", "lastupdated", "comment", "followup", "deleted"]
    for day, value in filtered_data.items():
        day_str = day.split(" ")[0]
        filename = f'data_{day_str}.csv'
        csv_data = []
        for org, count in value.items():
            data = [
                "DVj4areqLLK" if not filter_by_approved else "BDLKmLCokSH",
                day_str, org,
                "HllvX50cXC0",
                "HllvX50cXC0",
                count,
                "admin",
                day,
                "",
                "FALSE",
                "null"
            ]
            csv_data.append(data)
        write_data_to_csv(names_list, csv_data, filename)

