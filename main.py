from datetime import datetime, timedelta
import requests
import csv


def filter_by_week(data):
    current_week_start = datetime.now().date() - timedelta(days=datetime.now().weekday())
    last_week_start = current_week_start - timedelta(days=7)

    filtered_data = []

    for entry in data:
        date_str = entry[2]
        if date_str:
            date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f").date()
            if last_week_start <= date <= current_week_start:
                filtered_data.append(entry)

    return filtered_data


def retrieve_data_with_basic_auth(url, username, password):
    response = requests.get(url, auth=(username, password))

    if response.status_code == 200:
        return response.json()

    return []


def extract_names(data):
    names = [header["name"] for header in data["headers"]]
    return names


def write_data_to_csv(headers, data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    url = 'https://hmis.health.go.ug/api/37/events/query.json?programStage=aKclf7Yl1PE&paging=false&order=created&includeAllDataElements=false&attributeCc=UjXPudXlraY&attributeCos=l4UMmqvSBe5'
    username = 'moh-rch.dmurokora'
    password = 'Dhis@2022'
    data = retrieve_data_with_basic_auth(url, username, password)
    filtered_data = filter_by_week(data.get('rows'))
    names_list = extract_names(data)
    filename = 'data.csv'
    write_data_to_csv(names_list, filtered_data, filename)

