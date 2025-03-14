import json
import random
import zipfile
from datetime import datetime


def load_ocel_from_file(file_path: str) -> dict:
    if file_path.endswith('.zip'):
        with zipfile.ZipFile(file_path, 'r') as z:
            # assume there is only one JSON file inside or take the first one found
            json_filename = [name for name in z.namelist() if name.endswith('.jsonocel')
                             and not name.startswith('__MACOSX')][0]
            with z.open(json_filename) as file:
                data = json.load(file)
        return data
    elif file_path.endswith('.jsonocel'):
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data

def filter_ocel_json(data: dict, start_time="", end_time="", num_events=None, sampling=True) -> dict:
    events = data.get("ocel:events", {})

    event_list = [(event_id, event) for event_id, event in events.items()]

    # filter by time range
    if start_time or end_time:
        def within_time_range(event):
            timestamp = datetime.fromisoformat(event[1]["ocel:timestamp"])
            if start_time and timestamp < datetime.fromisoformat(start_time):
                return False
            if end_time and timestamp > datetime.fromisoformat(end_time):
                return False
            return True

        event_list = list(filter(within_time_range, event_list))

    # filter by taking the first num_events events
    if num_events and not sampling:
        event_list = event_list[:num_events]

    # filter by sampling num_events events
    if num_events and sampling:
        random.seed(42)
        event_list = random.sample(event_list, min(num_events, len(event_list)))

    # create a new dictionary with the filtered events
    filtered_events = {event_id: event for event_id, event in event_list}

    # find all objects referenced by the filtered events
    referenced_objects = set()
    for event in filtered_events.values():
        referenced_objects.update(event.get('ocel:omap', []))

    # filter objects based on referenced objects
    filtered_objects = {
        obj_id: obj for obj_id, obj in data.get('ocel:objects', {}).items()
        if obj_id in referenced_objects
    }

    filtered_data = {
        **data,
        "ocel:events": filtered_events,
        "ocel:objects": filtered_objects
    }

    return filtered_data


if __name__ == "__main__":
    # data = load_ocel_from_file("data/order-management.jsonocel")
    data = load_ocel_from_file("data/BPIC14.jsonocel.zip")
    filtered_data = filter_ocel_json(data, num_events=1000)

    with open("data/order-management-filtered.jsonocel", "w") as f:
        f.write(json.dumps(filtered_data, indent=4))
    print(json.dumps(filtered_data, indent=4))